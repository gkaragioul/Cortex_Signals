# SPDX-License-Identifier: MIT

"""Analytics queries that derive truth from the existing sim_bets table.

Phase 1 of the validation roadmap: no schema changes, no simulator touches.
Pure read-side queries that answer the three questions ChatGPT flagged:
- Score calibration: does score 70 actually mean 70% WR?
- Factor-family rollup: grouping signals reveals correlated overlap
- Signal decay / hold time: how long we hold, and whether it predicts PnL

Postgres-first with SQLite fallback, same pattern as pg_store.
"""
from __future__ import annotations

import logging
from app.services import pg_store
from app.database import get_db

logger = logging.getLogger(__name__)

# Map each signal_source value to a factor family. Families with multiple
# signals are where correlation double-counting lives — "whale consensus"
# + "leaderboard" + "copy" are all BEHAVIORAL and should share an exposure cap.
SOURCE_TO_FAMILY: dict[str, str] = {
    "mispricing": "structural",
    "correlated": "structural",
    "correlated_arb": "structural",
    "yesno_arb": "structural",
    "overround": "structural",
    "orderbook": "structural",
    "longshot": "structural",
    "harvest": "structural",
    "settlement": "structural",

    "copy": "behavioral",
    "wallet": "behavioral",
    "consensus": "behavioral",
    "leaderboard": "behavioral",

    "news_speed": "news",
    "sniper": "news",
    "chain": "news",

    "weather": "forecast",

    "confluence": "composite",
    "momentum": "momentum",
    "panic_fade": "momentum",
    "final_period": "momentum",
}

SCORE_BUCKET_WIDTH = 10


def _bucket_label(score: float) -> str:
    b = int(score // SCORE_BUCKET_WIDTH) * SCORE_BUCKET_WIDTH
    return f"{b}-{b + SCORE_BUCKET_WIDTH}"


def _predicted_wr(score: float) -> float:
    """Mirrors the est_win_prob formula in polymarket_simulator.py _kelly_size:
        est_win_prob = min(0.80, 0.45 + score * 0.004)
    Returns 0..1. Drift between this and the live sizer hides calibration miss
    on the dashboard, so they MUST stay in lockstep — when one changes, change both."""
    return min(0.80, 0.45 + score * 0.004)


async def _fetch_resolved_bets() -> list[dict]:
    """Pull every resolved bet with timing + score. PG first, SQLite fallback."""
    try:
        pool = await pg_store._get_pool()
    except Exception:
        pool = None

    if pool:
        try:
            async with pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT score, pnl, status, signal_source, timestamp, resolved_at
                    FROM sim_bets
                    WHERE status NOT IN ('open', 'void')
                """)
                return [dict(r) for r in rows]
        except Exception as e:
            logger.warning(f"analytics PG fetch failed: {e}")

    try:
        db = await get_db()
        try:
            cursor = await db.execute("""
                SELECT score, pnl, status, signal_source, timestamp, resolved_at
                FROM polymarket_sim_bets
                WHERE status NOT IN ('open', 'void')
            """)
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]
        finally:
            await db.close()
    except Exception as e:
        logger.warning(f"analytics SQLite fetch failed: {e}")
    return []


def _hold_seconds(ts: str | None, resolved: str | None) -> float | None:
    if not ts or not resolved:
        return None
    try:
        from datetime import datetime
        t0 = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        t1 = datetime.fromisoformat(resolved.replace("Z", "+00:00"))
        return max(0.0, (t1 - t0).total_seconds())
    except Exception:
        return None


def _is_win(row: dict) -> bool:
    status = row.get("status") or ""
    pnl = float(row.get("pnl") or 0)
    return status == "won" or (status == "sold" and pnl > 0)


async def get_calibration() -> dict:
    """Build score-bucket calibration, factor-family rollup, and hold-time stats.

    Returns a dict the dashboard renders straight into three small tables.
    """
    bets = await _fetch_resolved_bets()
    if not bets:
        return {"buckets": [], "families": [], "hold_by_source": [], "total_bets": 0}

    # ── Score bucket calibration ────────────────────────────────
    buckets: dict[str, dict] = {}
    for b in bets:
        score = float(b.get("score") or 0)
        label = _bucket_label(score)
        rec = buckets.setdefault(label, {
            "bucket": label,
            "bucket_start": int(score // SCORE_BUCKET_WIDTH) * SCORE_BUCKET_WIDTH,
            "n": 0, "wins": 0, "losses": 0,
            "pnl_sum": 0.0, "predicted_wr_sum": 0.0,
        })
        rec["n"] += 1
        if _is_win(b):
            rec["wins"] += 1
        else:
            rec["losses"] += 1
        rec["pnl_sum"] += float(b.get("pnl") or 0)
        rec["predicted_wr_sum"] += _predicted_wr(score)

    bucket_list = []
    for label, rec in sorted(buckets.items(), key=lambda kv: kv[1]["bucket_start"]):
        n = rec["n"]
        actual_wr = (rec["wins"] / n * 100.0) if n else 0.0
        predicted_wr = (rec["predicted_wr_sum"] / n * 100.0) if n else 0.0
        avg_pnl = (rec["pnl_sum"] / n) if n else 0.0
        bucket_list.append({
            "bucket": label,
            "n": n,
            "wins": rec["wins"],
            "losses": rec["losses"],
            "actual_wr": round(actual_wr, 1),
            "predicted_wr": round(predicted_wr, 1),
            "calibration_gap": round(actual_wr - predicted_wr, 1),
            "avg_pnl": round(avg_pnl, 2),
            "expectancy": round(avg_pnl, 2),  # per-bet dollar expectancy
            "total_pnl": round(rec["pnl_sum"], 2),
        })

    # ── Factor family rollup ────────────────────────────────────
    families: dict[str, dict] = {}
    for b in bets:
        src = b.get("signal_source") or "unknown"
        fam = SOURCE_TO_FAMILY.get(src, "other")
        rec = families.setdefault(fam, {
            "family": fam, "n": 0, "wins": 0, "losses": 0,
            "pnl_sum": 0.0, "sources": set(),
        })
        rec["n"] += 1
        if _is_win(b):
            rec["wins"] += 1
        else:
            rec["losses"] += 1
        rec["pnl_sum"] += float(b.get("pnl") or 0)
        rec["sources"].add(src)

    family_list = []
    for fam, rec in sorted(families.items(), key=lambda kv: -kv[1]["pnl_sum"]):
        n = rec["n"]
        family_list.append({
            "family": fam,
            "n": n,
            "wins": rec["wins"],
            "losses": rec["losses"],
            "win_rate": round((rec["wins"] / n * 100.0) if n else 0.0, 1),
            "total_pnl": round(rec["pnl_sum"], 2),
            "expectancy": round((rec["pnl_sum"] / n) if n else 0.0, 2),
            "sources": sorted(rec["sources"]),
        })

    # ── Hold time / decay by source ─────────────────────────────
    holds: dict[str, dict] = {}
    for b in bets:
        secs = _hold_seconds(b.get("timestamp"), b.get("resolved_at"))
        if secs is None:
            continue
        src = b.get("signal_source") or "unknown"
        rec = holds.setdefault(src, {
            "source": src, "n": 0, "hold_sum": 0.0,
            "wins_fast": 0, "wins_slow": 0,
            "losses_fast": 0, "losses_slow": 0,
        })
        rec["n"] += 1
        rec["hold_sum"] += secs
        fast = secs < 86400  # under 24h
        if _is_win(b):
            if fast:
                rec["wins_fast"] += 1
            else:
                rec["wins_slow"] += 1
        else:
            if fast:
                rec["losses_fast"] += 1
            else:
                rec["losses_slow"] += 1

    hold_list = []
    for src, rec in sorted(holds.items(), key=lambda kv: -kv[1]["n"]):
        n = rec["n"]
        avg_hold_h = (rec["hold_sum"] / n / 3600.0) if n else 0.0
        fast_n = rec["wins_fast"] + rec["losses_fast"]
        slow_n = rec["wins_slow"] + rec["losses_slow"]
        hold_list.append({
            "source": src,
            "n": n,
            "avg_hold_hours": round(avg_hold_h, 1),
            "fast_wr": round((rec["wins_fast"] / fast_n * 100.0) if fast_n else 0.0, 1),
            "fast_n": fast_n,
            "slow_wr": round((rec["wins_slow"] / slow_n * 100.0) if slow_n else 0.0, 1),
            "slow_n": slow_n,
        })

    # ── Bucket × source breakdown ───────────────────────────────
    # The biggest blind spot in the original three views: a source can have
    # decent overall WR but be catastrophic in one specific score bucket.
    # The auto-pruner uses per-source WR, so a bucket-level bleed slips
    # through. This cross-tab is what tells us "which source put the 10
    # losing bets into the 60-70 bucket?"
    bs: dict[tuple[str, str], dict] = {}
    for b in bets:
        score = float(b.get("score") or 0)
        label = _bucket_label(score)
        src = b.get("signal_source") or "unknown"
        key = (label, src)
        rec = bs.setdefault(key, {
            "bucket": label,
            "bucket_start": int(score // SCORE_BUCKET_WIDTH) * SCORE_BUCKET_WIDTH,
            "source": src,
            "n": 0, "wins": 0, "losses": 0, "pnl_sum": 0.0,
        })
        rec["n"] += 1
        if _is_win(b):
            rec["wins"] += 1
        else:
            rec["losses"] += 1
        rec["pnl_sum"] += float(b.get("pnl") or 0)

    bucket_source_list = []
    for rec in bs.values():
        n = rec["n"]
        bucket_source_list.append({
            "bucket": rec["bucket"],
            "bucket_start": rec["bucket_start"],
            "source": rec["source"],
            "n": n,
            "wins": rec["wins"],
            "losses": rec["losses"],
            "win_rate": round((rec["wins"] / n * 100.0) if n else 0.0, 1),
            "total_pnl": round(rec["pnl_sum"], 2),
            "expectancy": round((rec["pnl_sum"] / n) if n else 0.0, 2),
        })
    # Sort: biggest bleeders first (most negative PnL), then biggest winners,
    # so the top of the table is the most actionable. Filter n >= 2 to skip
    # noise from one-off bets.
    bucket_source_list = [r for r in bucket_source_list if r["n"] >= 2]
    bucket_source_list.sort(key=lambda r: (r["total_pnl"], -r["n"]))

    return {
        "buckets": bucket_list,
        "families": family_list,
        "hold_by_source": hold_list,
        "bucket_sources": bucket_source_list,
        "total_bets": len(bets),
    }
