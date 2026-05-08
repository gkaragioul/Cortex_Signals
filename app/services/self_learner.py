"""
Self-Learning Engine for Cortex Signals
===========================================
Learns from resolved bets to improve future performance.

Three learning systems:
1. Score Calibration — maps score ranges to REAL win rates (not estimates)
2. Source Exit Optimization — learns optimal hold times per signal source
3. Weather Calibration — tracks forecast error per city, adjusts sigma

All data persisted to Postgres. Recalibrates every cycle from resolved bets.
"""

import json
import logging
import re
import time
from datetime import datetime, timezone

from app.services import pg_store
from app.database import get_db
from app.services.websocket_manager import ws_manager

logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════
# CALIBRATION CACHE (in-memory, rebuilt from DB on each cycle)
# ══════════════════════════════════════════════════════════════

_score_calibration: dict[str, float] = {}    # score_range → real_win_rate
_source_hold_times: dict[str, float] = {}    # source → avg_hours_to_best_exit
_weather_sigma: dict[str, float] = {}        # city_slug → calibrated_sigma
_last_calibration: float = 0


# ══════════════════════════════════════════════════════════════
# 1. SCORE-TO-WIN-RATE CALIBRATION
# ══════════════════════════════════════════════════════════════

async def _calibrate_score_to_winrate():
    """Learn the REAL win rate for each score range from resolved bets.

    Instead of guessing score 50 = 65% win rate, we measure it.
    Groups bets by score range (30-40, 40-50, 50-60, 60-70, 70+)
    and calculates actual win rate per bucket.
    """
    global _score_calibration

    # Exclude weather bets — they use a binary probability model (forecast in/out of bucket)
    # that's unrelated to the score→win-rate relationship for non-weather signals.
    # Mixing them in poisons Kelly sizing: non-weather had 11% WR at score 60-70 which
    # makes Kelly go negative and blocks ALL weather bets (they score 60-85).
    rows = []
    try:
        pool = await pg_store._get_pool()
        if pool:
            async with pool.acquire() as conn:
                pg_rows = await conn.fetch(
                    "SELECT score, pnl FROM sim_bets WHERE status IN ('won', 'lost', 'sold') "
                    "AND score > 0 AND signal_source != 'weather'"
                )
                rows = [(float(r["score"]), float(r["pnl"])) for r in pg_rows]
    except Exception:
        pass

    if not rows:
        db = await get_db()
        try:
            cursor = await db.execute("""
                SELECT score, pnl FROM polymarket_sim_bets
                WHERE status IN ('won', 'lost', 'sold') AND score > 0
                AND signal_source != 'weather'
            """)
            rows = await cursor.fetchall()
        finally:
            await db.close()

    if len(rows) < 20:
        return  # Not enough data to calibrate

    # Group by score range (MIN_SCORE_FOR_BET=25, so below-30 bucket captures 25-29)
    buckets = {
        "below-30": {"wins": 0, "total": 0},
        "30-40": {"wins": 0, "total": 0},
        "40-50": {"wins": 0, "total": 0},
        "50-60": {"wins": 0, "total": 0},
        "60-70": {"wins": 0, "total": 0},
        "70+":   {"wins": 0, "total": 0},
    }

    for score, pnl in rows:
        score = float(score or 0)
        won = float(pnl or 0) > 0

        if score >= 70:
            key = "70+"
        elif score >= 60:
            key = "60-70"
        elif score >= 50:
            key = "50-60"
        elif score >= 40:
            key = "40-50"
        elif score >= 30:
            key = "30-40"
        else:
            key = "below-30"

        buckets[key]["total"] += 1
        if won:
            buckets[key]["wins"] += 1

    calibrated = {}
    for key, data in buckets.items():
        if data["total"] >= 5:  # Need at least 5 bets in a range
            wr = data["wins"] / data["total"]
            calibrated[key] = round(wr, 3)

    if calibrated:
        _score_calibration = calibrated
        logger.info(f"Score calibration: {calibrated}")


def get_calibrated_win_prob(score: float) -> float | None:
    """Return the REAL win probability for a score, or None if not enough data.

    When None, the caller should use the default estimate.
    """
    if not _score_calibration:
        return None

    if score >= 70 and "70+" in _score_calibration:
        return _score_calibration["70+"]
    elif score >= 60 and "60-70" in _score_calibration:
        return _score_calibration["60-70"]
    elif score >= 50 and "50-60" in _score_calibration:
        return _score_calibration["50-60"]
    elif score >= 40 and "40-50" in _score_calibration:
        return _score_calibration["40-50"]
    elif score >= 30 and "30-40" in _score_calibration:
        return _score_calibration["30-40"]

    return None


# ══════════════════════════════════════════════════════════════
# 2. PER-SOURCE EXIT OPTIMIZATION
# ══════════════════════════════════════════════════════════════

async def _calibrate_exit_timing():
    """Learn optimal hold times per signal source.

    Measures: for each source, what's the average time between bet placement
    and resolution? Sources with fast resolution (sports) should have tight
    stops. Sources with slow resolution (political) should have wide stops.
    """
    global _source_hold_times

    # Try Postgres first, fallback to SQLite
    rows = []
    try:
        pool = await pg_store._get_pool()
        if pool:
            async with pool.acquire() as conn:
                pg_rows = await conn.fetch(
                    "SELECT signal_source, timestamp, resolved_at FROM sim_bets "
                    "WHERE status IN ('won', 'lost', 'sold') AND resolved_at IS NOT NULL "
                    "AND signal_source IS NOT NULL AND signal_source != 'weather'"
                )
                rows = [(r["signal_source"], r["timestamp"], r["resolved_at"]) for r in pg_rows]
    except Exception:
        pass

    if not rows:
        db = await get_db()
        try:
            cursor = await db.execute("""
                SELECT signal_source, timestamp, resolved_at FROM polymarket_sim_bets
                WHERE status IN ('won', 'lost', 'sold') AND resolved_at IS NOT NULL
                AND signal_source IS NOT NULL AND signal_source != 'weather'
            """)
            rows = await cursor.fetchall()
        finally:
            await db.close()

    if len(rows) < 10:
        return

    # Calculate average hold time per source
    source_times: dict[str, list[float]] = {}
    for source, placed_at, resolved_at in rows:
        if not source or not placed_at or not resolved_at:
            continue
        try:
            placed = datetime.fromisoformat(placed_at.replace("Z", "+00:00"))
            resolved = datetime.fromisoformat(resolved_at.replace("Z", "+00:00"))
            hours = (resolved - placed).total_seconds() / 3600
            if hours > 0:
                source_times.setdefault(source, []).append(hours)
        except Exception:
            continue

    calibrated = {}
    for source, times in source_times.items():
        if len(times) >= 3:
            avg_hours = sum(times) / len(times)
            calibrated[source] = round(avg_hours, 1)

    if calibrated:
        _source_hold_times = calibrated
        logger.info(f"Exit timing calibration: {calibrated}")


def get_calibrated_exit_thresholds(source: str) -> tuple[float, float] | None:
    """Return optimized (win_threshold, loss_threshold) based on learned hold times.

    Fast-resolving sources (< 6 hours) → tight stops
    Medium (6-48 hours) → balanced stops
    Slow (48+ hours) → wide stops

    Returns None if not enough data — caller uses defaults.
    """
    avg_hours = _source_hold_times.get(source)
    if avg_hours is None:
        return None

    if avg_hours < 6:
        return (0.18, 0.10)   # Fast: tight stops
    elif avg_hours < 48:
        return (0.25, 0.12)   # Medium: balanced
    else:
        return (0.35, 0.15)   # Slow: wide stops


# ══════════════════════════════════════════════════════════════
# 3. WEATHER FORECAST CALIBRATION
# ══════════════════════════════════════════════════════════════

async def _calibrate_weather():
    """Track weather forecast accuracy per city from resolved weather bets.

    For each resolved weather bet, compare what we forecast vs the actual
    bucket that won. Calculate Mean Absolute Error per city and use it
    to adjust sigma (forecast uncertainty).
    """
    global _weather_sigma

    # Try Postgres first, fallback to SQLite
    rows = []
    try:
        pool = await pg_store._get_pool()
        if pool:
            async with pool.acquire() as conn:
                pg_rows = await conn.fetch(
                    "SELECT signal_detail, pnl, status FROM sim_bets "
                    "WHERE signal_source = 'weather' AND status IN ('won', 'lost') AND signal_detail IS NOT NULL"
                )
                rows = [(r["signal_detail"], float(r["pnl"] or 0), r["status"]) for r in pg_rows]
    except Exception:
        pass

    if not rows:
        db = await get_db()
        try:
            cursor = await db.execute("""
                SELECT signal_detail, pnl, status FROM polymarket_sim_bets
                WHERE signal_source = 'weather' AND status IN ('won', 'lost')
                AND signal_detail IS NOT NULL
            """)
            rows = await cursor.fetchall()
        finally:
            await db.close()

    if len(rows) < 10:
        return

    # Group errors by city
    city_errors: dict[str, list[float]] = {}
    city_results: dict[str, dict] = {}

    for detail_str, pnl, status in rows:
        try:
            detail = json.loads(detail_str) if isinstance(detail_str, str) else detail_str
            # Use city_slug as key (matches get_calibrated_sigma lookup).
            # Fall back to city name for legacy bets that predate city_slug storage.
            city = detail.get("city_slug") or detail.get("city", "")
            forecast = float(detail.get("forecast", 0) or 0)
            bucket = detail.get("bucket", "")

            if not city or not forecast:
                continue

            # Track wins/losses per city
            if city not in city_results:
                city_results[city] = {"wins": 0, "losses": 0}
            if float(pnl or 0) > 0:
                city_results[city]["wins"] += 1
            else:
                city_results[city]["losses"] += 1

            # Calculate forecast error: prefer actual_temp (real observed value),
            # fall back to bucket midpoint proxy only for old bets without it.
            actual = detail.get("actual_temp")
            if actual is not None:
                # Real observed temp available — exact error measurement
                error = abs(forecast - float(actual))
                city_errors.setdefault(city, []).append(error)
            elif status == "lost" and bucket:
                # No actual temp yet — estimate error from bucket midpoint proxy.
                # Only fires on losses (bucket midpoint is an underestimate on wins).
                parts = bucket.replace("°F", "").replace("°C", "").replace("F", "").replace("C", "").replace(" or below", "").replace(" or higher", "").strip()
                try:
                    m = re.match(r'^(-?\d+(?:\.\d+)?)-(-?\d+(?:\.\d+)?)$', parts)
                    if m:
                        mid = (float(m.group(1)) + float(m.group(2))) / 2
                    else:
                        mid = float(parts)
                    error = abs(forecast - mid)
                    city_errors.setdefault(city, []).append(error)
                except ValueError:
                    pass

        except Exception:
            continue

    # Calculate calibrated sigma per city
    calibrated = {}
    for city, errors in city_errors.items():
        if len(errors) >= 3:
            mae = sum(errors) / len(errors)
            # Sigma should be at least the MAE, but not less than 1.0
            calibrated[city] = round(max(1.0, mae), 2)

    if calibrated:
        _weather_sigma = calibrated
        logger.info(f"Weather sigma calibration: {calibrated}")


def get_calibrated_sigma(city_slug: str, default_sigma: float) -> float:
    """Return calibrated forecast uncertainty for a city.

    If we've learned the real error rate for this city, use it.
    Otherwise return the default (2.0F or 1.2C).
    """
    return _weather_sigma.get(city_slug, default_sigma)


# ══════════════════════════════════════════════════════════════
# MAIN CALIBRATION CYCLE
# ══════════════════════════════════════════════════════════════

async def run_calibration():
    """Run all three calibration systems. Called every scan cycle.

    Only recalibrates every 30 minutes to avoid excessive DB queries.
    """
    global _last_calibration

    if time.time() - _last_calibration < 1800:
        return

    try:
        await _calibrate_score_to_winrate()
        await _calibrate_exit_timing()
        await _calibrate_weather()
        _last_calibration = time.time()

        # Always report what was learned (even if empty)
        parts = []
        if _score_calibration:
            best = max(_score_calibration.items(), key=lambda x: x[1])
            worst = min(_score_calibration.items(), key=lambda x: x[1])
            parts.append(f"Score calibration: best={best[0]} at {best[1]*100:.0f}% WR, worst={worst[0]} at {worst[1]*100:.0f}% WR")
        if _source_hold_times:
            fastest = min(_source_hold_times.items(), key=lambda x: x[1])
            slowest = max(_source_hold_times.items(), key=lambda x: x[1])
            parts.append(f"Exit timing: fastest={fastest[0]} ({fastest[1]:.0f}h), slowest={slowest[0]} ({slowest[1]:.0f}h)")
        if _weather_sigma:
            parts.append(f"Weather sigma: {len(_weather_sigma)} cities calibrated")

        if parts:
            for p in parts:
                await ws_manager.send_log(f"[LEARN] {p}", "brain")
        else:
            await ws_manager.send_log(
                f"[LEARN] Calibration cycle complete — not enough resolved bets yet for learning (need 20+ total, 5+ per score range)",
                "brain"
            )

    except Exception as e:
        logger.debug(f"Calibration error: {e}")
