# SPDX-License-Identifier: MIT

"""Proactive Telegram opportunity alerts — Intelligence Officer pivot (v3.19.0).

The bot can't auto-trade Polymarket from Greece (geoblocked from Railway IPs),
but the user CAN trade manually from home/work. This module pushes high-
conviction opportunities to the user's phone via Telegram so they can act.

Design constraints:
- Throttling: max ALERTS_MAX_PER_HOUR (default 5), max ALERTS_MAX_PER_DAY (default 15)
  prevents phone-blasting on busy scan cycles.
- Dedup: each (slug, side) tracked for ALERT_DEDUP_HOURS (default 4) so the same
  signal doesn't re-alert as it persists across scan cycles.
- Threshold: minimum conviction score MIN_CONVICTION_FOR_ALERT (default 70).
  Above the bar = worth interrupting the user; below = stay quiet.
- Per-signal-type can have its own threshold via env vars in the future.
- Never raises into the parent scan loop. All failures swallowed.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from collections import deque
from typing import Optional

logger = logging.getLogger(__name__)

# ── Config (env-overridable) ───────────────────────────────────
def _max_per_hour() -> int:
    return int(os.getenv("ALERTS_MAX_PER_HOUR", "5"))


def _max_per_day() -> int:
    return int(os.getenv("ALERTS_MAX_PER_DAY", "15"))


def _min_conviction() -> int:
    return int(os.getenv("MIN_CONVICTION_FOR_ALERT", "70"))


def _dedup_hours() -> float:
    return float(os.getenv("ALERT_DEDUP_HOURS", "4"))


def _enabled_sources() -> set[str]:
    """Which signal types can fire Telegram alerts. Default = mispricing only.
    Mispricing is the bot's proven-edge signal (66% paper WR, +$224 / 35 bets).
    Other signals (whale, weather, harvest) have weaker math foundations and
    are opt-in via env. Set ALERT_SOURCES="mispricing,whale,weather,harvest"
    to enable all. Set ALERT_SOURCES="all" as shorthand."""
    raw = os.getenv("ALERT_SOURCES", "mispricing").strip().lower()
    if raw == "all":
        return {"mispricing", "whale", "weather", "harvest"}
    return {s.strip() for s in raw.split(",") if s.strip()}


# ── In-memory state (resets on deploy — that's OK) ─────────────
# Each entry: (timestamp, dedup_key)
_recent_alerts: deque = deque(maxlen=100)
# Each entry: (timestamp, dedup_key) — for hourly + daily caps
_alert_history: deque = deque(maxlen=200)


def _is_recently_alerted(dedup_key: str) -> bool:
    """True if (slug, side) has been alerted within the dedup window."""
    cutoff = time.time() - (_dedup_hours() * 3600)
    for ts, key in _recent_alerts:
        if ts > cutoff and key == dedup_key:
            return True
    return False


def _hourly_count() -> int:
    cutoff = time.time() - 3600
    return sum(1 for ts, _ in _alert_history if ts > cutoff)


def _daily_count() -> int:
    cutoff = time.time() - 86400
    return sum(1 for ts, _ in _alert_history if ts > cutoff)


def _record_alert(dedup_key: str) -> None:
    now = time.time()
    _recent_alerts.append((now, dedup_key))
    _alert_history.append((now, dedup_key))


# ── Public scanning entrypoint ─────────────────────────────────

async def scan_and_alert() -> int:
    """Periodic scanner — runs alongside the simulator scan loop.

    Pulls all signal types, scores each, dedups against recent alerts,
    respects hourly/daily caps, fires Telegram for new high-conviction
    opportunities. Returns count of alerts fired this cycle.

    Never raises into the parent scan loop.
    """
    try:
        from app.services.polymarket_wallets import (
            get_mispricing, get_copy_signals, get_settled_bets,
            get_news_speed_signals,
        )
        from app.services.weather_engine import get_weather_signals
        from app.services.polymarket_simulator import MOONSHOT_EV_THRESHOLD
    except Exception as e:
        logger.warning("opportunity_alerts: signal-source import failed: %s", e)
        return 0

    # Hard caps first — if we're already at limit, bail early to save work
    if _hourly_count() >= _max_per_hour():
        return 0
    if _daily_count() >= _max_per_day():
        return 0

    enabled = _enabled_sources()
    candidates: list[dict] = []

    # ── Mispricing (proven highest-edge signal in paper) ──
    if "mispricing" in enabled:
        try:
            for m in (get_mispricing() or []):
                edge = abs(float(m.get("edge_pct") or m.get("edge") or 0))
                if edge < 12:  # higher bar than dashboard's 8% — alert-worthy only
                    continue
                slug = m.get("slug") or m.get("market_slug") or ""
                side = "YES" if (m.get("edge_pct") or m.get("edge") or 0) > 0 else "NO"
                candidates.append({
                    "kind": "MISPRICING",
                    "emoji": "📈",
                    "slug": slug,
                    "side": side,
                    "market": m.get("market") or m.get("title") or "?",
                    # Consensus scanner emits poly_prob/fair_value (0-100); news-speed scanner
                    # emits poly_price/manifold_price. Fall through both so reasoning never
                    # silently says "0c vs 0c" (v3.20.2 bugfix).
                    "price": float(m.get("poly_price") or m.get("polymarket_price") or m.get("poly_prob") or 0),
                    "fair_value": float(m.get("manifold_price") or m.get("fair_price") or m.get("fair_value") or m.get("manifold_prob") or 0),
                    "event_slug": m.get("event_slug") or "",
                    "conviction": min(95, 50 + int(edge)),
                    "reasoning": f"Polymarket {round(float(m.get('poly_price') or m.get('polymarket_price') or m.get('poly_prob') or 0))}c vs Manifold/Kalshi fair {round(float(m.get('manifold_price') or m.get('fair_price') or m.get('fair_value') or m.get('manifold_prob') or 0))}c — {round(edge)}% edge",
                    "stake_suggestion": "$5-$15",
                    "url": m.get("market_url") or (f"https://polymarket.com/event/{m['event_slug']}/{slug}" if (slug and m.get("event_slug")) else (f"https://polymarket.com/market/{slug}" if slug else "")),
                })
        except Exception as e:
            logger.debug("opportunity_alerts mispricing scan: %s", e)

    # ── Whale entries (smart money copy signals) ──
    if "whale" in enabled:
        try:
            for w in (get_copy_signals() or []):
                wr = float(w.get("wallet_wr") or w.get("win_rate") or 0)
                trades = int(w.get("wallet_trades") or w.get("total_trades") or 0)
                if wr < 65 or trades < 20:
                    continue
                slug = w.get("slug") or w.get("market_slug") or ""
                side = w.get("side") or "YES"
                candidates.append({
                    "kind": "WHALE",
                    "emoji": "🐋",
                    "slug": slug,
                    "side": side,
                    "market": w.get("market") or "?",
                    "price": float(w.get("price") or 0.5),
                    "conviction": min(95, int(wr) + 10),
                    "reasoning": f"Tracked whale ({round(wr)}% WR / {trades} trades) just bought {side} at {round(float(w.get('price') or 0.5)*100)}c",
                    "stake_suggestion": "$2-$8",
                    "url": w.get("market_url") or (f"https://polymarket.com/market/{slug}" if slug else ""),
                })
        except Exception as e:
            logger.debug("opportunity_alerts whale scan: %s", e)

    # ── Weather moonshots (extreme EV + near-cert prob) ──
    if "weather" in enabled:
        try:
            for w in (get_weather_signals() or []):
                ev = float(w.get("ev") or 0)
                prob = float(w.get("probability") or 0)
                if ev < MOONSHOT_EV_THRESHOLD or prob < 95:
                    continue
                slug = w.get("slug") or ""
                side = w.get("side") or "YES"
                price = float(w.get("price") or 0.05)
                payout = round((1.0 / price) - 1) if price > 0 else 0
                candidates.append({
                    "kind": "WEATHER",
                    "emoji": "🚀",
                    "slug": slug,
                    "side": side,
                    "market": w.get("market") or f"{w.get('city','?')} {w.get('bucket','?')}",
                    "price": price,
                    "conviction": min(95, 70 + int(ev / 50)),
                    "reasoning": f"Forecast {w.get('forecast_temp','?')}° in {w.get('bucket','?')} bucket — EV +{round(ev)}%, models agree at {round(prob)}% probability",
                    "stake_suggestion": f"$2-$5 (potential {payout}x payout)",
                    "url": w.get("market_url") or (f"https://polymarket.com/market/{slug}" if slug else ""),
                })
        except Exception as e:
            logger.debug("opportunity_alerts weather scan: %s", e)

    # ── Settlement harvester (free money at 95-99c) ──
    if "harvest" in enabled:
        try:
            for h in (get_settled_bets() or []):
                layers = int(h.get("layers_passed") or 0)
                if layers < 6:  # alert only on max-safety harvester picks
                    continue
                slug = h.get("slug") or ""
                side = h.get("winning_side") or "YES"
                price = float(h.get("price") or 0.97)
                profit_pct = ((1.0 - price) / price * 100) if price > 0 else 0
                candidates.append({
                    "kind": "HARVEST",
                    "emoji": "💰",
                    "slug": slug,
                    "side": side,
                    "market": h.get("market") or "?",
                    "price": price,
                    "conviction": 90,  # max-safety harvester = always high conviction
                    "reasoning": f"Outcome determined; buy {side} at {round(price*100)}c, settle to $1.00 in ~{int(h.get('days_to_resolution') or 0)}d (+{profit_pct:.1f}%)",
                    "stake_suggestion": "$20-$50",
                    "url": h.get("market_url") or (f"https://polymarket.com/market/{slug}" if slug else ""),
                })
        except Exception as e:
            logger.debug("opportunity_alerts harvester scan: %s", e)

    # ── Filter, sort, alert ──
    threshold = _min_conviction()
    candidates = [c for c in candidates if c["conviction"] >= threshold]
    candidates.sort(key=lambda c: -c["conviction"])

    fired = 0
    for c in candidates:
        if _hourly_count() >= _max_per_hour() or _daily_count() >= _max_per_day():
            break
        dedup_key = f"{c['slug']}::{c['side']}::{c['kind']}"
        if _is_recently_alerted(dedup_key):
            continue
        if not c.get("slug"):
            continue
        try:
            await _enrich_with_walls(c)
            _send_alert(c)
            _record_alert(dedup_key)
            fired += 1
        except Exception as e:
            logger.warning("opportunity_alerts: send failed for %s: %s", dedup_key, e)

    if fired:
        logger.info("opportunity_alerts: fired %d alerts this cycle", fired)
    return fired


async def _enrich_with_walls(c: dict) -> None:
    """v3.20.0b — augment alert payload with orderbook wall summary so the
    user can see book structure inline (Polymonit-style). Best-effort only;
    skipped on failure. Mispricing alerts benefit most because they're the
    opt-in default — adds a concrete "smart-money resistance" data point."""
    if not c.get("slug"):
        return
    try:
        from app.services.polymarket_wallets import compute_walls_for_slug, summarize_walls
        walls = await compute_walls_for_slug(c["slug"], c.get("side", "YES"))
        c["walls_summary"] = summarize_walls(walls)
        c["walls_imbalance_pct"] = walls.get("imbalance_pct")
    except Exception as e:
        logger.debug("opportunity_alerts wall enrichment failed: %s", e)


def _cortex_deep_dive_url(slug: str, side: str) -> str:
    """v3.22.0 — the Cortex deep-dive page was removed with the webapp.
    Retained for callers as a direct Polymarket URL builder. Prefer
    candidate["url"] (canonical /event/ form) at the call site when
    available; this is the last-resort fallback."""
    return f"https://polymarket.com/market/{slug}" if slug else ""


def _tracked_poly_url(c: dict) -> str:
    """v3.20.8 — route Polymarket click through /api/polymarket/open so we
    only fire EXIT alerts for positions the user actually opened. Falls
    back to direct Polymarket link if CORTEX_PUBLIC_URL unset or slug
    missing (graceful degrade — user still gets a working link)."""
    base = os.getenv("CORTEX_PUBLIC_URL", "").rstrip("/")
    poly = c.get("url") or ""
    slug = c.get("slug") or ""
    if not base or not poly or not slug:
        return poly
    from urllib.parse import urlencode
    params = {
        "slug": slug,
        "side": c.get("side", "YES"),
        "kind": c.get("kind", "DEFAULT"),
        "redirect": poly,
    }
    if c.get("price") is not None:
        try:
            params["entry_price"] = f"{float(c['price']):.4f}"
        except (TypeError, ValueError):
            pass
    if c.get("market"):
        params["market"] = str(c["market"])[:200]
    if c.get("event_slug"):
        params["event_slug"] = c["event_slug"]
    return f"{base}/api/polymarket/open?{urlencode(params)}"


def _send_alert(c: dict) -> None:
    """Format + send a Telegram alert. Sync (telegram_notify._send is sync).

    v3.20.4 — every entry alert now includes an EXIT line (target/stop
    or hold-to-settlement) and auto-tracks the position in Postgres so
    the exit_monitor loop can fire a Telegram EXIT push when conditions
    trigger. Tracking happens after the Telegram send so a DB outage
    never blocks alert delivery.
    """
    from app.services.telegram_notify import _send
    from app.services.exit_advisor import compute_exit_plan
    price_c = round(float(c.get("price") or 0) * 100)
    exit_plan = compute_exit_plan(
        c.get("kind", ""),
        c.get("price"),
        c.get("fair_value"),
    )
    msg = (
        f"{c['emoji']} <b>OPPORTUNITY · {c['kind']}</b>  "
        f"<i>conviction {c['conviction']}/100</i>\n"
        f"<b>{(c.get('market') or '?')[:80]}</b>\n"
        f"<b>Bet:</b> {c['side']} @ {price_c}c\n"
        f"<b>Stake:</b> {c.get('stake_suggestion','$2-$10')}\n"
        f"<b>Why:</b> {c['reasoning']}\n"
        f"<b>Exit:</b> {exit_plan['rule_text']}\n"
    )
    if c.get("walls_summary"):
        msg += f"<b>Book:</b> {c['walls_summary']}\n"
    # v3.22.0 — webapp deleted, single CTA straight to Polymarket. The
    # tracked URL routes through /api/polymarket/open so the user's click
    # marks the tracked_alerts row opened (trigger for exit monitoring).
    tracked = _tracked_poly_url(c) or c.get("url") or _cortex_deep_dive_url(c.get("slug", ""), c.get("side", "YES"))
    if tracked:
        msg += f'\n<a href="{tracked}">→ Open on Polymarket</a>'
    _send(msg)
    # v3.20.8 — no pre-tracking on alert send. The /api/polymarket/open
    # click handler calls mark_or_create_opened which inserts (or revives)
    # the tracked_alerts row only when the user actually opens the URL.
    # Exits fire only for positions the user personally acted on.


def get_alert_state() -> dict:
    """Read-only state for /api/polymarket/alerts/status endpoint."""
    return {
        "hourly_count": _hourly_count(),
        "hourly_cap": _max_per_hour(),
        "daily_count": _daily_count(),
        "daily_cap": _max_per_day(),
        "min_conviction": _min_conviction(),
        "dedup_hours": _dedup_hours(),
        "enabled_sources": sorted(_enabled_sources()),
        "recent_dedup_keys": [k for _, k in list(_recent_alerts)[-20:]],
    }
