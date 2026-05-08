"""
Signal configuration — per-signal enable/disable with database persistence.
Postgres-first, SQLite fallback (same pattern as simulator).
"""
import logging
from app.database import get_db

logger = logging.getLogger(__name__)

# All 15 signal sources with human labels and descriptions
ALL_SIGNALS = [
    {"id": "weather",      "label": "Weather Forecasts",        "description": "Bets on city temperature outcomes using real forecast data. 10-30% EV."},
    {"id": "mispricing",   "label": "Cross-Platform Mispricing","description": "Polymarket vs Manifold + Kalshi. Needs 8%+ edge."},
    {"id": "copy",         "label": "Wallet Copy Trading",      "description": "Follow wallets with 65%+ win rate and 20+ resolved trades."},
    {"id": "consensus",    "label": "Whale Consensus",          "description": "3+ top wallets agree on same direction."},
    {"id": "correlated",   "label": "Correlated Arbitrage",     "description": "Internal Polymarket contradictions with 30%+ spread."},
    {"id": "overround",    "label": "Favorite Fade (experimental)", "description": "Buys NO on the most-overpriced outcome when group YES prices sum > 110%. NOT true arbitrage — a directional bet that the favorite won't win. Disabled by default.", "default_enabled": False},
    {"id": "momentum",     "label": "Momentum / Mean Reversion","description": "15%+ crash then stabilise → buy the dip. Disabled 2026-04-16: 29% WR, -$112 across 21 bets.", "default_enabled": False},
    {"id": "news_speed",   "label": "Breaking News Speed",      "description": "Manifold lagging 10%+ behind Polymarket rapid moves."},
    {"id": "yesno_arb",    "label": "YES+NO Arbitrage",         "description": "CLOB: buy both sides when combined price < $1.00."},
    {"id": "orderbook",    "label": "Orderbook Depth",          "description": "CLOB bid/ask imbalance signals at 75%+ ratio. Disabled 2026-04-16: 38% WR, -$47 across 16 bets.", "default_enabled": False},
    {"id": "longshot",     "label": "Longshot Bias",            "description": "Academic: contracts under 12c are overpriced → sell YES."},
    {"id": "leaderboard",  "label": "Leaderboard Whales",       "description": "Follow top 100 most profitable Polymarket wallets."},
    {"id": "sniper",       "label": "Resolution Sniper",        "description": "Near-certain markets resolving within 48h."},
    {"id": "chain",        "label": "Market Chain Cascade",     "description": "Lagging markets in correlated event groups."},
    {"id": "settlement",   "label": "Settlement Harvester",     "description": "Outcome determined but not yet resolved. Buy winning side at 95-99c."},
]

_config_cache: dict[str, bool] = {}


async def _ensure_table():
    db = await get_db()
    try:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS signal_config (
                signal_id TEXT PRIMARY KEY,
                enabled INTEGER NOT NULL DEFAULT 1
            )
        """)
        await db.commit()
    finally:
        await db.close()


async def get_signal_config() -> list[dict]:
    """Return all signals with enabled state."""
    await _ensure_table()
    db = await get_db()
    try:
        cursor = await db.execute("SELECT signal_id, enabled FROM signal_config")
        rows = {r[0]: bool(r[1]) for r in await cursor.fetchall()}
    finally:
        await db.close()

    result = []
    for sig in ALL_SIGNALS:
        default = sig.get("default_enabled", True)
        result.append({
            **sig,
            "enabled": rows.get(sig["id"], default),
        })
    return result


async def is_signal_enabled(signal_id: str) -> bool:
    """Check if a signal is enabled. Falls back to per-signal default_enabled."""
    if signal_id in _config_cache:
        return _config_cache[signal_id]
    config = await get_signal_config()
    for s in config:
        _config_cache[s["id"]] = s["enabled"]
    if signal_id in _config_cache:
        return _config_cache[signal_id]
    for sig in ALL_SIGNALS:
        if sig["id"] == signal_id:
            return sig.get("default_enabled", True)
    return True


def invalidate_cache():
    """Clear the in-memory cache so next read hits the DB."""
    _config_cache.clear()


async def set_signal_enabled(signal_id: str, enabled: bool) -> dict:
    """Enable or disable a signal. Returns updated signal dict."""
    await _ensure_table()
    db = await get_db()
    try:
        await db.execute(
            "INSERT INTO signal_config (signal_id, enabled) VALUES (?, ?) "
            "ON CONFLICT(signal_id) DO UPDATE SET enabled=excluded.enabled",
            (signal_id, 1 if enabled else 0),
        )
        await db.commit()
    finally:
        await db.close()
    _config_cache[signal_id] = enabled
    logger.info(f"Signal '{signal_id}' {'ENABLED' if enabled else 'DISABLED'}")
    matching = [s for s in ALL_SIGNALS if s["id"] == signal_id]
    if matching:
        return {**matching[0], "enabled": enabled}
    return {"signal_id": signal_id, "enabled": enabled}
