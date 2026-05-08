"""
Persistent Postgres storage for Polymarket simulator.

Uses the DATABASE_URL from Railway's Postgres service.
Falls back to in-memory storage if Postgres is unavailable.
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

_pool = None
_initialized = False


async def _get_pool():
    """Get or create the asyncpg connection pool."""
    global _pool
    if _pool is not None:
        return _pool

    db_url = os.getenv("DATABASE_URL", "")
    if not db_url:
        logger.warning("PG_STORE: No DATABASE_URL set — simulator data will not persist")
        return None

    try:
        import asyncpg
        _pool = await asyncpg.create_pool(db_url, min_size=1, max_size=3, timeout=10)
        logger.info("PG_STORE: Connected to Postgres")
        return _pool
    except Exception as e:
        logger.error(f"PG_STORE: Failed to connect to Postgres: {e}")
        return None


async def init_tables():
    """Create simulator tables in Postgres if they don't exist."""
    global _initialized
    pool = await _get_pool()
    if not pool:
        return False

    try:
        async with pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS sim_bets (
                    id SERIAL PRIMARY KEY,
                    timestamp TEXT NOT NULL,
                    market TEXT NOT NULL,
                    market_slug TEXT,
                    side TEXT NOT NULL,
                    entry_price REAL NOT NULL,
                    bet_amount REAL NOT NULL,
                    shares REAL NOT NULL,
                    signal_source TEXT,
                    signal_detail TEXT,
                    score REAL DEFAULT 0,
                    status TEXT DEFAULT 'open',
                    exit_price REAL,
                    pnl REAL,
                    resolved_at TEXT,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS sim_snapshots (
                    id SERIAL PRIMARY KEY,
                    date TEXT NOT NULL UNIQUE,
                    bankroll REAL NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS sim_state (
                    id INTEGER PRIMARY KEY DEFAULT 1,
                    bankroll REAL DEFAULT 1000.0,
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """)
            # Add watermarks column for trailing stop persistence (safe if already exists)
            await conn.execute("""
                ALTER TABLE sim_state ADD COLUMN IF NOT EXISTS watermarks TEXT DEFAULT '{}'
            """)
            # Insert default state if not exists
            await conn.execute("""
                INSERT INTO sim_state (id, bankroll) VALUES (1, 1000.0)
                ON CONFLICT (id) DO NOTHING
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS sim_signal_cache (
                    signal_type TEXT PRIMARY KEY,
                    data TEXT NOT NULL,
                    scan_time REAL DEFAULT 0,
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """)
        _initialized = True
        logger.info("PG_STORE: Tables initialized")
        return True
    except Exception as e:
        logger.error(f"PG_STORE: Table init failed: {e}")
        return False


async def get_bankroll() -> float:
    """Get current bankroll from Postgres."""
    pool = await _get_pool()
    if not pool:
        return 1000.0
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT bankroll FROM sim_state WHERE id = 1")
            return float(row["bankroll"]) if row else 1000.0
    except Exception as e:
        logger.error(f"PG_STORE get_bankroll: {e}")
        return 1000.0


async def set_bankroll(amount: float):
    """Update bankroll in Postgres."""
    pool = await _get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE sim_state SET bankroll = $1, updated_at = NOW() WHERE id = 1",
                amount)
    except Exception as e:
        logger.error(f"PG_STORE set_bankroll: {e}")


async def adjust_bankroll(pnl: float):
    """Atomically add PnL to bankroll. Avoids the read-then-write race condition."""
    pool = await _get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE sim_state SET bankroll = bankroll + $1, updated_at = NOW() WHERE id = 1",
                pnl)
    except Exception as e:
        logger.error(f"PG_STORE adjust_bankroll: {e}")


async def place_bet(market: str, slug: str, side: str, price: float,
                    amount: float, shares: float, source: str, detail: str, score: float) -> int | None:
    """Record a bet in Postgres. Returns the bet ID."""
    pool = await _get_pool()
    if not pool:
        return None
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO sim_bets (timestamp, market, market_slug, side, entry_price,
                    bet_amount, shares, signal_source, signal_detail, score, status)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'open')
                RETURNING id
            """, datetime.now(timezone.utc).isoformat(), market, slug, side,
                price, amount, shares, source, detail, score)
            return row["id"] if row else None
    except Exception as e:
        logger.error(f"PG_STORE place_bet: {e}")
        return None


async def resolve_bet(bet_id: int, exit_price: float, pnl: float, status: str):
    """Resolve a bet (won/lost/sold)."""
    pool = await _get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("""
                UPDATE sim_bets SET exit_price = $1, pnl = $2, status = $3,
                    resolved_at = $4 WHERE id = $5
            """, exit_price, pnl, status, datetime.now(timezone.utc).isoformat(), bet_id)
    except Exception as e:
        logger.error(f"PG_STORE resolve_bet: {e}")


async def get_open_bets() -> list[dict]:
    """Get all open bets."""
    pool = await _get_pool()
    if not pool:
        return []
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM sim_bets WHERE status = 'open' ORDER BY created_at DESC")
            return [dict(r) for r in rows]
    except Exception as e:
        logger.error(f"PG_STORE get_open_bets: {e}")
        return []


async def get_bet_history(limit: int = 50) -> list[dict]:
    """Get resolved bets."""
    pool = await _get_pool()
    if not pool:
        return []
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM sim_bets WHERE status != 'open' ORDER BY resolved_at DESC LIMIT $1",
                limit)
            return [dict(r) for r in rows]
    except Exception as e:
        logger.error(f"PG_STORE get_bet_history: {e}")
        return []


async def get_stats() -> dict:
    """Get win/loss stats."""
    pool = await _get_pool()
    if not pool:
        return {"wins": 0, "losses": 0, "total_pnl": 0, "total_bets": 0}
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT
                    COUNT(*) FILTER (WHERE status = 'won' OR (status = 'sold' AND pnl > 0)) as wins,
                    COUNT(*) FILTER (WHERE status = 'lost' OR (status = 'sold' AND pnl <= 0)) as losses,
                    COALESCE(SUM(pnl), 0) as total_pnl,
                    COUNT(*) FILTER (WHERE status NOT IN ('open', 'void')) as total_bets
                FROM sim_bets
            """)
            return dict(row) if row else {"wins": 0, "losses": 0, "total_pnl": 0, "total_bets": 0}
    except Exception as e:
        logger.error(f"PG_STORE get_stats: {e}")
        return {"wins": 0, "losses": 0, "total_pnl": 0, "total_bets": 0}


async def get_open_bet_count() -> int | None:
    """Count open bets. Returns None if Postgres unavailable (caller falls back to SQLite)."""
    pool = await _get_pool()
    if not pool:
        return None
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT COUNT(*) as cnt FROM sim_bets WHERE status = 'open'")
            return int(row["cnt"]) if row else 0
    except Exception:
        return None


async def has_open_bet_on_market(market: str) -> bool:
    """Check if there's already an open bet on this market."""
    pool = await _get_pool()
    if not pool:
        return False
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT id FROM sim_bets WHERE market = $1 AND status = 'open' LIMIT 1",
                market)
            return row is not None
    except Exception as e:
        return False


async def save_snapshot(bankroll: float):
    """Save a daily bankroll snapshot."""
    pool = await _get_pool()
    if not pool:
        return
    try:
        today = time.strftime("%Y-%m-%d")
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO sim_snapshots (date, bankroll)
                VALUES ($1, $2)
                ON CONFLICT (date) DO UPDATE SET bankroll = $2
            """, today, bankroll)
    except Exception as e:
        logger.error(f"PG_STORE save_snapshot: {e}")


async def get_snapshots() -> list[dict]:
    """Get bankroll snapshots for the chart."""
    pool = await _get_pool()
    if not pool:
        return []
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT date, bankroll FROM sim_snapshots ORDER BY date ASC LIMIT 90")
            return [dict(r) for r in rows]
    except Exception as e:
        return []


async def get_performance_by_source() -> dict:
    """Get win/loss breakdown by signal source."""
    pool = await _get_pool()
    if not pool:
        return {}
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT signal_source,
                    COUNT(*) FILTER (WHERE status = 'won' OR (status = 'sold' AND pnl > 0)) as wins,
                    COUNT(*) FILTER (WHERE status = 'lost' OR (status = 'sold' AND pnl <= 0)) as losses,
                    COALESCE(SUM(pnl), 0) as total_pnl
                FROM sim_bets WHERE status != 'open'
                GROUP BY signal_source
            """)
            return {r["signal_source"]: dict(r) for r in rows}
    except Exception as e:
        logger.error(f"PG_STORE get_performance_by_source: {e}")
        return {}


async def save_watermarks(watermarks: dict) -> None:
    """Persist trailing stop high-watermarks so they survive deploys."""
    pool = await _get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE sim_state SET watermarks = $1 WHERE id = 1",
                json.dumps({str(k): v for k, v in watermarks.items()})
            )
    except Exception as e:
        logger.warning(f"PG_STORE save_watermarks: {e}")


async def load_watermarks() -> dict:
    """Load trailing stop high-watermarks from Postgres."""
    pool = await _get_pool()
    if not pool:
        return {}
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT watermarks FROM sim_state WHERE id = 1")
            if row and row["watermarks"]:
                raw = json.loads(row["watermarks"])
                return {int(k): float(v) for k, v in raw.items()}
    except Exception as e:
        logger.warning(f"PG_STORE load_watermarks: {e}")
    return {}


async def save_signal_cache(signal_type: str, signals: list, scan_time: float) -> None:
    """Persist signal scan results so they survive redeploys."""
    pool = await _get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO sim_signal_cache (signal_type, data, scan_time, updated_at)
                VALUES ($1, $2, $3, NOW())
                ON CONFLICT (signal_type) DO UPDATE
                SET data = $2, scan_time = $3, updated_at = NOW()
            """, signal_type, json.dumps(signals), scan_time)
    except Exception as e:
        logger.warning(f"PG_STORE save_signal_cache({signal_type}): {e}")


async def load_signal_cache(signal_type: str) -> tuple[list, float]:
    """Load cached signals from Postgres. Returns (signals, scan_time)."""
    pool = await _get_pool()
    if not pool:
        return [], 0.0
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT data, scan_time FROM sim_signal_cache WHERE signal_type = $1",
                signal_type
            )
            if row and row["data"]:
                return json.loads(row["data"]), float(row["scan_time"])
    except Exception as e:
        logger.warning(f"PG_STORE load_signal_cache({signal_type}): {e}")
    return [], 0.0


async def reset_all():
    """Reset simulator — clear all bets and reset bankroll."""
    pool = await _get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM sim_bets")
            await conn.execute("DELETE FROM sim_snapshots")
            await conn.execute("DELETE FROM sim_signal_cache")
            await conn.execute("UPDATE sim_state SET bankroll = 1000.0, watermarks = '{}', updated_at = NOW() WHERE id = 1")
        logger.info("PG_STORE: Simulator reset to $1,000")
    except Exception as e:
        logger.error(f"PG_STORE reset: {e}")
