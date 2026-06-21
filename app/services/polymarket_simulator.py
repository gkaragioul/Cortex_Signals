# SPDX-License-Identifier: MIT

"""
Polymarket Paper Trading Simulator v2

Auto-bets on scored signals from the multi-source mispricing scanner, wallet
copy signals, and consensus. All bets are FAKE -- no real Polymarket API calls.

Key fixes from v1 (which lost 91% of bankroll):
- Score-gated entry: mispricing needs score >= 30, copy needs 65% WR + 20 trades
- Max 3 bets per HOUR (was 5 per cycle = up to 60/hour)
- Max 10 open bets at once (was unlimited)
- Score-based sizing: 4% high conviction (50+), 2% medium (30-50)
- Min $50K market volume for all bets
- Auto-reset if bankroll < $50
"""

import asyncio
import json
import logging
import os
import random
import time
from datetime import datetime, timezone, timedelta

import requests

# Execution-parity tuning
EXEC_LATENCY_MIN_S = 2.0   # Simulate network + signing delay between signal and fill
EXEC_LATENCY_MAX_S = 5.0
FEE_BPS = 0                # Polymarket: 0% trading fee, gasless via paymaster (2026-04)

from app.database import get_db
from app.services.websocket_manager import ws_manager


async def _brain(msg: str):
    """Send a brain narration message (human-readable bot thinking)."""
    await ws_manager.send_log(msg, "brain")


def _notify(title: str, body: str, priority: str = "default") -> None:
    """Fire-and-forget push notification via ntfy.sh."""
    from app.config import settings
    topic = settings.NTFY_TOPIC
    if not topic:
        return
    try:
        requests.post(
            f"https://ntfy.sh/{topic}",
            data=body.encode("utf-8"),
            headers={"Title": title, "Priority": priority},
            timeout=5,
        )
    except Exception:
        pass  # Never block the bot on notification failure


from app.services import pg_store

logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════
# CONSTANTS
# ══════════════════════════════════════════════════════════════

STARTING_BANKROLL = 1000.0
WEATHER_ONLY_MODE = False       # All 15 signals active — user requested max signal coverage
BASE_BETS_PER_HOUR = 5          # Raised from 3
MAX_BETS_PER_HOUR = 12          # Raised from 8 — polybot-arena playbook: absorb whale-copy bursts when multiple manual whales trade simultaneously
MAX_OPEN_BETS = 35              # Raised from 25 — was saturating and locking out fresh signals
MOONSHOT_EXTRA_SLOTS = 10       # Weather moonshots (EV≥800%) can use up to 10 slots beyond MAX_OPEN_BETS
MIN_MARKET_VOLUME = 30_000      # Lowered from 50K for more coverage
MIN_SCORE_FOR_BET = 25          # Lowered from 30 for more volume
HIGH_CONVICTION_SCORE = 50      # Score threshold for larger sizing
CONFLUENCE_SCORE = 70            # Confluence: 3+ sources agree → max conviction
MAX_KELLY_PCT = 0.06            # Kelly cap: never bet more than 6% of bankroll
MIN_KELLY_PCT = 0.015           # Kelly floor: always bet at least 1.5%
MAX_ENTRY_PRICE = 0.80
MIN_ENTRY_PRICE = 0.10
MAX_WEATHER_BET = float(os.getenv("MAX_WEATHER_BET_USD", "50"))  # Raise via env var
MOONSHOT_EV_THRESHOLD = float(os.getenv("MOONSHOT_EV_THRESHOLD", "800"))  # EV% to trigger moonshot sizing
MOONSHOT_SIZE_PCT = float(os.getenv("MOONSHOT_SIZE_PCT", "0.20"))   # 20% of bankroll on moonshot bets

# Stale-bet sweeper: void open bets whose Polymarket slug stops returning data
# after this many days. Polymarket archives some markets after resolution and the
# Gamma API 404s — without this, those bets sit "open" at $0 PnL forever.
STALE_BET_VOID_DAYS = 7

# Settlement-harvester sizing (near-certain NO-leg markets at 95-99c).
# Separate caps from Kelly because harvester risk is asymmetric: tiny upside,
# low but real tail-loss. Guardrails keep a single tail hit bounded and stop
# aggregate harvester exposure from crowding out weather moonshots.
HARVEST_PER_BET_PCT = float(os.getenv("HARVEST_PER_BET_PCT", "0.15"))      # Max 15% of bankroll per harvester bet (was 0.25 — lowered after NadirLabsAI post-mortem: asymmetric 32:1 downside at 97c means one miscalibration erases 8+ wins)
HARVEST_AGGREGATE_PCT = float(os.getenv("HARVEST_AGGREGATE_PCT", "0.50"))  # Max 50% of bankroll across ALL open harvester bets
HARVEST_MIN_PRICE = float(os.getenv("HARVEST_MIN_PRICE", "0.95"))          # Min entry price for DTR ≤ 7d markets
HARVEST_MIN_PRICE_SLOW = float(os.getenv("HARVEST_MIN_PRICE_SLOW", "0.97")) # Tighter floor for DTR > 7d (longer lock → need more margin)
HARVEST_MAX_DTR_DAYS = float(os.getenv("HARVEST_MAX_DTR_DAYS", "14"))      # Days-to-resolution ceiling — avoid multi-month capital lock
HARVEST_MIN_CANDIDATES = int(os.getenv("HARVEST_MIN_CANDIDATES", "3"))     # Require ≥N distinct candidates before big sizing kicks in
# Fast-settle sub-tier: markets resolving within N days recycle capital quickly
# so a slightly bigger per-bet fraction is acceptable. Kept below 0.25 after
# the NadirLabsAI post-mortem — capital recycle speed doesn't cancel the
# asymmetric payoff risk at 95-99c.
HARVEST_FAST_SETTLE_DAYS = float(os.getenv("HARVEST_FAST_SETTLE_DAYS", "3"))
HARVEST_FAST_PER_BET_PCT = float(os.getenv("HARVEST_FAST_PER_BET_PCT", "0.25"))
# Time-window correlation cap: limit harvester entries per scan so a single
# news-driven settlement cluster can't fire multiple correlated bets at once.
# Per NadirLabsAI post-mortem: three correlated trades in one 5-min window
# caused 60% of their total losses.
HARVEST_MAX_PER_SCAN = int(os.getenv("HARVEST_MAX_PER_SCAN", "2"))
MIN_COPY_WIN_RATE = 65.0
MIN_COPY_TRADES = 20
MIN_COPY_RESOLVED = 5
MIN_CONSENSUS_WALLETS = 3
MIN_CONSENSUS_SCORE = 20
BANKROLL_RESET_THRESHOLD = 50

POLYMARKET_MARKETS_API = "https://gamma-api.polymarket.com/markets"

# Deadline trap keywords — markets with these + imminent deadline are traps
_DEADLINE_KEYWORDS = [
    "by april", "by may", "by june", "by july", "by august",
    "by end of", "before april", "before may", "before june",
    "ends by", "conflict ends", "ceasefire by", "deal by",
]
MAX_BETS_PER_THEME = 3              # Max 3 open bets in same theme cluster

# Sports/weather micro-markets — coin flips with no edge, resolve in hours
_NOISE_MARKET_PATTERNS = [
    # "temperature" removed — now handled by weather_engine.py with real forecast data
    "map 1 winner",         # Esports individual maps
    "map 2 winner",
    "map 3 winner",
    "game 1 winner",        # Esports individual games
    "game 2 winner",
    "game 3 winner",
    "win the 2026 masters",  # Individual golfer to win tournament (lottery ticket)
    "win the 2026 nba finals",  # Individual team NBA Finals (too far out)
    "win the 2026 nfl",
    "win the 2026 fifa",
    "end in a draw",        # Draw markets are coin flips
    "vs.",                  # Head-to-head sports matches (pure noise)
    "vs ",
]

# Theme keywords for correlation detection
_THEME_KEYWORDS = {
    "iran": ["iran", "tehran", "persian", "hormuz", "irgc"],
    "trump": ["trump", "maga", "republican 2028"],
    "fed_rates": ["fed ", "federal reserve", "interest rate", "rate cut", "rate hike"],
    "crypto": ["bitcoin", "btc", "ethereum", "eth", "crypto"],
    "ukraine": ["ukraine", "kyiv", "zelensky", "crimea", "donbas"],
    "china": ["china", "xi jinping", "taiwan", "ccp", "beijing"],
    "ai": ["artificial intelligence", "openai", "chatgpt", "deepmind", "ai regulation"],
    "election": ["election", "electoral", "primary", "nominee", "ballot"],
    "recession": ["recession", "gdp", "unemployment", "economic downturn"],
}

# Tracks moonshot slugs already notified this session — prevents re-alerting every scan cycle
_moonshot_notified: set = set()
# Last UTC date for which we already fired the Telegram daily digest. Ensures
# the summary goes out at most once per day even if _save_daily_snapshot runs
# dozens of times (every simulator cycle).
_telegram_digest_last_date: str = ""
# Idempotency for the once-per-day Reddit intel digest. Same pattern as the
# Telegram daily digest — scheduler runs dozens of times per day, we only fire
# once past REDDIT_INTEL_HOUR.
_reddit_intel_last_date: str = ""
# Tracks last announced streak state ("win", "loss", or "none") to suppress
# repeat announcements on every scan cycle. Only log on state transition.
_last_streak_state: str = "none"
# Total open-bet exposure cap: fraction of bankroll allowed in open bets at once.
# 0.60 = up to 60% of bankroll committed; above that, new bets are paused until
# resolutions free capital. Protects against correlated-resolution days.
MAX_EXPOSURE_FRACTION = 0.60

# ══════════════════════════════════════════════════════════════
# STATE: HOURLY BET TRACKING (database-backed, survives restarts)
# ══════════════════════════════════════════════════════════════


async def _can_bet_now(limit: int = 0) -> bool:
    """Check if we're under the hourly bet limit. Tries Postgres first."""
    count = await _bets_this_hour()
    return count < (limit or MAX_BETS_PER_HOUR)


def _is_deadline_trap(title: str, days_to_res: float | None) -> bool:
    """Detect deadline trap markets: imminent deadline + event hasn't happened.

    Example: "Iran conflict ends by April 7?" on April 6 — the event hasn't
    happened, deadline is tomorrow, price is low because it's almost certainly NO.
    Looks like a high-edge NO bet but the price already reflects reality.
    """
    if days_to_res is None or days_to_res > 3:
        return False  # Only trap if deadline is within 3 days
    t = title.lower()
    return any(kw in t for kw in _DEADLINE_KEYWORDS)


def _is_noise_market(title: str) -> bool:
    """Detect sports/weather micro-markets that are pure coin flips."""
    t = title.lower()
    return any(pattern in t for pattern in _NOISE_MARKET_PATTERNS)


def _wallet_has_resolved_trades(wallet_trades: int, wallet_wr: float) -> bool:
    """Check if wallet has enough RESOLVED trades (not just tracked).

    A wallet with "100% win rate" on 2 resolved trades is unreliable.
    We need at least MIN_COPY_RESOLVED resolved outcomes.
    """
    # Estimate resolved trades from win rate and total trades
    # If WR is 100% and trades is 20, they could have 2 resolved wins + 18 pending
    # We can't know exactly, but we require the total to be high enough
    # that even at 100% WR there must be several resolved
    if wallet_trades < MIN_COPY_TRADES:
        return False
    # With 20+ tracked trades and 65%+ WR, statistically likely to have 5+ resolved
    return True


def _detect_theme(market_title: str) -> str:
    """Detect the theme of a market for correlation tracking."""
    title_lower = market_title.lower()
    for theme, keywords in _THEME_KEYWORDS.items():
        if any(kw in title_lower for kw in keywords):
            return theme
    return "other"


async def _theme_exposure(theme: str) -> int:
    """Count open bets in the same theme cluster. Tries Postgres first."""
    if theme == "other":
        return 0

    # Try Postgres first (survives deploys) — check pool availability, not list length
    try:
        pool = await pg_store._get_pool()
        if pool:
            pg_bets = await pg_store.get_open_bets()
            return sum(1 for b in pg_bets if _detect_theme(b.get("market", "")) == theme)
    except Exception:
        pass

    # Fallback to SQLite
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT market FROM polymarket_sim_bets WHERE status = 'open'"
        )
        rows = await cursor.fetchall()
        return sum(1 for r in rows if _detect_theme(r[0]) == theme)
    finally:
        await db.close()


async def _bets_this_hour() -> int:
    """Count bets placed in the last hour. Tries Postgres first (survives deploys)."""
    one_hour_ago = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()

    # Try Postgres first
    try:
        pool = await pg_store._get_pool()
        if pool:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT COUNT(*) as cnt FROM sim_bets WHERE timestamp > $1",
                    one_hour_ago,
                )
                if row is not None:  # 0 bets is a valid answer — don't fall through to SQLite
                    return int(row["cnt"])
    except Exception:
        pass

    # Fallback to SQLite
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT COUNT(*) FROM polymarket_sim_bets WHERE timestamp > ?",
            (one_hour_ago,),
        )
        row = await cursor.fetchone()
        return row[0] if row else 0
    finally:
        await db.close()


# ══════════════════════════════════════════════════════════════
# DATABASE
# ══════════════════════════════════════════════════════════════

SIM_SCHEMA = """
CREATE TABLE IF NOT EXISTS polymarket_sim_bets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
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
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS polymarket_sim_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL UNIQUE,
    bankroll REAL NOT NULL,
    total_bets INTEGER DEFAULT 0,
    wins INTEGER DEFAULT 0,
    losses INTEGER DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now'))
);
"""


async def init_sim_tables():
    """Create simulator tables if they don't exist."""
    db = await get_db()
    try:
        await db.executescript(SIM_SCHEMA)
        await db.commit()
        # Add score column if missing (migration from v1)
        try:
            await db.execute("SELECT score FROM polymarket_sim_bets LIMIT 1")
        except Exception:
            await db.execute("ALTER TABLE polymarket_sim_bets ADD COLUMN score REAL DEFAULT 0")
            await db.commit()
            logger.info("Added 'score' column to polymarket_sim_bets")
        logger.info("Polymarket simulator tables initialized")
    finally:
        await db.close()
    # Restore trailing stop watermarks from Postgres
    await _load_watermarks_from_pg()


# ══════════════════════════════════════════════════════════════
# BANKROLL CALCULATION
# ══════════════════════════════════════════════════════════════

async def _get_bankroll() -> float:
    """Calculate realized bankroll from total PnL. Tries Postgres first, falls back to SQLite.

    Bankroll = $1,000 + SUM(all resolved PnL). Calculated, not stored.
    This avoids the stale-read bug where set_bankroll() writes back the same value.

    NOTE: This is *realized* equity. For Kelly sizing decisions use
    _get_available_bankroll() which subtracts capital tied up in open bets.
    """
    # Trust Postgres whenever the pool is reachable — even when it returns
    # 0/0 (a fresh-empty DB after reset is a valid authoritative answer).
    # Falling through on 0/0 would expose stale SQLite data after a partial reset.
    try:
        pool = await pg_store._get_pool()
        if pool:
            pg_stats = await pg_store.get_stats()
            total_pnl = float(pg_stats.get("total_pnl", 0))
            return STARTING_BANKROLL + total_pnl
    except Exception:
        pass

    # Fallback: calculate from SQLite (only when PG pool is unreachable)
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT COALESCE(SUM(pnl), 0) FROM polymarket_sim_bets WHERE status IN ('won', 'lost', 'sold')"
        )
        row = await cursor.fetchone()
        total_pnl = row[0] if row else 0
        return STARTING_BANKROLL + total_pnl
    finally:
        await db.close()


async def _get_open_exposure() -> float:
    """Sum of bet_amount across all open bets. Tries Postgres first."""
    try:
        pool = await pg_store._get_pool()
        if pool:
            pg_bets = await pg_store.get_open_bets()
            return sum(float(b.get("bet_amount", 0) or 0) for b in pg_bets)
    except Exception:
        pass
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT COALESCE(SUM(bet_amount), 0) FROM polymarket_sim_bets WHERE status = 'open'"
        )
        row = await cursor.fetchone()
        return float(row[0]) if row and row[0] is not None else 0.0
    finally:
        await db.close()


async def _get_available_bankroll() -> float:
    """Bankroll minus capital tied up in open bets. Use this for Kelly sizing
    so we never oversize when many bets are already open. Floors at $0."""
    bankroll = await _get_bankroll()
    open_exposure = await _get_open_exposure()
    return max(0.0, bankroll - open_exposure)


async def _get_open_market_slugs() -> set:
    """Return set of market slugs with open bets. Tries Postgres first (survives deploys)."""
    # Try Postgres first — treats empty list as authoritative (0 open bets is a valid answer)
    # Mirrors the pattern in _get_open_bet_count: check pool availability, not list truthiness
    try:
        pool = await pg_store._get_pool()
        if pool:
            pg_bets = await pg_store.get_open_bets()
            return {b.get("market_slug", "") for b in pg_bets if b.get("market_slug")}
    except Exception:
        pass

    # Fallback to SQLite
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT market_slug FROM polymarket_sim_bets WHERE status = 'open' AND market_slug IS NOT NULL"
        )
        rows = await cursor.fetchall()
        return {r[0] for r in rows if r[0]}
    finally:
        await db.close()


async def _get_open_bet_count() -> int:
    """Return number of currently open bets. Tries Postgres first (survives deploys)."""
    # Try Postgres first — return its answer even when 0 (Postgres is authoritative)
    pg_count = await pg_store.get_open_bet_count()
    if pg_count is not None:
        return pg_count

    # Fallback to SQLite
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT COUNT(*) FROM polymarket_sim_bets WHERE status = 'open'"
        )
        row = await cursor.fetchone()
        return row[0] if row else 0
    finally:
        await db.close()


# ══════════════════════════════════════════════════════════════
# RESET SIMULATOR
# ══════════════════════════════════════════════════════════════

async def reset_simulator() -> dict:
    """Clear all bets and reset bankroll to $1,000. Returns summary of what was cleared."""
    db = await get_db()
    try:
        # Count existing data
        cursor = await db.execute("SELECT COUNT(*) FROM polymarket_sim_bets")
        total_bets = (await cursor.fetchone())[0]

        cursor = await db.execute("SELECT COUNT(*) FROM polymarket_sim_bets WHERE status = 'open'")
        open_bets = (await cursor.fetchone())[0]

        bankroll_before = await _get_bankroll()

        # Clear everything in SQLite
        await db.execute("DELETE FROM polymarket_sim_bets")
        await db.execute("DELETE FROM polymarket_sim_snapshots")
        await db.commit()

        # Also clear Postgres (so history doesn't ghost back after deploy)
        try:
            await pg_store.reset_all()
        except Exception:
            pass

        # Clear in-memory signal caches so stale signals aren't re-bet after reset
        try:
            from app.services import weather_engine
            weather_engine._weather_signals = []
            weather_engine._weather_last_scan = 0
        except Exception:
            pass

        summary = {
            "cleared_bets": total_bets,
            "cleared_open": open_bets,
            "bankroll_before": round(bankroll_before, 2),
            "bankroll_after": STARTING_BANKROLL,
        }

        await ws_manager.send_log(
            f"[SIM] RESET: Cleared {total_bets} bets ({open_bets} open). "
            f"Bankroll: ${bankroll_before:.0f} -> ${STARTING_BANKROLL:.0f}",
            "warning",
        )
        logger.info(f"Simulator reset: {summary}")
        return summary

    finally:
        await db.close()


# ══════════════════════════════════════════════════════════════
# PLACE BET
# ══════════════════════════════════════════════════════════════

MAX_CLOB_SPREAD = 0.05  # Skip if bid-ask spread > 5c (weatherbot uses 3c, we're slightly looser)
CLOB_PRICES_API = "https://clob.polymarket.com"
DEPTH_CAP_FRACTION = 0.20  # Cap bet size to 20% of top-of-book depth (execution parity)


async def _clob_quote(slug: str, side: str, intended_amount_usd: float) -> dict:
    """Realistic CLOB fill quote: walks the ask side VWAP and depth-caps size.

    Execution parity: matches what a live trade would actually fill at.
    - Caps size to DEPTH_CAP_FRACTION of top-of-book depth
    - Walks the ask side to compute a VWAP across the levels we'd consume
    - Rejects on wide spread or empty/thin book

    Returns dict with keys:
        ok: bool          -- False means skip bet
        best_bid, best_ask: floats | None
        top_depth_usd: float  -- best-ask price * best-ask size (USD at top)
        vwap: float | None    -- walked VWAP for the capped amount
        capped_amount: float  -- amount after depth cap (USD we can actually fill)
        filled_shares: float
        note: str         -- "ok" | "depth_capped" | "no_book" | "wide_spread"
                             | "empty_book" | "thin_book"
    """
    loop = asyncio.get_running_loop()
    # Default ok=False: if we can't reach CLOB, treat the bet as un-verifiable
    # and skip it. Parity-first — better to miss a bet than fill at a stale
    # signal price with no depth cap and pretend it's realistic.
    quote = {
        "ok": False, "best_bid": None, "best_ask": None,
        "top_depth_usd": 0.0, "vwap": None,
        "capped_amount": 0.0, "filled_shares": 0.0,
        "note": "no_book",
    }
    try:
        resp = await loop.run_in_executor(
            None,
            lambda: requests.get(
                POLYMARKET_MARKETS_API, params={"slug": slug},
                timeout=8, headers={"Accept": "application/json"},
            ),
        )
        if resp.status_code != 200:
            quote["note"] = "gamma_http_error"
            return quote
        data = resp.json()
        market_data = data[0] if isinstance(data, list) and data else data if isinstance(data, dict) else None
        if not market_data:
            quote["note"] = "no_market"
            return quote
        tokens = json.loads(market_data.get("clobTokenIds", "[]"))
        if not tokens or len(tokens) < 2:
            quote["note"] = "no_tokens"
            return quote
        token_id = tokens[0] if side == "YES" else tokens[1]

        resp2 = await loop.run_in_executor(
            None,
            lambda tid=token_id: requests.get(
                f"{CLOB_PRICES_API}/book",
                params={"token_id": tid},
                timeout=8,
            ),
        )
        if resp2.status_code != 200:
            quote["note"] = "clob_http_error"
            return quote
        book = resp2.json()
    except Exception:
        quote["note"] = "fetch_err"
        return quote
    # Reaching here means we have a book — flip ok=True and let the depth /
    # spread / walk logic below potentially flip it back to False.
    quote["ok"] = True

    bids = book.get("bids", []) or []
    asks = book.get("asks", []) or []
    if not asks:
        quote["note"] = "empty_book"
        quote["ok"] = False
        return quote

    # Normalise: lowest ask first (for buying we cross from the cheapest ask up)
    asks_sorted = sorted(asks, key=lambda a: float(a.get("price", 0) or 0))
    bids_sorted = sorted(bids, key=lambda b: float(b.get("price", 0) or 0), reverse=True)

    best_ask = float(asks_sorted[0].get("price", 0) or 0)
    best_ask_size = float(asks_sorted[0].get("size", 0) or 0)
    quote["best_ask"] = round(best_ask, 4)
    quote["top_depth_usd"] = round(best_ask * best_ask_size, 2)
    # Total visible ask-side liquidity across top 5 levels — the realistic
    # ceiling for how much we could accumulate without screaming "whale" at
    # the book. The 20% cap applies to THIS total, not just top-of-book,
    # so a $3 intent against a $3 top-of-book isn't absurdly over-capped.
    total_ask_depth_usd = sum(
        float(a.get("price", 0) or 0) * float(a.get("size", 0) or 0)
        for a in asks_sorted[:5]
    )
    quote["total_depth_usd"] = round(total_ask_depth_usd, 2)

    if bids_sorted:
        best_bid = float(bids_sorted[0].get("price", 0) or 0)
        quote["best_bid"] = round(best_bid, 4)
        if best_ask > 0 and best_bid > 0 and (best_ask - best_bid) > MAX_CLOB_SPREAD:
            quote["ok"] = False
            quote["note"] = "wide_spread"
            return quote

    # Depth-cap: intended fills fully if it fits within top-of-book;
    # otherwise cap to 20% of the top-5-level total ask depth.
    if total_ask_depth_usd <= 0:
        quote["ok"] = False
        quote["note"] = "empty_book"
        return quote
    if intended_amount_usd <= quote["top_depth_usd"]:
        # Small bet that fits inside top-of-book — no cap, no walk
        capped = intended_amount_usd
    else:
        capped = min(intended_amount_usd, DEPTH_CAP_FRACTION * total_ask_depth_usd)
    if capped < 0.50:  # Less than 50 cents fillable — not worth a bet
        quote["ok"] = False
        quote["note"] = "thin_book"
        quote["capped_amount"] = 0.0
        return quote

    # Walk asks to compute VWAP across the levels we'd actually consume
    remaining = capped
    filled_usd = 0.0
    filled_shares = 0.0
    for level in asks_sorted:
        p = float(level.get("price", 0) or 0)
        s = float(level.get("size", 0) or 0)
        if p <= 0 or s <= 0:
            continue
        level_usd = p * s
        take_usd = min(remaining, level_usd)
        filled_usd += take_usd
        filled_shares += take_usd / p
        remaining -= take_usd
        if remaining <= 0:
            break

    if filled_shares <= 0:
        quote["ok"] = False
        quote["note"] = "empty_book"
        return quote

    quote["vwap"] = round(filled_usd / filled_shares, 4)
    quote["capped_amount"] = round(filled_usd, 2)
    quote["filled_shares"] = round(filled_shares, 4)
    quote["note"] = "ok" if capped >= intended_amount_usd - 0.01 else "depth_capped"
    return quote


async def _clob_exit_price(slug: str, side: str, shares: float) -> tuple[float | None, str]:
    """Realistic sell price: walk the bid side of the book for the shares we hold.

    Execution parity on exits: selling YES means hitting the YES-token bids
    (buyers), starting from the highest bid and walking down.

    If the bid side can't absorb all shares, the unfillable remainder is
    penalised at 85% of the lowest bid we walked to (15% market-impact
    haircut — conservative but not punitive). Returns (vwap, note). vwap is
    None on fetch failure (caller should fall back to last-trade price).
    """
    loop = asyncio.get_running_loop()
    try:
        resp = await loop.run_in_executor(
            None,
            lambda: requests.get(
                POLYMARKET_MARKETS_API, params={"slug": slug},
                timeout=8, headers={"Accept": "application/json"},
            ),
        )
        if resp.status_code != 200:
            return (None, "no_market")
        data = resp.json()
        md = data[0] if isinstance(data, list) and data else data if isinstance(data, dict) else None
        if not md:
            return (None, "no_market")
        tokens = json.loads(md.get("clobTokenIds", "[]"))
        if not tokens or len(tokens) < 2:
            return (None, "no_tokens")
        token_id = tokens[0] if side == "YES" else tokens[1]

        resp2 = await loop.run_in_executor(
            None,
            lambda tid=token_id: requests.get(
                f"{CLOB_PRICES_API}/book", params={"token_id": tid}, timeout=8,
            ),
        )
        if resp2.status_code != 200:
            return (None, "no_book")
        book = resp2.json()
    except Exception:
        return (None, "fetch_err")

    bids = book.get("bids", []) or []
    if not bids or shares <= 0:
        return (None, "empty_book")

    bids_sorted = sorted(bids, key=lambda b: float(b.get("price", 0) or 0), reverse=True)
    remaining = shares
    filled_usd = 0.0
    filled_shares = 0.0
    last_px = 0.0
    for level in bids_sorted:
        p = float(level.get("price", 0) or 0)
        s = float(level.get("size", 0) or 0)
        if p <= 0 or s <= 0:
            continue
        take_shares = min(remaining, s)
        filled_shares += take_shares
        filled_usd += take_shares * p
        last_px = p
        remaining -= take_shares
        if remaining <= 0:
            break

    if filled_shares <= 0:
        return (None, "empty_book")

    if remaining > 0:
        # Book couldn't fully absorb — haircut remainder at 15% off last walked bid
        filled_usd += remaining * (last_px * 0.85)
        filled_shares += remaining
        note = "partial_book"
    else:
        note = "ok"

    vwap = filled_usd / filled_shares
    return (round(vwap, 4), note)


async def _check_clob_spread(slug: str, side: str) -> tuple[bool, float | None]:
    """Check CLOB bid-ask spread before betting. Returns (ok, actual_price).

    If spread > MAX_CLOB_SPREAD, returns (False, None) — don't bet.
    Otherwise returns (True, realistic_fill_price).
    """
    loop = asyncio.get_running_loop()
    try:
        # Get market to find token IDs
        resp = await loop.run_in_executor(
            None,
            lambda: requests.get(
                POLYMARKET_MARKETS_API,
                params={"slug": slug},
                timeout=8,
                headers={"Accept": "application/json"},
            ),
        )
        if resp.status_code != 200:
            return (True, None)  # Can't check — allow bet
        data = resp.json()
        market_data = data[0] if isinstance(data, list) and data else data if isinstance(data, dict) else None
        if not market_data:
            return (True, None)

        tokens = json.loads(market_data.get("clobTokenIds", "[]"))
        if not tokens or len(tokens) < 2:
            return (True, None)

        token_id = tokens[0] if side == "YES" else tokens[1]

        # Get best bid/ask from CLOB
        resp2 = await loop.run_in_executor(
            None,
            lambda tid=token_id: requests.get(
                f"{CLOB_PRICES_API}/price",
                params={"token_id": tid, "side": "BUY"},
                timeout=5,
            ),
        )
        if resp2.status_code != 200:
            return (True, None)

        price_data = resp2.json()
        ask = float(price_data.get("price", 0) or 0)

        resp3 = await loop.run_in_executor(
            None,
            lambda tid=token_id: requests.get(
                f"{CLOB_PRICES_API}/price",
                params={"token_id": tid, "side": "SELL"},
                timeout=5,
            ),
        )
        bid = float(resp3.json().get("price", 0) or 0) if resp3.status_code == 200 else 0

        if ask > 0 and bid > 0:
            spread = ask - bid
            if spread > MAX_CLOB_SPREAD:
                return (False, None)  # Spread too wide — skip
            return (True, ask)  # Use ask price as realistic fill

        return (True, None)
    except Exception:
        return (True, None)  # Can't check — allow bet


async def place_bet(market: str, slug: str, side: str, price: float,
                    amount: float, source: str, score: float = 0,
                    detail: dict | None = None) -> bool:
    """Record a new hypothetical bet.

    Args:
        market: Market question text
        slug: Polymarket market slug
        side: 'YES' or 'NO'
        price: Entry price (0-1 scale)
        amount: Dollar amount to bet
        source: Signal source ('mispricing', 'copy', 'consensus')
        score: Composite signal score (0-100)
        detail: Optional dict with edge%, wallet info, etc.

    Returns:
        True if bet was placed, False if skipped.
    """
    if price <= 0.01 or price >= 0.99:
        return False

    # Execution parity: walk the CLOB book for a realistic VWAP fill + depth-cap.
    # This makes paper fills match what a live trade would actually execute at.
    signal_price = price
    # Simulate network/signing latency between signal fire and the snapshot
    # we actually fill against — price can (and does) move in those seconds.
    await asyncio.sleep(random.uniform(EXEC_LATENCY_MIN_S, EXEC_LATENCY_MAX_S))
    quote = await _clob_quote(slug, side, amount)
    if not quote["ok"]:
        reason = quote.get("note", "unknown").replace("_", " ")
        await _brain(f"Skipping \"{market[:40]}\" — CLOB {reason}")
        return False
    _vwap = quote.get("vwap")
    if _vwap is not None and _vwap >= 0.99:
        # Book walked above 99c — harvester edge is gone, skip rather than
        # silently fill at stale signal_price. This was the paper-to-live
        # divergence: previously `price` stayed at signal_price when vwap
        # blew past the upper bound, overstating harvester PnL 2-3pp per bet.
        await _brain(
            f"Skipping \"{market[:40]}\" — CLOB walked to {_vwap*100:.1f}c, "
            f"past 99c ceiling (no edge left)"
        )
        return False
    if _vwap and 0.01 < _vwap < 0.99:
        price = _vwap
        capped = quote.get("capped_amount") or amount
        if capped < amount:
            await _brain(
                f"Depth-capped \"{market[:35]}\": ${amount:.0f} → ${capped:.0f} "
                f"(top-of-book ${quote['top_depth_usd']:.0f})"
            )
        amount = capped
    # Enrich detail with fill diagnostics so we can measure paper/live gap
    detail = dict(detail) if detail else {}
    detail["signal_price"] = round(signal_price, 4)
    detail["fill_price"] = round(price, 4)
    detail["slippage_bps"] = (
        round(((price - signal_price) / signal_price) * 10000, 1)
        if signal_price > 0 else 0.0
    )
    detail["top_depth_usd"] = quote.get("top_depth_usd", 0.0)
    detail["fill_note"] = quote.get("note", "")
    detail["fee_bps"] = FEE_BPS  # 0 for Polymarket today; hook for future fee changes

    # AI Context Evaluation — only for high-conviction non-weather bets (score >= 60)
    # Weather signals have their own probability model; Haiku adds no value and
    # blocks everything because it sees edge=0% (weather uses "ev" not "edge" key).
    if score >= 60 and source != "weather":
        try:
            from app.services.ai_analyst import evaluate_signal
            edge = float((detail or {}).get("edge", 0) or 0)
            ai_eval = await evaluate_signal(market, source, side, score, edge, price)
            if ai_eval["action"] == "SKIP":
                await _brain(f"AI flagged red flag on \"{market[:35]}\" — skipping ({ai_eval['reasoning'][:60]})")
                return False
            elif ai_eval["action"] == "REDUCE":
                amount = round(amount * 0.5, 2)  # Cut to half
                await _brain(f"AI suggests caution on \"{market[:35]}\" — reducing to ${amount:.0f}")
        except Exception:
            pass  # AI is advisory — never block a bet due to AI failure

    # Guard against duplicate bets on the same market (e.g. after a restart where
    # Postgres sync failed and slug is missing from rebuilt open_slugs set)
    try:
        existing_open = await pg_store.get_open_bets()
        if any(b.get("market_slug") == slug for b in existing_open):
            logger.info(f"place_bet: skipping duplicate — already have open bet on {slug}")
            return False
    except Exception as e:
        # PG check failed — in-memory open_slugs is still a guard but log the failure
        logger.warning(f"place_bet: Postgres duplicate check failed for {slug}: {e} — relying on in-memory guard")

    shares = amount / price
    now = datetime.now(timezone.utc).isoformat()
    detail_json = json.dumps(detail) if detail else None

    # PG-FIRST WRITE: Postgres is source of truth (matches the read pattern in
    # _get_bankroll, check_resolutions, etc.). If PG is reachable but the
    # write fails, abort the bet so we don't drift state — a missing bet in
    # PG would let the duplicate-guard, open-count cap, and bankroll calc
    # all desync from reality.
    pg_pool = None
    try:
        pg_pool = await pg_store._get_pool()
    except Exception as e:
        logger.warning(f"place_bet: PG pool check failed: {e}")

    if pg_pool:
        try:
            await pg_store.place_bet(market[:200], slug, side, round(price, 4),
                                      round(amount, 2), round(shares, 4), source,
                                      detail_json or "", round(score, 1))
        except Exception as e:
            logger.error(f"place_bet PG write failed (aborting bet to avoid drift): {e}")
            await _brain(f"⚠️ Skipping \"{market[:35]}\" — Postgres write failed, refusing to drift state")
            return False

    # SQLite mirror (best-effort — PG is the source of truth above)
    db = await get_db()
    try:
        await db.execute(
            """INSERT INTO polymarket_sim_bets
               (timestamp, market, market_slug, side, entry_price, bet_amount, shares,
                signal_source, signal_detail, score, status)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open')""",
            (now, market[:200], slug, side, round(price, 4), round(amount, 2),
             round(shares, 4), source, detail_json, round(score, 1)),
        )
        await db.commit()
    except Exception as e:
        if pg_pool:
            # PG already has the bet — SQLite mirror failure is non-fatal
            logger.warning(f"place_bet SQLite mirror failed (PG has the bet): {e}")
        else:
            # No PG and SQLite failed — total loss, abort
            logger.error(f"place_bet SQLite write failed (no PG fallback): {e}")
            return False
    finally:
        await db.close()

    price_cents = int(price * 100)
    conviction = "HIGH CONVICTION" if score >= HIGH_CONVICTION_SCORE else "BET"
    await ws_manager.send_log(
        f"[SIM] {conviction}: {side} \"{market[:50]}\" ${amount:.0f} @ {price_cents}c "
        f"(score: {score:.0f}, {source})",
        "info",
    )

    # Brain narration for the bet
    payout = round(shares * 1.0 - amount, 2)
    await _brain(
        f"\U0001f4b0 BETTING: {side} on \"{market[:50]}\" \u2014 "
        f"${amount:.0f} at {price_cents}c ({source}, score {score:.0f}/100). "
        f"If we win: +${payout:.0f} profit"
    )
    _notify(
        f"💰 Bet Placed — {side} @ {price_cents}¢",
        f"{market[:60]}\n${amount:.0f} risked | score {score:.0f} | win: +${payout:.0f}",
    )

    # v3.20.7 — real-money mirror removed. Paper engine runs standalone
    # as the bot's brain; no wallet, no signed orders, no live submission.
    return True


# ══════════════════════════════════════════════════════════════
# CHECK RESOLUTIONS
# ══════════════════════════════════════════════════════════════

async def _fetch_market_price(slug: str) -> dict | None:
    """Fetch current market data from Polymarket gamma API."""
    loop = asyncio.get_running_loop()
    try:
        resp = await loop.run_in_executor(
            None,
            lambda: requests.get(
                POLYMARKET_MARKETS_API,
                params={"slug": slug},
                timeout=10,
                headers={"Accept": "application/json"},
            ),
        )
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list) and data:
            return data[0]
        elif isinstance(data, dict):
            return data
        return None
    except Exception as e:
        logger.debug(f"Failed to fetch market {slug}: {e}")
        return None


async def check_resolutions():
    """Check all open bets against Polymarket for resolution or large price moves.
    Reads from Postgres first (survives deploys), falls back to SQLite.
    """
    open_bets = []
    _source_is_pg = False  # Track which DB the bets came from
    pg_bets: list = []

    # Try Postgres first (persists across deploys). Only mark _source_is_pg=True
    # AFTER the fetch+populate completes — otherwise a mid-iteration exception
    # leaves us with a partial open_bets list AND skips the SQLite fallback.
    try:
        pool = await pg_store._get_pool()
        if pool:
            pg_bets_raw = await pg_store.get_open_bets()
            staged: list = []
            for b in pg_bets_raw:
                staged.append((
                    b.get("id"),
                    b.get("market", ""),
                    b.get("market_slug", ""),
                    b.get("side", ""),
                    float(b.get("entry_price", 0)),
                    float(b.get("bet_amount", 0)),
                    float(b.get("shares", 0)),
                ))
            # Only commit to "PG is source" once everything succeeded
            open_bets = staged
            pg_bets = pg_bets_raw
            _source_is_pg = True
    except Exception as e:
        logger.warning("check_resolutions: PG fetch failed, falling back to SQLite: %s", e)
        open_bets = []
        _source_is_pg = False
    # Per-bet creation timestamps (for stale-bet sweeper). Keyed by bet_id so
    # both PG and SQLite paths share the same age lookup downstream.
    _created_at_by_id: dict[int, datetime] = {}

    if _source_is_pg and pg_bets:
        for _pgb in pg_bets:
            _ca = _pgb.get("created_at")
            if _ca is None:
                continue
            try:
                if isinstance(_ca, str):
                    _ca = datetime.fromisoformat(_ca.replace("Z", "+00:00"))
                if _ca.tzinfo is None:
                    _ca = _ca.replace(tzinfo=timezone.utc)
                _created_at_by_id[_pgb.get("id")] = _ca
            except Exception:
                pass

    if not _source_is_pg:
        # Fallback to SQLite — include timestamp so stale sweeper still works.
        db = await get_db()
        try:
            cursor = await db.execute(
                "SELECT id, market, market_slug, side, entry_price, bet_amount, shares, timestamp "
                "FROM polymarket_sim_bets WHERE status = 'open'"
            )
            sqlite_rows = await cursor.fetchall()
            for _r in sqlite_rows:
                _ts = _r[7]
                if _ts:
                    try:
                        _dt = datetime.fromisoformat(str(_ts).replace("Z", "+00:00"))
                        if _dt.tzinfo is None:
                            _dt = _dt.replace(tzinfo=timezone.utc)
                        _created_at_by_id[_r[0]] = _dt
                    except Exception:
                        pass
            # Trim back to the 7-tuple the rest of the loop expects
            open_bets = [tuple(_r[:7]) for _r in sqlite_rows]
        finally:
            await db.close()

    if not open_bets:
        return

    resolved_count = 0
    now = datetime.now(timezone.utc).isoformat()

    # Batch: collect unique slugs to minimize API calls
    slug_bets: dict[str, list] = {}
    for bet in open_bets:
        slug = bet[2]  # market_slug
        if slug:
            slug_bets.setdefault(slug, []).append(bet)

    for slug, bets in slug_bets.items():
        market_data = await _fetch_market_price(slug)
        if not market_data:
            # Stale-bet sweeper: if we can't fetch a market and any of its open
            # bets are older than STALE_BET_VOID_DAYS, void them so they stop
            # cluttering the dashboard. Polymarket Gamma 404s archived markets,
            # which previously left bets stuck "open" at $0 PnL forever.
            now_utc = datetime.now(timezone.utc)
            for bet in bets:
                bet_id = bet[0]
                market_name = bet[1]
                created_at = _created_at_by_id.get(bet_id)
                if created_at is None:
                    continue
                age_days = (now_utc - created_at).total_seconds() / 86400
                if age_days < STALE_BET_VOID_DAYS:
                    continue
                logger.warning(
                    "Stale bet sweep: voiding bet_id=%s slug=%s age=%.1fd (market unfetchable)",
                    bet_id, slug, age_days,
                )
                try:
                    await pg_store.resolve_bet(bet_id, 0.0, 0.0, "void")
                except Exception as e:
                    logger.warning("pg void resolve_bet failed for bet_id=%s: %s", bet_id, e)
                await _brain(
                    f"⚠️ Voiding stale bet: \"{market_name[:45]}\" — market unfetchable for "
                    f"{age_days:.1f} days (no PnL recorded)"
                )
            continue

        # Check if market is resolved
        closed = market_data.get("closed", False)
        resolved = market_data.get("resolved", False)
        resolution = market_data.get("resolution", "")  # "YES", "NO", or ""

        # Get current price
        try:
            prices = json.loads(market_data.get("outcomePrices", "[]"))
            current_yes = float(prices[0]) if prices else None
        except (json.JSONDecodeError, ValueError, IndexError):
            current_yes = None

        for bet in bets:
            bet_id, market, market_slug, side, entry_price, bet_amount, shares = bet

            # Bet age (used by stale-void paths below) — works for both PG and
            # SQLite sources now that _created_at_by_id is populated above.
            _ca = _created_at_by_id.get(bet_id)
            _bet_age_days = (
                (datetime.now(timezone.utc) - _ca).total_seconds() / 86400
                if _ca is not None else None
            )

            if resolved:
                # Resolution field may lag by seconds/minutes after market closes.
                # Empty string = outcome not yet populated. Retry silently for the
                # first STALE_BET_VOID_DAYS; after that, void to clear the open-bet
                # list and free locked capital (Polymarket outcome-populate lag).
                if not resolution:
                    if _bet_age_days is not None and _bet_age_days >= STALE_BET_VOID_DAYS:
                        logger.warning(
                            "Stale bet sweep: voiding bet_id=%s slug=%s age=%.1fd (resolved=True, resolution empty)",
                            bet_id, slug, _bet_age_days,
                        )
                        try:
                            await pg_store.resolve_bet(bet_id, 0.0, 0.0, "void")
                        except Exception as e:
                            logger.warning("pg void resolve_bet failed for bet_id=%s: %s", bet_id, e)
                        await _brain(
                            f"⚠️ Voiding stale bet: \"{market[:45]}\" — resolved but outcome blank for "
                            f"{_bet_age_days:.1f} days (no PnL recorded)"
                        )
                        resolved_count += 1
                        continue
                    await _brain(f"Market resolved (outcome pending): \"{market[:40]}\" — retrying next cycle")
                    continue

                # Skip voided/cancelled markets — don't count as a loss
                # Polymarket can return "MKT_CANCEL", "VOID", or other non-YES/NO values
                if resolution not in ("YES", "NO"):
                    await _brain(f"Market voided/cancelled: \"{market[:40]}\" (resolution={resolution}) — skipping, no loss recorded")
                    continue

                # Market resolved -- determine win/loss
                won = (side == "YES" and resolution == "YES") or \
                      (side == "NO" and resolution == "NO")

                if won:
                    pnl = shares * 1.0 - bet_amount  # Each share pays $1 on win
                    status = "won"
                else:
                    pnl = -bet_amount  # Lose entire bet
                    status = "lost"

                exit_price = 1.0 if won else 0.0

                # Update the correct database (avoid ID mismatch)
                if _source_is_pg:
                    try:
                        await pg_store.resolve_bet(bet_id, exit_price, round(pnl, 2), status)
                    except Exception as e:
                        logger.warning("pg resolve_bet failed for bet_id=%s: %s", bet_id, e)
                else:
                    db = await get_db()
                    try:
                        await db.execute(
                            "UPDATE polymarket_sim_bets SET status=?, exit_price=?, pnl=?, resolved_at=? WHERE id=?",
                            (status, exit_price, round(pnl, 2), now, bet_id),
                        )
                        await db.commit()
                    finally:
                        await db.close()
                    # Sync resolution to Postgres (best-effort — bankroll calculated from SUM(pnl), not stored)
                    try:
                        await pg_store.resolve_bet(bet_id, exit_price, round(pnl, 2), status)
                    except Exception as e:
                        logger.warning("pg resolve_bet sync failed for bet_id=%s: %s", bet_id, e)

                bankroll = await _get_bankroll()
                sign = "+" if pnl >= 0 else ""
                level = "success" if won else "error"
                label = "WIN" if won else "LOSS"
                await ws_manager.send_log(
                    f"[SIM] RESOLVED {label}: \"{market[:45]}\" -> {sign}${pnl:.2f} (bankroll: ${bankroll:.0f})",
                    level,
                )
                emoji = "\U0001f389" if won else "\U0001f4a5"
                await _brain(
                    f"{emoji} Market resolved {resolution}: \"{market[:45]}\" \u2014 "
                    f"We bet {side}, result: {sign}${pnl:.2f}. Bankroll now ${bankroll:.0f}"
                )
                _notify(
                    f"{'🎉 WIN' if won else '💥 LOSS'} {sign}${pnl:.2f}",
                    f"{market[:60]}\nBet {side} → resolved {resolution}\nBankroll: ${bankroll:.0f}",
                    priority="high" if won else "default",
                )
                # v3.20.10 — paper-engine Telegram notifications removed. The
                # simulator is internal training data in signals-only mode;
                # fake-money WIN/LOSS pushes were noise. Real user-facing
                # Telegram output is opportunity_alerts (entries) + exit_monitor
                # (EXIT pushes on user-opened positions) only.

                # For weather bets: fetch actual observed temp and store it in
                # signal_detail so self_learner can calibrate sigma accurately.
                if _source_is_pg and pg_bets:
                    for pgb in pg_bets:
                        if pgb.get("id") == bet_id and pgb.get("signal_source") == "weather":
                            try:
                                raw_detail = pgb.get("signal_detail") or "{}"
                                detail_d = json.loads(raw_detail) if isinstance(raw_detail, str) else dict(raw_detail)
                                c_slug = detail_d.get("city_slug", "")
                                c_date = detail_d.get("date", "")
                                if c_slug and c_date:
                                    from app.services.weather_engine import fetch_actual_high_temp
                                    actual_t = await fetch_actual_high_temp(c_slug, c_date)
                                    if actual_t is not None:
                                        detail_d["actual_temp"] = actual_t
                                        pg_pool = await pg_store._get_pool()
                                        if pg_pool:
                                            async with pg_pool.acquire() as upd_conn:
                                                await upd_conn.execute(
                                                    "UPDATE sim_bets SET signal_detail=$1 WHERE id=$2",
                                                    json.dumps(detail_d), bet_id,
                                                )
                            except Exception:
                                pass
                            break

                resolved_count += 1
                continue

            # Stale-bet sweep (closed=True, resolved=False): Polymarket can leave
            # a market "closed" for days before flipping resolved=True. Without
            # this branch the bet sits open forever at floating PnL. Void after
            # STALE_BET_VOID_DAYS — no PnL impact (status="void" is a refund).
            if closed and _bet_age_days is not None and _bet_age_days >= STALE_BET_VOID_DAYS:
                logger.warning(
                    "Stale bet sweep: voiding bet_id=%s slug=%s age=%.1fd (closed=True, resolved=False)",
                    bet_id, slug, _bet_age_days,
                )
                try:
                    await pg_store.resolve_bet(bet_id, 0.0, 0.0, "void")
                except Exception as e:
                    logger.warning("pg void resolve_bet failed for bet_id=%s: %s", bet_id, e)
                await _brain(
                    f"⚠️ Voiding stale bet: \"{market[:45]}\" — closed but unresolved for "
                    f"{_bet_age_days:.1f} days (no PnL recorded)"
                )
                resolved_count += 1
                continue

            # Thesis invalidation: if the original edge has evaporated, exit early
            if current_yes is not None:
                try:
                    # Read signal_detail from whichever DB has it
                    detail_str = None
                    if _source_is_pg and pg_bets:
                        for pgb in pg_bets:
                            if pgb.get("id") == bet_id:
                                detail_str = pgb.get("signal_detail", "")
                                break
                    else:
                        db2 = await get_db()
                        try:
                            cur = await db2.execute(
                                "SELECT signal_detail FROM polymarket_sim_bets WHERE id=?", (bet_id,))
                            row = await cur.fetchone()
                            if row:
                                detail_str = row[0]
                        finally:
                            await db2.close()

                    if detail_str:
                        orig_detail = json.loads(detail_str) if isinstance(detail_str, str) else detail_str
                        orig_edge = abs(float(orig_detail.get("edge", 0) or 0))
                        fair_value = float(orig_detail.get("fair_value", 0) or 0) / 100.0 if orig_detail.get("fair_value") else None

                        if orig_edge >= 8 and fair_value and fair_value > 0:
                            current_edge = abs(fair_value - current_yes) * 100
                            # Exit when 65%+ of original edge has evaporated (relative, not absolute)
                            if current_edge < orig_edge * 0.35:
                                # Execution parity: sell at walked bid VWAP, not last-trade mid
                                _fb_px = current_yes if side == "YES" else 1.0 - current_yes
                                _walked_exit, _exit_note = await _clob_exit_price(market_slug or "", side, shares)
                                exit_px = _walked_exit if _walked_exit is not None else _fb_px
                                pnl = shares * exit_px - bet_amount
                                if _source_is_pg:
                                    try:
                                        await pg_store.resolve_bet(bet_id, round(exit_px, 4), round(pnl, 2), "sold")
                                    except Exception:
                                        pass
                                else:
                                    db3 = await get_db()
                                    try:
                                        await db3.execute(
                                            "UPDATE polymarket_sim_bets SET status='sold', exit_price=?, pnl=?, resolved_at=? WHERE id=?",
                                            (round(exit_px, 4), round(pnl, 2), now, bet_id))
                                        await db3.commit()
                                    finally:
                                        await db3.close()
                                    try:
                                        await pg_store.resolve_bet(bet_id, round(exit_px, 4), round(pnl, 2), "sold")
                                    except Exception:
                                        pass

                                sign = "+" if pnl > 0 else ""
                                await ws_manager.send_log(
                                    f"[SIM] THESIS EXIT: \"{market[:40]}\" edge gone ({orig_edge:.0f}%→{current_edge:.0f}%) → {sign}${pnl:.2f}",
                                    "warning",
                                )
                                await _brain(f"Edge evaporated on \"{market[:35]}\" ({orig_edge:.0f}%→{current_edge:.0f}%). Exiting to free capital.")
                                _notify(
                                    f"{'📈' if pnl > 0 else '📉'} Thesis Exit {sign}${pnl:.2f}",
                                    f"{market[:60]}\nEdge gone ({orig_edge:.0f}%→{current_edge:.0f}%) | Bankroll: ${bankroll:.0f}",
                                )
                                # v3.20.10 — paper-engine Telegram push removed
                                _high_watermarks.pop(bet_id, None)
                                resolved_count += 1
                                continue
                except Exception as e:
                    logger.warning(f"Thesis check error for bet {bet_id}: {e}")

            # Not resolved -- check for trailing stops and early exits
            if current_yes is not None:
                if side == "YES":
                    current_price = current_yes
                else:
                    current_price = 1.0 - current_yes

                if entry_price > 0.01:
                    rel_move = (current_price - entry_price) / entry_price
                else:
                    rel_move = 0

                # Use CALIBRATED exit thresholds if available, fallback to market-type
                from app.services.self_learner import get_calibrated_exit_thresholds
                _bet_source = None
                try:
                    if _source_is_pg and pg_bets:
                        for pgb in pg_bets:
                            if pgb.get("id") == bet_id:
                                _bet_source = pgb.get("signal_source")
                                break
                    if not _bet_source:
                        # Fallback: read from SQLite
                        db_src = await get_db()
                        try:
                            cur = await db_src.execute("SELECT signal_source FROM polymarket_sim_bets WHERE id=?", (bet_id,))
                            row = await cur.fetchone()
                            if row:
                                _bet_source = row[0]
                        finally:
                            await db_src.close()
                except Exception:
                    pass
                calibrated_exits = get_calibrated_exit_thresholds(_bet_source) if _bet_source else None
                if calibrated_exits:
                    win_thresh, loss_thresh = calibrated_exits
                else:
                    win_thresh, loss_thresh = _get_exit_thresholds(market)

                # Check trailing stop first (overrides fixed win threshold)
                trail_exit, trail_reason = _check_trailing_stop(bet_id, entry_price, current_price, win_thresh, market)

                should_exit_win = trail_exit or rel_move >= win_thresh
                should_exit_loss = rel_move <= -loss_thresh

                # Weather bets: ALWAYS hold to resolution — never exit early on price movement.
                # We trust the forecast model, not the market crowd. A -15% dip before
                # resolution is noise; the payout at resolution is 4-25x what we'd salvage
                # by stopping out. Thesis exit also doesn't fire (no "edge" key in weather detail).
                _is_weather = "temperature" in market.lower()
                if _is_weather:
                    if rel_move > 0:
                        await _brain(f"Holding weather bet \"{market[:35]}\" (+{rel_move*100:.0f}%) — waiting for resolution to collect full payout")
                    else:
                        await _brain(f"Holding weather bet \"{market[:35]}\" ({rel_move*100:.0f}%) — forecast unchanged, holding to resolution")
                    continue  # Never early-exit a weather bet — win or lose, hold to resolution

                # Time-decay harvesting: if profitable + near resolution → HOLD
                if should_exit_win and not trail_exit and market_data:
                    if _time_decay_hold(market_data, rel_move):
                        await _brain(f"Holding \"{market[:35]}\" (+{rel_move*100:.0f}%) — resolution near, harvesting time decay")
                        continue  # Skip exit, let profit grow

                exit_reason = trail_reason if trail_exit else (f"+{rel_move*100:.0f}%" if should_exit_win else f"{rel_move*100:.0f}%")

                if should_exit_win or should_exit_loss:
                    # Execution parity: sell at walked bid VWAP across the levels
                    # we'd actually hit, not the last-trade price.
                    _walked_exit, _ = await _clob_exit_price(market_slug or "", side, shares)
                    exit_px = _walked_exit if _walked_exit is not None else current_price
                    pnl = shares * exit_px - bet_amount
                    level = "success" if pnl > 0 else "error"
                    label = "TRAIL WIN" if trail_exit else ("EARLY WIN" if pnl > 0 else "EARLY LOSS")

                    # Write to correct database
                    if _source_is_pg:
                        try:
                            await pg_store.resolve_bet(bet_id, round(exit_px, 4), round(pnl, 2), "sold")
                        except Exception:
                            pass
                    else:
                        db = await get_db()
                        try:
                            await db.execute(
                                "UPDATE polymarket_sim_bets SET status='sold', exit_price=?, pnl=?, resolved_at=? WHERE id=?",
                                (round(exit_px, 4), round(pnl, 2), now, bet_id),
                            )
                            await db.commit()
                        finally:
                            await db.close()
                        try:
                            await pg_store.resolve_bet(bet_id, round(exit_px, 4), round(pnl, 2), "sold")
                        except Exception:
                            pass

                    # Clean up trailing stop tracking
                    _high_watermarks.pop(bet_id, None)

                    bankroll = await _get_bankroll()
                    sign = "+" if pnl > 0 else ""
                    await ws_manager.send_log(
                        f"[SIM] {label}: \"{market[:45]}\" {exit_reason} -> {sign}${pnl:.2f} (bankroll: ${bankroll:.0f})",
                        level,
                    )
                    emoji = "\U0001f4c8" if pnl > 0 else "\U0001f4c9"
                    await _brain(
                        f"{emoji} Exiting \"{market[:40]}\" \u2014 {exit_reason}. "
                        f"P&L: {sign}${pnl:.2f}. Bankroll: ${bankroll:.0f}"
                    )
                    _notify(
                        f"{emoji} {label} {sign}${pnl:.2f}",
                        f"{market[:60]}\n{exit_reason} | Bankroll: ${bankroll:.0f}",
                    )
                    # v3.20.10 — paper-engine Telegram push removed
                    resolved_count += 1

        # Small delay between API calls to be polite
        await asyncio.sleep(0.5)

    # Persist watermarks so trailing stops survive deploys (fire-and-forget)
    if _high_watermarks:
        try:
            asyncio.create_task(pg_store.save_watermarks(_high_watermarks))
        except Exception:
            pass

    if resolved_count > 0:
        logger.info(f"Simulator resolved {resolved_count} bets")
        # AI Post-Trade Review: analyze patterns every 10 resolved bets
        try:
            from app.services.ai_analyst import review_recent_performance
            history = await get_bet_history(20)
            formatted = [{"market": b.get("market", ""), "side": b.get("side", ""),
                          "entry_price": b.get("entry_price", 0), "pnl": b.get("pnl", 0),
                          "score": b.get("score", 0), "source": b.get("source", "")}
                         for b in history]
            await review_recent_performance(formatted)
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════
# SOURCE PERFORMANCE TRACKING
# ══════════════════════════════════════════════════════════════

async def get_source_win_rates() -> dict:
    """Get win rate by signal source. Tries Postgres first."""
    # Try Postgres first
    pg_perf = await pg_store.get_performance_by_source()
    if pg_perf:
        result = {}
        for source, data in pg_perf.items():
            wins = int(data.get("wins", 0))
            losses = int(data.get("losses", 0))
            total = wins + losses
            result[source or "unknown"] = {
                "wins": wins, "losses": losses,
                "win_rate": round(wins / total * 100, 1) if total > 0 else 0,
                "total": total,
            }
        return result

    # Fallback to SQLite
    db = await get_db()
    try:
        cursor = await db.execute("""
            SELECT signal_source,
                   SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
                   SUM(CASE WHEN pnl <= 0 THEN 1 ELSE 0 END) as losses
            FROM polymarket_sim_bets
            WHERE status IN ('won', 'lost', 'sold')
            GROUP BY signal_source
        """)
        rows = await cursor.fetchall()
        result = {}
        for r in rows:
            source = r[0] or "unknown"
            wins = r[1] or 0
            losses = r[2] or 0
            total = wins + losses
            result[source] = {
                "wins": wins,
                "losses": losses,
                "win_rate": round(wins / total * 100, 1) if total > 0 else 0,
                "total": total,
            }
        return result
    finally:
        await db.close()


def _source_size_multiplier(source: str, source_stats: dict) -> float:
    """Auto-signal pruning: scale or DISABLE sources based on real performance.

    Self-improving system — sources that lose money get killed automatically:
    - <8 bets:  1.0x (too early to judge)
    - 8+ bets, <20% WR: 0.0x (fast-kill — obvious disaster, stop digging)
    - 8-9 bets, otherwise: 1.0x (still learning)
    - 10+ bets, 60%+ WR: 1.5x (proven winner, bet bigger)
    - 10+ bets, 50-60% WR: 1.0x (neutral, keep watching)
    - 10+ bets, 40-50% WR: 0.5x (underperforming, reduce)
    - 10+ bets, <40% WR: 0.0x (KILL — consistently loses money)

    Weather has its own safety floor (see below) because its EV model is not
    score-based and needs different failure criteria.
    """
    stats = source_stats.get(source)

    if source == "weather":
        # Weather uses EV model, not score-based WR. Normally exempt — but add a
        # catastrophic-drift circuit breaker so a broken forecast API or stale
        # model can't bleed forever.
        if stats and stats["total"] >= 40 and stats["win_rate"] < 42:
            return 0.0  # Weather drift detected — pause until investigated
        return 1.0

    if not stats:
        return 1.0
    total = stats["total"]
    wr = stats["win_rate"]

    # Fast-kill: 8+ bets and catastrophically bad — stop digging immediately
    if total >= 8 and wr < 20:
        return 0.0

    if total < 10:
        return 1.0  # Still learning, not enough data for full pruning decision

    if wr >= 60:
        return 1.5  # Proven winner
    elif wr >= 50:
        return 1.0  # Break-even, keep watching
    elif wr >= 40:
        return 0.5  # Losing slightly, reduce exposure
    else:
        return 0.0  # KILL IT — consistently loses money (10+ bets, <40% WR)


# ══════════════════════════════════════════════════════════════
# SIZE CALCULATOR
# ══════════════════════════════════════════════════════════════

def _kelly_size(score: float, price: float, bankroll: float, source_mult: float = 1.0) -> float:
    """Kelly criterion sizing based on estimated edge from score.

    Maps composite score to estimated win probability:
    - Score 30 → ~55% win prob (barely above break-even)
    - Score 50 → ~62% win prob (solid edge)
    - Score 70+ → ~72% win prob (strong confluence)

    Kelly formula: f = (p * b - q) / b
    where p = win prob, q = 1-p, b = payout ratio (1/price - 1)
    """
    if score < MIN_SCORE_FOR_BET:
        return 0

    # Use CALIBRATED win probability if available (learned from real data)
    # Falls back to estimate if not enough resolved bets yet
    from app.services.self_learner import get_calibrated_win_prob
    calibrated = get_calibrated_win_prob(score)
    if calibrated is not None:
        est_win_prob = calibrated
    else:
        # Default estimate until calibration kicks in
        est_win_prob = min(0.80, 0.45 + score * 0.004)

    # Payout ratio for binary prediction market
    if price <= 0.01 or price >= 0.99:
        return 0
    payout_ratio = (1.0 / price) - 1.0  # At 40c: payout = 1.5x. At 25c: payout = 3x.

    # Kelly fraction
    q = 1.0 - est_win_prob
    kelly = (est_win_prob * payout_ratio - q) / payout_ratio if payout_ratio > 0 else 0

    # Quarter-Kelly (proven: weatherbot used 0.25x and turned $300→$101K)
    kelly = kelly * 0.25

    # If Kelly is negative or zero, don't bet (no edge at this price)
    if kelly <= 0:
        return 0

    # Clamp to min/max (only when Kelly is positive)
    kelly = max(MIN_KELLY_PCT, min(MAX_KELLY_PCT, kelly))

    # Apply source multiplier then re-cap — source_mult can be 1.5x which would
    # otherwise bypass MAX_KELLY_PCT and silently over-bet by 50%
    amount = bankroll * kelly * source_mult
    amount = min(amount, bankroll * MAX_KELLY_PCT)
    return round(max(0, amount), 2)


def _weather_volume_multiplier(volume: float) -> float:
    """Boost Kelly when weather market volume signals real crowd participation.
    At thin markets price is set by 1-2 traders, so forecast-vs-price disagreement
    is weak evidence. At deep markets the crowd has weighed in, so disagreement
    is stronger evidence — size up. Caps at 1.5x; non-moonshot path only
    (moonshots are already supersized via MOONSHOT_SIZE_PCT). dynamic_cap and
    MAX_KELLY_PCT downstream still hard-bound the final amount."""
    if volume >= 20000:
        return 1.5
    if volume >= 5000:
        return 1.2
    return 1.0


# ══════════════════════════════════════════════════════════════
# SIGNAL CONFLUENCE DETECTOR
# ══════════════════════════════════════════════════════════════

# Wallet-flow signal sources — copy/leaderboard/consensus all reflect "smart
# money is taking this side." When any one of these AND the orderbook signal
# fire on the same slug, that's the documented r/Kalshi $50→$2000 method:
# wallet flow + bid replenishment = conviction worth sizing into.
_WALLET_FLOW_SOURCES = frozenset({"copy", "consensus", "leaderboard"})


def _detect_confluence(all_signals_by_slug: dict) -> dict:
    """Detect when multiple signal sources agree on the same market.

    If mispricing says BUY, orderbook says BUY, and momentum says BUY,
    that's 3x confluence → much stronger than any single signal.

    Wallet-flow × orderbook super-confluence: when at least one wallet-flow
    signal (copy/consensus/leaderboard) AND the orderbook signal both agree,
    add an extra +15pt bonus on top of the count-based bonus. Cap raised to
    60 (from 45) to allow the super-bonus to actually take effect alongside
    4-source confluence (45 + 15 = 60). The downstream `boosted_score = min(95,
    avg_score + bonus)` cap still hard-bounds the resulting score.

    Returns: {slug: {"sources": [...], "count": N, "avg_score": float,
                     "side": str, "bonus": int, "wallet_orderbook_super": bool}}
    """
    confluence = {}
    for slug, signals in all_signals_by_slug.items():
        if len(signals) < 2:
            continue
        # Check if sources agree on direction
        sides = [s["side"] for s in signals]
        yes_count = sides.count("YES")
        no_count = sides.count("NO")
        if yes_count >= 2 or no_count >= 2:
            if yes_count == no_count:
                continue  # Tied — no real consensus, skip
            dominant_side = "YES" if yes_count > no_count else "NO"
            agreeing = [s for s in signals if s["side"] == dominant_side]
            avg_score = sum(s["score"] for s in agreeing) / len(agreeing)

            # Count-based confluence bonus: 2 sources = +15, 3 = +30, 4+ = +45
            count_bonus = (len(agreeing) - 1) * 15

            # Super-confluence: wallet-flow source + orderbook source agreeing
            agreeing_sources = {s["source"] for s in agreeing}
            wallet_orderbook_super = bool(
                agreeing_sources & _WALLET_FLOW_SOURCES
            ) and ("orderbook" in agreeing_sources)
            super_bonus = 15 if wallet_orderbook_super else 0

            confluence[slug] = {
                "sources": [s["source"] for s in agreeing],
                "count": len(agreeing),
                "avg_score": avg_score,
                "side": dominant_side,
                "best_signal": max(agreeing, key=lambda s: s["score"]),
                "bonus": min(60, count_bonus + super_bonus),
                "wallet_orderbook_super": wallet_orderbook_super,
            }
    return confluence


def _dynamic_bet_limit(avg_signal_score: float) -> int:
    """Allow more bets per hour when signal quality is high.

    avg_score < 40: 3/hour (base — cautious)
    avg_score 40-60: 4/hour (good signals)
    avg_score > 60: 5/hour (excellent signals)
    """
    if avg_signal_score >= 60:
        return MAX_BETS_PER_HOUR  # 5
    elif avg_signal_score >= 40:
        return 4
    return BASE_BETS_PER_HOUR  # 3


# ══════════════════════════════════════════════════════════════
# MARKET-TYPE-AWARE EXIT THRESHOLDS
# ══════════════════════════════════════════════════════════════

_POLITICAL_KEYWORDS = ["president", "election", "senate", "congress", "governor", "party",
                       "democrat", "republican", "nominee", "vote", "legislation", "bill"]
_SPORTS_KEYWORDS = ["vs.", "vs ", "game", "match", "win on 2026", "premier league",
                    "nba", "nfl", "mlb", "champions league", "world cup"]


def _get_exit_thresholds(market_title: str) -> tuple[float, float]:
    """Return (win_threshold, loss_threshold) based on market type.

    Political markets: slow-moving → wide stops (let them develop)
    Sports markets: fast-moving → tight stops (resolve quickly)
    Default: balanced
    """
    t = market_title.lower()
    if any(kw in t for kw in _POLITICAL_KEYWORDS):
        return (0.35, 0.15)  # Political: +35% win, -15% loss (slow, wide)
    elif any(kw in t for kw in _SPORTS_KEYWORDS):
        return (0.18, 0.10)  # Sports: +18% win, -10% loss (fast, tight)
    return (0.25, 0.12)      # Default: +25% win, -12% loss


def _time_decay_hold(market_data: dict, rel_move: float) -> bool:
    """Time-decay harvesting: if we're profitable AND resolution is near, HOLD.

    As markets approach resolution, prices converge to 0 or 1 faster.
    A +15% position with 2 days to resolution will likely grow to +30-50%
    if we're on the right side. Don't take the early exit — let time decay work.

    Returns True if we should HOLD (skip early exit) to harvest more time decay.
    """
    if rel_move <= 0:
        return False  # Only hold winners

    # Check days to resolution from market data
    end_date = market_data.get("endDate") or market_data.get("end_date_iso")
    if not end_date:
        return False

    try:
        from datetime import datetime, timezone
        if isinstance(end_date, str):
            # Try ISO format
            end_dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
        else:
            return False
        now = datetime.now(timezone.utc)
        days_left = (end_dt - now).total_seconds() / 86400
    except Exception:
        return False

    # If resolution is within 5 days and we're profitable → HOLD for more profit
    # The closer to resolution, the more we should hold
    if days_left <= 2 and rel_move >= 0.10:
        return True  # Resolution imminent + profitable → HOLD, price will converge more
    elif days_left <= 5 and rel_move >= 0.15:
        return True  # Resolution soon + solidly profitable → HOLD
    return False


# ══════════════════════════════════════════════════════════════
# TRAILING STOP LOGIC
# ══════════════════════════════════════════════════════════════

# Track highest price seen for each open bet (for trailing stop)
# Persisted to Postgres so trailing stops survive deploys/restarts
_high_watermarks: dict[int, float] = {}  # bet_id → highest price seen


async def _load_watermarks_from_pg():
    """Restore watermarks on startup so trailing stops survive restarts."""
    global _high_watermarks
    try:
        loaded = await pg_store.load_watermarks()
        if loaded:
            _high_watermarks.update(loaded)
            logger.info(f"Trailing stops restored: {len(loaded)} watermarks loaded from Postgres")
    except Exception as e:
        logger.warning(f"Could not load watermarks from Postgres: {e}")


def _check_trailing_stop(bet_id: int, entry_price: float, current_price: float,
                         win_threshold: float, market_title: str = "") -> tuple[bool, str]:
    """Trailing stop: once in profit, trail at highest_price - 40% of gain.

    Example: entry 40c, peaks at 60c (+50% gain). Trail = 60 - (20 * 0.4) = 52c.
    If price drops to 52c → exit with profit locked in.

    EXCEPTION: Weather bets (temperature markets) skip trailing stop entirely.
    They resolve to $1.00 within 24-48h — exiting at +$5 when payout is +$147
    leaves 95% of profit on the table.

    Returns (should_exit, reason).
    """
    # Weather bets: NEVER trail — hold to resolution for full $1.00 payout
    t = market_title.lower()
    if "temperature" in t or "highest temp" in t or "lowest temp" in t:
        return (False, "")

    # Update high watermark (async save handled by check_resolutions caller)
    prev_high = _high_watermarks.get(bet_id, entry_price)
    if current_price > prev_high:
        _high_watermarks[bet_id] = current_price
        prev_high = current_price

    # Only activate trailing stop after 15%+ gain
    gain_from_entry = (prev_high - entry_price) / entry_price if entry_price > 0 else 0
    if gain_from_entry < 0.15:
        return (False, "")

    # Trail: allow 40% pullback from high watermark
    trail_level = prev_high - (prev_high - entry_price) * 0.40
    if current_price <= trail_level:
        return (True, f"trail (high {prev_high*100:.0f}c → stopped {current_price*100:.0f}c)")

    return (False, "")


# ══════════════════════════════════════════════════════════════
# AUTO-BET ON SIGNALS
# ══════════════════════════════════════════════════════════════

async def auto_bet_on_signals():
    """Read current mispricing/copy/consensus signals and place bets.

    v3 rules:
    - Mispricing: score >= 30, min $50K volume
    - Copy: wallet win_rate >= 65%, trades >= 20
    - Consensus: 3+ wallets, score >= 20
    - Max 3 bets per hour, max 10 open bets
    - Auto-reset if bankroll < $50
    """
    from app.services.polymarket_wallets import (
        get_mispricing, get_copy_signals, get_consensus, get_rapid_moves,
        get_correlated_arbs, get_overround_arbs, get_momentum_signals, get_news_speed_signals,
        get_yesno_arbs, get_orderbook_signals, get_longshot_signals,
        get_leaderboard_signals, get_resolution_sniper, get_chain_signals, get_settled_bets,
        get_panic_fade_signals, get_final_period_signals,
        get_result_reversal_signals,
    )
    from app.services.weather_engine import get_weather_signals

    bankroll = await _get_bankroll()

    # Auto-reset if bankroll is critically low (the -91% situation)
    if bankroll < BANKROLL_RESET_THRESHOLD:
        await ws_manager.send_log(
            f"[SIM] Bankroll critically low: ${bankroll:.0f}. Auto-resetting to ${STARTING_BANKROLL:.0f}.",
            "warning",
        )
        await reset_simulator()
        bankroll = STARTING_BANKROLL

    if bankroll <= 10:
        await ws_manager.send_log("[SIM] Bankroll depleted ($10 or less), skipping bets", "warning")
        return

    # ── BANKROLL PROTECTION ──
    # Scale down bets when on a losing streak, scale up when profitable
    roi = (bankroll - STARTING_BANKROLL) / STARTING_BANKROLL
    if roi >= 0.10:
        # Up 10%+ → protect profits: reduce max bet size by 30%
        protection_mult = 0.7
        await _brain(f"Bankroll protection: +{roi*100:.0f}% profit → reducing bet sizes to protect gains")
    elif roi <= -0.10:
        # Down 10%+ → defensive mode: reduce by 50%
        protection_mult = 0.5
        await _brain(f"Bankroll protection: {roi*100:.0f}% drawdown → defensive mode, halving bet sizes")
    else:
        protection_mult = 1.0

    # Check streak: 3+ consecutive losses → reduce, 3+ wins → increase.
    # Only announce on state transition — previously spammed every scan tick.
    global _last_streak_state
    try:
        recent_bets = await get_bet_history(5)
        if len(recent_bets) >= 3:
            last_3_pnl = [b.get("pnl", 0) for b in recent_bets[:3]]
            if all(p < 0 for p in last_3_pnl):
                protection_mult *= 0.4
                if _last_streak_state != "loss":
                    await ws_manager.send_log(
                        f"[SIM] Losing streak (3 losses). Reducing bets to 40% size.",
                        "warning",
                    )
                    await _brain("3 consecutive losses — reducing bet sizes to 40% and being more selective.")
                    _last_streak_state = "loss"
            elif all(p > 0 for p in last_3_pnl):
                protection_mult *= 1.3
                if _last_streak_state != "win":
                    await ws_manager.send_log(
                        f"[SIM] Win streak (3 wins)! Increasing bet sizes by 30%.",
                        "success",
                    )
                    await _brain("3 consecutive wins — increasing bet sizes by 30% to capitalize on momentum.")
                    _last_streak_state = "win"
            else:
                _last_streak_state = "none"
    except Exception:
        pass

    # Check global limits
    open_count = await _get_open_bet_count()
    if open_count >= MAX_OPEN_BETS + MOONSHOT_EXTRA_SLOTS:
        await ws_manager.send_log(
            f"[SIM] At absolute max bets ({open_count}/{MAX_OPEN_BETS + MOONSHOT_EXTRA_SLOTS}), skipping new bets", "info"
        )
        return
    elif open_count >= MAX_OPEN_BETS:
        await ws_manager.send_log(
            f"[SIM] At max open bets ({open_count}/{MAX_OPEN_BETS}), moonshot weather signals only", "info"
        )
        # Fall through — weather moonshots (EV≥800%) can still fill slots 26-30

    # Apply bankroll protection multiplier to *available* bankroll (realized
    # equity minus capital tied up in open bets). Sizing off raw bankroll
    # would oversize Kelly when many bets are already open.
    available = await _get_available_bankroll()
    if available < bankroll:
        await _brain(
            f"Sizing off available bankroll: ${available:.0f} "
            f"(${bankroll - available:.0f} tied up in open bets)"
        )
    eff_bankroll = available * protection_mult

    # Load source performance for auto-weighting
    source_stats = await get_source_win_rates()

    # Load per-signal enabled flags (cached, DB-backed)
    from app.services.signal_config import is_signal_enabled as _sig_on

    # ── WEATHER-ONLY MODE ────────────────────────────────────────────────────
    # Investigation (2026-04-11): weather is the only profitable source.
    # All other sources had net-negative PnL. Set WEATHER_ONLY_MODE=False to
    # re-enable them once more resolved data justifies it.
    if WEATHER_ONLY_MODE:
        from app.services.weather_engine import get_weather_signals
        open_slugs = await _get_open_market_slugs()
        bets_placed = 0
        now_ts = time.time()
        weather = get_weather_signals()
        for w in weather:
            if not await _can_bet_now(MAX_BETS_PER_HOUR) or open_count + bets_placed >= MAX_OPEN_BETS + MOONSHOT_EXTRA_SLOTS:
                break
            # When at normal cap, only moonshots (EV≥threshold) get through
            if open_count + bets_placed >= MAX_OPEN_BETS:
                if not (w.get("ev", 0) >= MOONSHOT_EV_THRESHOLD and w.get("probability", 0) >= 95):
                    continue
            slug = w.get("slug", "")
            if not slug or slug in open_slugs:
                continue
            if now_ts - w.get("timestamp", 0) > 3600:
                continue
            price = w.get("price", 0.5)
            _ms_pre = w.get("ev", 0) >= MOONSHOT_EV_THRESHOLD and w.get("probability", 0) >= 95
            if _ms_pre:
                if price > MAX_ENTRY_PRICE:
                    continue
            elif price < MIN_ENTRY_PRICE or price > MAX_ENTRY_PRICE:
                continue

            # Recheck hours_left from end_date — cached value is stale.
            # A signal at 2.05h when scanned can be at 1.75h by the time we
            # try to bet. Betting this close to resolution is pointless.
            from app.services.weather_engine import MIN_HOURS as WE_MIN_HOURS, _hours_to_resolution
            real_hours = _hours_to_resolution(w.get("end_date", ""))
            if real_hours < WE_MIN_HOURS:
                await _brain(f"Skip weather bet {w.get('city')} — only {real_hours:.1f}h left (min {WE_MIN_HOURS}h)")
                continue

            weather_open = sum(1 for s in open_slugs if "temperature" in s.lower() or "temp" in s.lower())
            if weather_open >= 30:
                continue

            w_score = w.get("score", 40)
            w_ev = w.get("ev", 0)
            w_prob = w.get("probability", 0)
            w_mult = _source_size_multiplier("weather", source_stats)

            is_moonshot = w_ev >= MOONSHOT_EV_THRESHOLD and w_prob >= 95
            if is_moonshot:
                amount = min(eff_bankroll * MOONSHOT_SIZE_PCT, eff_bankroll * 0.30)
                # Alert immediately on detection — fires even if at capacity
                if slug not in _moonshot_notified:
                    _moonshot_notified.add(slug)
                    _notify(
                        f"🚀 MOONSHOT DETECTED — {w.get('side','YES')} @ {price*100:.0f}¢",
                        f"{w.get('city')} {w.get('bucket')} | EV +{w_ev:.0f}% | prob {w_prob:.0f}%\n"
                        f"Auto-sizing ${amount:.0f} ({MOONSHOT_SIZE_PCT*100:.0f}% bankroll) — CHECK DASHBOARD",
                        priority="urgent",
                    )
            else:
                vol_mult = _weather_volume_multiplier(w.get("volume", 0))
                amount = _kelly_size(w_score, price, eff_bankroll, w_mult * vol_mult)
                dynamic_cap = max(MAX_WEATHER_BET, eff_bankroll * 0.04)
                amount = min(amount, dynamic_cap)

            if amount < 5:
                continue
            placed = await place_bet(
                market=w.get("market", "Unknown"),
                slug=slug, side=w.get("side", "YES"), price=price, amount=amount,
                source="weather", score=w_score,
                detail={"city": w.get("city"), "city_slug": w.get("city_slug"),
                        "forecast": w.get("forecast_temp"),
                        "ev": w_ev, "bucket": w.get("bucket"),
                        "source": w.get("forecast_source"),
                        "date": w.get("date", ""),
                        "end_date": w.get("end_date", ""),
                        "volume": w.get("volume", 0),
                        "moonshot": is_moonshot},
            )
            if placed:
                bets_placed += 1
                open_slugs.add(slug)
                if is_moonshot:
                    await _brain(
                        f"🚀 MOONSHOT: {w.get('city')} {w.get('bucket')} — "
                        f"EV +{w_ev:.0f}%, prob {w_prob:.0f}% | "
                        f"${amount:.0f} @ {price*100:.0f}c ({w.get('forecast_source')})"
                    )
                else:
                    await _brain(
                        f"WEATHER BET: {w.get('city')} {w.get('bucket')} — "
                        f"forecast {w.get('forecast_temp')} ({w.get('forecast_source')}), "
                        f"EV +{w_ev}%, ${amount:.0f} @ {price*100:.0f}c"
                    )
        await _save_daily_snapshot()
        portfolio = await get_portfolio()
        roi = portfolio["roi_pct"]
        sign = "+" if roi >= 0 else ""
        hr_count = await _bets_this_hour()
        await ws_manager.send_log(
            f"[SIM] Portfolio: ${portfolio['bankroll']:.0f} ({sign}{roi:.1f}%) | "
            f"{portfolio['open_bets']} open ({bets_placed} new) | "
            f"{portfolio['resolved']} resolved | "
            f"{portfolio['win_rate']:.0f}% win rate | "
            f"{hr_count}/{MAX_BETS_PER_HOUR} bets/hr",
            "info",
        )
        await _brain(f"Portfolio: ${portfolio['bankroll']:.0f} | {portfolio['open_bets']} open bets | Weather-only mode | Scanning {len(weather)} weather signals...")
        return
    # ────────────────────────────────────────────────────────────────────────

    # ─── CONFLUENCE DETECTION: collect all signals by slug ───
    all_signals_by_slug: dict[str, list] = {}

    def _collect_signal(slug: str, side: str, score: float, source: str):
        """Add a signal to the confluence tracker."""
        if slug:
            all_signals_by_slug.setdefault(slug, []).append(
                {"slug": slug, "side": side, "score": score, "source": source}
            )

    # Collect signals from all sources for confluence detection
    now_ts = time.time()
    for m in get_mispricing():
        if now_ts - m.get("timestamp", 0) > 900:
            continue
        s = "YES" if m.get("edge", 0) > 0 else "NO"
        _collect_signal(m.get("slug", ""), s, m.get("score", 0), "mispricing")

    for ob in get_orderbook_signals():
        if now_ts - ob.get("timestamp", 0) > 600:
            continue
        _collect_signal(ob.get("slug", ""), ob.get("side", "YES"), ob.get("score", 0), "orderbook")

    for mom in get_momentum_signals():
        if now_ts - mom.get("timestamp", 0) > 900:
            continue
        _collect_signal(mom.get("slug", ""), mom.get("side", "YES"), mom.get("score", 0), "momentum")

    for pf in get_panic_fade_signals():
        if now_ts - pf.get("timestamp", 0) > 900:
            continue
        _collect_signal(pf.get("slug", ""), "YES", pf.get("score", 0), "panic_fade")

    for fp in get_final_period_signals():
        if now_ts - fp.get("timestamp", 0) > 600:
            continue
        _collect_signal(fp.get("slug", ""), "YES", fp.get("score", 0), "final_period")

    for rv in get_result_reversal_signals():
        if now_ts - rv.get("timestamp", 0) > 600:
            continue
        _collect_signal(rv.get("slug", ""), rv.get("side", "YES"), rv.get("score", 0), "result_reversal")

    for ns in get_news_speed_signals():
        if now_ts - ns.get("timestamp", 0) > 300:
            continue
        _collect_signal(ns.get("slug", ""), ns.get("side", "YES"), ns.get("score", 0), "news_speed")

    for ls in get_longshot_signals():
        _collect_signal(ls.get("slug", ""), "NO", ls.get("score", 0), "longshot")

    confluence = _detect_confluence(all_signals_by_slug)

    # Log confluence detections
    for slug, conf in confluence.items():
        if conf["count"] >= 3 or conf.get("wallet_orderbook_super"):
            tag = "WALLET×ORDERBOOK" if conf.get("wallet_orderbook_super") else "CONFLUENCE"
            await ws_manager.send_log(
                f"[{tag}] {conf['count']}x agreement on \"{conf['best_signal'].get('slug', '')[:30]}\": "
                f"{', '.join(conf['sources'])} → {conf['side']} (bonus: +{conf['bonus']}pts)",
                "success",
            )

    # Dynamic bet limit based on average signal quality
    all_scores = [s["score"] for sigs in all_signals_by_slug.values() for s in sigs]
    avg_score = sum(all_scores) / len(all_scores) if all_scores else 0
    current_bet_limit = _dynamic_bet_limit(avg_score)

    # Check if we can bet
    hr_count = await _bets_this_hour()
    if hr_count >= current_bet_limit:
        await ws_manager.send_log(
            f"[SIM] Hourly limit reached ({hr_count}/{current_bet_limit}/hr)", "info"
        )
        return

    open_count = await _get_open_bet_count()
    if open_count >= MAX_OPEN_BETS:
        await ws_manager.send_log(
            f"[SIM] At max open bets ({open_count}/{MAX_OPEN_BETS}), skipping new bets", "info"
        )
        return

    open_slugs = await _get_open_market_slugs()
    bets_placed = 0

    await _brain(f"Portfolio: ${bankroll:.0f} | {open_count} open bets | Limit: {current_bet_limit}/hr | Scanning {len(all_scores)} signals across 10 sources...")

    # Build rapid move lookup for momentum check
    rapid_moves = get_rapid_moves()
    rapid_by_slug = {}
    for rm in rapid_moves:
        rapid_by_slug[rm.get("slug", "")] = rm

    # ─── 0. CONFLUENCE BETS (highest priority — multiple sources agree) ───
    for slug, conf in sorted(confluence.items(), key=lambda x: x[1]["count"], reverse=True):
        if bets_placed >= 2:  # Max 2 confluence bets per cycle
            break
        if slug in open_slugs:
            continue

        best = conf["best_signal"]
        boosted_score = min(95, conf["avg_score"] + conf["bonus"])
        side = conf["side"]
        market_title = best.get("slug", "Unknown")

        if _is_noise_market(market_title):
            continue

        # Fetch live price for the market
        live = await _fetch_market_price(slug)
        if not live:
            continue
        try:
            lp = json.loads(live.get("outcomePrices", "[]"))
            live_yes = float(lp[0]) if lp else 0.5
        except (json.JSONDecodeError, ValueError, IndexError):
            continue

        price = live_yes if side == "YES" else 1.0 - live_yes
        if price < MIN_ENTRY_PRICE or price > MAX_ENTRY_PRICE:
            continue

        # Kelly sizing with confluence boost
        conf_mult = _source_size_multiplier("confluence", source_stats)
        amount = _kelly_size(boosted_score, price, eff_bankroll, conf_mult)
        if amount < 5:
            continue

        market_name = live.get("question", market_title)[:80]
        placed = await place_bet(
            market=market_name, slug=slug, side=side, price=price,
            amount=amount, source="confluence",
            score=boosted_score,
            detail={"sources": conf["sources"], "count": conf["count"],
                    "bonus": conf["bonus"], "avg_score": round(conf["avg_score"], 1),
                    "wallet_orderbook_super": conf.get("wallet_orderbook_super", False)},
        )
        if placed:
            bets_placed += 1
            open_slugs.add(slug)
            await ws_manager.send_log(
                f"[SIM] CONFLUENCE BET: {conf['count']}x {', '.join(conf['sources'])} → "
                f"{side} \"{market_name[:40]}\" ${amount:.0f} (score: {boosted_score:.0f})",
                "success",
            )

    # ─── 1. MISPRICING SIGNALS (score-gated) ───
    now_ts = time.time()
    # Total-exposure guard: refuse new bets when > MAX_EXPOSURE_FRACTION of bankroll
    # is already committed. Cluster resolutions could otherwise blow a big chunk
    # of bankroll simultaneously.
    exposure_ratio = (bankroll - available) / bankroll if bankroll > 0 else 0
    exposure_cap_hit = exposure_ratio >= MAX_EXPOSURE_FRACTION
    if exposure_cap_hit:
        await ws_manager.send_log(
            f"[SIM] Exposure cap: {exposure_ratio*100:.0f}% of bankroll in open bets "
            f"(max {MAX_EXPOSURE_FRACTION*100:.0f}%) — pausing new non-moonshot bets",
            "warning",
        )

    mispriced = get_mispricing() if await _sig_on("mispricing") else []
    for m in mispriced:
        if not await _can_bet_now(current_bet_limit) or open_count + bets_placed >= MAX_OPEN_BETS or exposure_cap_hit:
            break

        slug = m.get("slug", "")
        market_title = m.get("market", "")
        if not slug:
            continue
        if slug in open_slugs:
            await ws_manager.send_log(
                f"[SIM] Skipped mispricing: \"{market_title[:40]}\" — already open",
                "info",
            )
            continue

        # Signal freshness: skip if older than 15 minutes (edges decay fast)
        signal_age = now_ts - m.get("timestamp", 0)
        if signal_age > 900:
            await ws_manager.send_log(
                f"[SIM] Skipped mispricing: \"{market_title[:40]}\" — signal stale ({signal_age/60:.0f}min old)",
                "info",
            )
            continue

        score = m.get("score", 0)
        if score < MIN_SCORE_FOR_BET:
            await ws_manager.send_log(
                f"[SIM] Skipped mispricing: \"{market_title[:40]}\" score {score:.0f} (need {MIN_SCORE_FOR_BET}+)",
                "info",
            )
            continue

        # Manifold adverse-selection guard — huge edges from Manifold alone
        # are usually noise (play-money crowd misreading the question or
        # emotional bets), not real alpha. Skip when edge > 25% and Manifold
        # is the only divergent source. Kalshi (real-money retail) confirming
        # the same direction keeps the signal live.
        edge_abs = abs(m.get("edge", 0))
        sources_list = [str(s).lower() for s in m.get("sources", [])]
        manifold_only = sources_list == ["manifold"] or (
            "manifold" in sources_list and "kalshi" not in sources_list and len(sources_list) == 1
        )
        if edge_abs > 25 and manifold_only:
            await ws_manager.send_log(
                f"[SIM] Skipped mispricing: \"{market_title[:40]}\" — {edge_abs:.0f}% edge from Manifold only (likely play-money noise)",
                "warning",
            )
            continue

        # Noise market filter — skip sports/weather coin flips
        if _is_noise_market(market_title):
            await ws_manager.send_log(
                f"[SIM] Skipped mispricing: \"{market_title[:40]}\" — noise market",
                "info",
            )
            continue

        # Deadline trap detection — imminent deadlines where event hasn't happened
        days_left = m.get("days_to_resolution")
        if _is_deadline_trap(market_title, days_left):
            await _brain(f"Skipping deadline trap: \"{market_title[:40]}\" ({days_left:.0f}d left)")
            continue

        # Volume check
        volume = m.get("poly_volume_24h", 0)
        if volume < MIN_MARKET_VOLUME:
            await ws_manager.send_log(
                f"[SIM] Skipped mispricing: \"{market_title[:40]}\" — low volume ${volume:.0f} (need ${MIN_MARKET_VOLUME:.0f}+)",
                "info",
            )
            continue

        # Theme correlation check — max 3 open bets in same theme
        theme = _detect_theme(market_title)
        if theme != "other" and await _theme_exposure(theme) >= MAX_BETS_PER_THEME:
            await ws_manager.send_log(
                f"[SIM] Skipped: too many {theme} bets open ({MAX_BETS_PER_THEME} max)", "info"
            )
            continue

        # Momentum check — don't buy into a market that's crashing against us
        rm = rapid_by_slug.get(slug)
        if rm:
            edge_dir = "UP" if m.get("edge", 0) > 0 else "DOWN"
            if rm["direction"] != edge_dir:
                await ws_manager.send_log(
                    f"[SIM] Skipped: price moving {rm['direction']} but edge says {edge_dir} "
                    f"(\"{market_title[:35]}\")", "info"
                )
                continue

        # Determine side and entry price
        # Use the actual Polymarket price — that's what you'd pay on the orderbook.
        # The edge comes from believing fair value is different, not from getting a better fill.
        poly_prob = m.get("poly_prob", 50) / 100.0
        if m.get("edge", 0) > 0:
            side = "YES"
            price = poly_prob  # Buy YES at current market price
        else:
            side = "NO"
            price = 1.0 - poly_prob  # Buy NO at current market price

        if price < MIN_ENTRY_PRICE or price > MAX_ENTRY_PRICE:
            continue

        # Live price verification: re-check current price, update entry price, skip if edge gone
        fair_value = m.get("fair_value", m.get("poly_prob", 50)) / 100.0
        live_data = await _fetch_market_price(slug)
        if live_data:
            try:
                live_prices = json.loads(live_data.get("outcomePrices", "[]"))
                live_yes = float(live_prices[0]) if live_prices else None
                if live_yes is not None:
                    live_edge = abs(fair_value - live_yes) * 100
                    if live_edge < 5.0:
                        await ws_manager.send_log(
                            f"[SIM] Skipped: edge shrunk to {live_edge:.1f}% on re-check (\"{market_title[:35]}\")",
                            "info",
                        )
                        continue
                    # Update entry price to live market price
                    if side == "YES":
                        price = live_yes
                    else:
                        price = 1.0 - live_yes
            except (json.JSONDecodeError, ValueError, IndexError):
                pass

        # Score-based sizing with source performance weighting
        misp_mult = _source_size_multiplier("mispricing", source_stats)
        amount = _kelly_size(score, price, eff_bankroll, misp_mult)
        if amount < 5:
            continue

        sources = m.get("sources", [])
        detail = {
            "edge": m.get("edge"),
            "score": score,
            "sources": sources,
            "fair_value": m.get("fair_value"),
            "has_news": m.get("has_news", False),
            "days": m.get("days_to_resolution"),
        }
        placed = await place_bet(
            market=m.get("market", "Unknown"),
            slug=slug,
            side=side,
            price=price,
            amount=amount,
            source="mispricing",
            score=score,
            detail=detail,
        )
        if placed:
            bets_placed += 1
            open_slugs.add(slug)

    # ─── 2. COPY SIGNALS (stricter wallet requirements) ───
    signals = get_copy_signals() if await _sig_on("copy") else []
    for s in signals:
        if not await _can_bet_now(current_bet_limit) or open_count + bets_placed >= MAX_OPEN_BETS or exposure_cap_hit:
            break

        slug = s.get("slug", "")
        if not slug or slug in open_slugs:
            continue

        # Signal freshness: skip if older than 15 minutes
        if now_ts - s.get("timestamp", 0) > 900:
            continue

        # Noise market filter
        if _is_noise_market(s.get("market", "")):
            continue

        # Strict wallet requirements (was 60% / 10 trades -- that lost 91%)
        wallet_wr = s.get("wallet_score", 0)
        wallet_trades = s.get("wallet_trades", 0)
        if wallet_wr < MIN_COPY_WIN_RATE:
            await ws_manager.send_log(
                f"[SIM] Skipped copy signal: wallet {s.get('wallet', '???')} "
                f"win rate {wallet_wr:.0f}% (need {MIN_COPY_WIN_RATE:.0f}%+)",
                "info",
            )
            continue
        if wallet_trades < MIN_COPY_TRADES:
            continue

        # Theme correlation check
        copy_title = s.get("market", "")
        copy_theme = _detect_theme(copy_title)
        if copy_theme != "other" and await _theme_exposure(copy_theme) >= MAX_BETS_PER_THEME:
            continue

        action = s.get("action", "")
        price_pct = s.get("price", 50)
        price = price_pct / 100.0

        if "BUY" in action:
            side = "YES"
        elif "SELL" in action:
            side = "NO"
            price = 1.0 - price
        else:
            continue

        if price < MIN_ENTRY_PRICE or price > MAX_ENTRY_PRICE:
            continue

        # Estimate score for copy signals
        # Scale copy score by wallet quality — allow high conviction for elite wallets
        # 65% WR = 30 pts, 75% WR = 50 pts (high conviction), 85% WR = 70 pts
        copy_score = min(70, (wallet_wr - 50) * 2 + wallet_trades * 0.2)
        copy_mult = _source_size_multiplier("copy", source_stats)
        amount = _kelly_size(copy_score, price, eff_bankroll, copy_mult)
        if amount < 5:
            continue
        detail = {
            "wallet": s.get("wallet"),
            "wallet_wr": wallet_wr,
            "wallet_rank": s.get("wallet_rank"),
            "wallet_trades": wallet_trades,
        }
        placed = await place_bet(
            market=s.get("market", "Unknown"),
            slug=slug,
            side=side,
            price=price,
            amount=amount,
            source="copy",
            score=copy_score,
            detail=detail,
        )
        if placed:
            bets_placed += 1
            open_slugs.add(slug)

    # ─── 3. CONSENSUS SIGNALS (stricter requirements) ───
    consensus = get_consensus() if await _sig_on("consensus") else []
    for c in consensus:
        if not await _can_bet_now(current_bet_limit) or open_count + bets_placed >= MAX_OPEN_BETS or exposure_cap_hit:
            break

        slug = c.get("slug", "")
        # Noise filter
        if _is_noise_market(c.get("title", "")):
            continue
        if not slug or slug in open_slugs:
            continue

        # Require 3+ wallets (was 2) and at least 1 scored
        whale_count = c.get("whale_count", 0)
        if whale_count < MIN_CONSENSUS_WALLETS:
            continue
        if c.get("scored_whale_count", 0) < 1:
            continue

        # Theme correlation check
        cons_title = c.get("title", "")
        cons_theme = _detect_theme(cons_title)
        if cons_theme != "other" and await _theme_exposure(cons_theme) >= MAX_BETS_PER_THEME:
            continue

        signal = c.get("signal", "")
        if signal == "BUY":
            side = "YES"
        elif signal == "SELL":
            side = "NO"
        else:
            continue

        # Estimate consensus score
        # Weight scored wallets heavily — unscored wallets are unknown quality
        scored_count = c.get("scored_whale_count", 0)
        consensus_score = scored_count * 15 + whale_count * 2
        if consensus_score < MIN_CONSENSUS_SCORE:
            continue

        # Fetch LIVE market price for consensus bets (whale avg_price is historical, not current)
        cons_slug = c.get("slug", "")
        cons_live = await _fetch_market_price(cons_slug) if cons_slug else None
        if cons_live:
            try:
                cons_prices = json.loads(cons_live.get("outcomePrices", "[]"))
                cons_yes = float(cons_prices[0]) if cons_prices else 0.50
            except (json.JSONDecodeError, ValueError, IndexError):
                cons_yes = 0.50
        else:
            cons_yes = c.get("avg_price", 0.50)  # Fallback to whale price

        if signal == "BUY":
            price = cons_yes
        else:
            price = 1.0 - cons_yes

        if price < MIN_ENTRY_PRICE or price > MAX_ENTRY_PRICE:
            continue

        cons_mult = _source_size_multiplier("consensus", source_stats)
        amount = _kelly_size(consensus_score, price, eff_bankroll, cons_mult)
        if amount < 5:
            continue

        detail = {
            "whale_count": whale_count,
            "scored": c.get("scored_whale_count"),
            "volume": c.get("total_volume"),
        }
        placed = await place_bet(
            market=c.get("title", "Unknown"),
            slug=slug,
            side=side,
            price=price,
            amount=amount,
            source="consensus",
            score=consensus_score,
            detail=detail,
        )
        if placed:
            bets_placed += 1
            open_slugs.add(slug)

    # ─── 4. CORRELATED ARBITRAGE (buy cheap side of contradictions) ───
    corr_arbs = get_correlated_arbs()
    for arb in corr_arbs:
        if not await _can_bet_now(current_bet_limit) or open_count + bets_placed >= MAX_OPEN_BETS or exposure_cap_hit:
            break

        # Bet on the CHEAP side (lowest price in the group)
        markets = arb.get("markets", [])
        if len(markets) < 2:
            continue
        cheap = markets[-1]  # Sorted by price desc, last = cheapest
        slug = cheap.get("slug", "")
        if not slug or slug in open_slugs:
            continue
        if _is_noise_market(cheap.get("question", "")):
            continue

        price = cheap.get("price", 50) / 100.0
        if price < MIN_ENTRY_PRICE or price > MAX_ENTRY_PRICE:
            continue

        arb_score = min(70, 30 + arb.get("spread", 0))
        arb_mult = _source_size_multiplier("correlated_arb", source_stats)
        amount = _kelly_size(arb_score, price, eff_bankroll, arb_mult)
        if amount < 5:
            continue

        placed = await place_bet(
            market=cheap.get("question", "Unknown"),
            slug=slug, side="YES", price=price, amount=amount,
            source="correlated_arb", score=arb_score,
            detail={"group": arb.get("group"), "spread": arb.get("spread")},
        )
        if placed:
            bets_placed += 1
            open_slugs.add(slug)

    # ─── 5. OVERROUND ARBITRAGE (sell overpriced outcomes) ───
    overrounds = get_overround_arbs() if await _sig_on("overround") else []
    for ov in overrounds:
        if not await _can_bet_now(current_bet_limit) or open_count + bets_placed >= MAX_OPEN_BETS or exposure_cap_hit:
            break

        best = ov.get("best_no_bet", {})
        slug = best.get("slug", "")
        if not slug or slug in open_slugs:
            continue
        if _is_noise_market(best.get("market", "")):
            continue

        no_price = best.get("no_price", 50) / 100.0
        if no_price < MIN_ENTRY_PRICE or no_price > MAX_ENTRY_PRICE:
            continue

        ov_score = ov.get("score", 30)
        ov_mult = _source_size_multiplier("overround", source_stats)
        amount = _kelly_size(ov_score, no_price, eff_bankroll, ov_mult)
        if amount < 5:
            continue

        placed = await place_bet(
            market=best.get("market", "Unknown"),
            slug=slug, side="NO", price=no_price, amount=amount,
            source="overround", score=ov_score,
            detail={"overround_pct": ov.get("overround_pct"), "group": ov.get("group")},
        )
        if placed:
            bets_placed += 1
            open_slugs.add(slug)

    # ─── 6. MOMENTUM / MEAN REVERSION ───
    momentum = get_momentum_signals() if await _sig_on("momentum") else []
    for m in momentum:
        if not await _can_bet_now(current_bet_limit) or open_count + bets_placed >= MAX_OPEN_BETS or exposure_cap_hit:
            break

        slug = m.get("slug", "")
        if not slug or slug in open_slugs:
            continue
        if now_ts - m.get("timestamp", 0) > 900:
            continue

        price = m.get("current_price", 50) / 100.0
        side = m.get("side", "YES")
        if side == "NO":
            price = 1.0 - price
        if price < MIN_ENTRY_PRICE or price > MAX_ENTRY_PRICE:
            continue

        # Defensive cap: live data shows momentum at score 60+ goes 0-W/8-L
        # (-$118 across 60-70 + 70-80 buckets per 2026-04-17 calibration).
        # Signal is currently disabled by v3.8 migration, but if anyone toggles
        # it back on via ATSettings, never let it size as high-conviction.
        mom_score = min(50, m.get("score", 30))
        mom_mult = _source_size_multiplier("momentum", source_stats)
        amount = _kelly_size(mom_score, price, eff_bankroll, mom_mult)
        if amount < 5:
            continue

        placed = await place_bet(
            market=m.get("market", "Unknown"),
            slug=slug, side=side, price=price, amount=amount,
            source="momentum", score=mom_score,
            detail={"edge": m.get("edge"), "type": m.get("type")},
        )
        if placed:
            bets_placed += 1
            open_slugs.add(slug)

    # ─── 7. BREAKING NEWS SPEED EDGE ───
    speed_signals = get_news_speed_signals() if await _sig_on("news_speed") else []
    for ns in speed_signals:
        if not await _can_bet_now(current_bet_limit) or open_count + bets_placed >= MAX_OPEN_BETS or exposure_cap_hit:
            break

        slug = ns.get("slug", "")
        if not slug or slug in open_slugs:
            continue
        if now_ts - ns.get("timestamp", 0) > 300:  # Speed edges decay in 5 min
            continue
        if _is_noise_market(ns.get("market", "")):
            continue

        # Use Polymarket price (the fast-moving one)
        poly_price = ns.get("poly_price", 50) / 100.0
        side = ns.get("side", "YES")
        if side == "NO":
            price = 1.0 - poly_price
        else:
            price = poly_price
        if price < MIN_ENTRY_PRICE or price > MAX_ENTRY_PRICE:
            continue

        ns_score = ns.get("score", 40)
        ns_mult = _source_size_multiplier("news_speed", source_stats)
        amount = _kelly_size(ns_score, price, eff_bankroll, ns_mult)
        if amount < 5:
            continue

        placed = await place_bet(
            market=ns.get("market", "Unknown"),
            slug=slug, side=side, price=price, amount=amount,
            source="news_speed", score=ns_score,
            detail={"speed_edge": ns.get("speed_edge"), "rapid_move": ns.get("rapid_move")},
        )
        if placed:
            bets_placed += 1
            open_slugs.add(slug)

    # ─── 8. YES+NO ARBITRAGE (guaranteed profit) ───
    yesno_arbs = get_yesno_arbs() if await _sig_on("yesno_arb") else []
    for arb in yesno_arbs:
        if not await _can_bet_now(current_bet_limit) or open_count + bets_placed >= MAX_OPEN_BETS or exposure_cap_hit:
            break

        slug = arb.get("slug", "")
        if not slug or slug in open_slugs:
            continue
        if now_ts - arb.get("timestamp", 0) > 600:
            continue

        # For YES+NO arb: buy YES at the ask price
        # (In reality you'd buy both YES and NO, but simulator tracks single-side bets)
        # We buy YES since the combined < $1 means YES is underpriced
        yes_ask = arb.get("yes_ask", 50) / 100.0
        if yes_ask < MIN_ENTRY_PRICE or yes_ask > MAX_ENTRY_PRICE:
            continue

        arb_score = arb.get("score", 50)
        arb_mult = _source_size_multiplier("yesno_arb", source_stats)
        amount = _kelly_size(arb_score, yes_ask, eff_bankroll, arb_mult)
        if amount < 5:
            continue

        placed = await place_bet(
            market=arb.get("market", "Unknown"),
            slug=slug, side="YES", price=yes_ask, amount=amount,
            source="yesno_arb", score=arb_score,
            detail={"profit_pct": arb.get("profit_pct"), "combined": arb.get("combined")},
        )
        if placed:
            bets_placed += 1
            open_slugs.add(slug)

    # ─── 9. ORDERBOOK DEPTH SIGNALS ───
    ob_signals = get_orderbook_signals() if await _sig_on("orderbook") else []
    for ob in ob_signals:
        if not await _can_bet_now(current_bet_limit) or open_count + bets_placed >= MAX_OPEN_BETS or exposure_cap_hit:
            break

        slug = ob.get("slug", "")
        if not slug or slug in open_slugs:
            continue
        if now_ts - ob.get("timestamp", 0) > 600:
            continue
        if _is_noise_market(ob.get("market", "")):
            continue

        current = ob.get("current_price", 50) / 100.0
        side = ob.get("side", "YES")
        price = current if side == "YES" else 1.0 - current
        if price < MIN_ENTRY_PRICE or price > MAX_ENTRY_PRICE:
            continue

        ob_score = ob.get("score", 30)
        ob_mult = _source_size_multiplier("orderbook", source_stats)
        amount = _kelly_size(ob_score, price, eff_bankroll, ob_mult)
        if amount < 5:
            continue

        placed = await place_bet(
            market=ob.get("market", "Unknown"),
            slug=slug, side=side, price=price, amount=amount,
            source="orderbook", score=ob_score,
            detail={"bid_depth": ob.get("bid_depth"), "ask_depth": ob.get("ask_depth"),
                    "bid_ratio": ob.get("bid_ratio")},
        )
        if placed:
            bets_placed += 1
            open_slugs.add(slug)

    # ─── 10. LONGSHOT BIAS (sell overpriced YES) ───
    longshots = get_longshot_signals() if await _sig_on("longshot") else []
    for ls in longshots:
        if not await _can_bet_now(current_bet_limit) or open_count + bets_placed >= MAX_OPEN_BETS or exposure_cap_hit:
            break

        slug = ls.get("slug", "")
        if not slug or slug in open_slugs:
            continue
        if _is_noise_market(ls.get("market", "")):
            continue

        # Buy NO (= sell the overpriced YES)
        no_price = ls.get("no_price", 90) / 100.0
        if no_price < MIN_ENTRY_PRICE or no_price > MAX_ENTRY_PRICE:
            continue

        # Theme check
        ls_theme = _detect_theme(ls.get("market", ""))
        if ls_theme != "other" and await _theme_exposure(ls_theme) >= MAX_BETS_PER_THEME:
            continue

        ls_score = ls.get("score", 35)
        ls_mult = _source_size_multiplier("longshot", source_stats)
        amount = _kelly_size(ls_score, no_price, eff_bankroll, ls_mult)
        if amount < 5:
            continue

        placed = await place_bet(
            market=ls.get("market", "Unknown"),
            slug=slug, side="NO", price=no_price, amount=amount,
            source="longshot", score=ls_score,
            detail={"yes_price": ls.get("yes_price"), "edge_pct": ls.get("edge_pct")},
        )
        if placed:
            bets_placed += 1
            open_slugs.add(slug)

    # ─── 11. LEADERBOARD WHALE SIGNALS ───
    lb_signals = get_leaderboard_signals() if await _sig_on("leaderboard") else []
    for lb in lb_signals:
        if not await _can_bet_now(current_bet_limit) or open_count + bets_placed >= MAX_OPEN_BETS or exposure_cap_hit:
            break
        slug = lb.get("slug", "")
        if not slug or slug in open_slugs:
            continue
        if now_ts - lb.get("timestamp", 0) > 900:
            continue
        if _is_noise_market(lb.get("market", "")):
            continue
        side = lb.get("side", "YES")
        price = lb.get("price", 50) / 100.0
        if side == "NO":
            price = 1.0 - price
        if price < MIN_ENTRY_PRICE or price > MAX_ENTRY_PRICE:
            continue
        lb_mult = _source_size_multiplier("leaderboard", source_stats)
        amount = _kelly_size(lb.get("score", 45), price, eff_bankroll, lb_mult)
        if amount < 5:
            continue
        placed = await place_bet(
            market=lb.get("market", "Unknown"), slug=slug, side=side,
            price=price, amount=amount, source="leaderboard", score=lb.get("score", 45),
            detail={"wallet": lb.get("wallet"), "whale_size": lb.get("size")},
        )
        if placed:
            bets_placed += 1
            open_slugs.add(slug)

    # ─── 12. RESOLUTION SNIPER ───
    _sniper_list = get_resolution_sniper() if await _sig_on("sniper") else []
    for sn in _sniper_list:
        if not await _can_bet_now(current_bet_limit) or open_count + bets_placed >= MAX_OPEN_BETS or exposure_cap_hit:
            break
        slug = sn.get("slug", "")
        if not slug or slug in open_slugs or _is_noise_market(sn.get("market", "")):
            continue
        price = sn.get("price", 50) / 100.0
        if price < MIN_ENTRY_PRICE or price > MAX_ENTRY_PRICE:
            continue
        sn_mult = _source_size_multiplier("sniper", source_stats)
        amount = _kelly_size(sn.get("score", 50), price, eff_bankroll, sn_mult)
        if amount < 5:
            continue
        placed = await place_bet(
            market=sn.get("market", "Unknown"), slug=slug, side=sn.get("side", "YES"),
            price=price, amount=amount, source="sniper", score=sn.get("score", 50),
            detail={"days_left": sn.get("days_left"), "profit_pct": sn.get("profit_pct")},
        )
        if placed:
            bets_placed += 1
            open_slugs.add(slug)

    # ─── 13. MARKET CHAIN CASCADE ───
    _chain_list = get_chain_signals() if await _sig_on("chain") else []
    for ch in _chain_list:
        if not await _can_bet_now(current_bet_limit) or open_count + bets_placed >= MAX_OPEN_BETS or exposure_cap_hit:
            break
        slug = ch.get("slug", "")
        if not slug or slug in open_slugs or _is_noise_market(ch.get("market", "")):
            continue
        if now_ts - ch.get("timestamp", 0) > 900:
            continue
        side = ch.get("side", "YES")
        cp = ch.get("current_price", 50) / 100.0
        price = cp if side == "YES" else 1.0 - cp
        if price < MIN_ENTRY_PRICE or price > MAX_ENTRY_PRICE:
            continue
        ch_mult = _source_size_multiplier("chain", source_stats)
        amount = _kelly_size(ch.get("score", 30), price, eff_bankroll, ch_mult)
        if amount < 5:
            continue
        placed = await place_bet(
            market=ch.get("market", "Unknown"), slug=slug, side=side,
            price=price, amount=amount, source="chain", score=ch.get("score", 30),
            detail={"chain": ch.get("chain"), "lagging_by": ch.get("lagging_by")},
        )
        if placed:
            bets_placed += 1
            open_slugs.add(slug)

    # ─── 13. SETTLEMENT HARVESTER ───
    # Near-certain markets (95-99c): ~2-5% per bet, ~50% APR rolled. Low but
    # non-zero tail risk — sized separately from Kelly with per-bet and
    # aggregate caps so one tail hit can't blow up the account and the
    # strategy can't crowd out weather moonshots.
    _harvest_list = get_settled_bets() if await _sig_on("settlement") else []

    # Aggregate exposure budget: how much MORE we can safely deploy into
    # harvester across all open bets. Subtract current open-harvester
    # notional from the 50%-of-bankroll ceiling.
    try:
        _existing_open_bets = await pg_store.get_open_bets()
        _harvest_exposure = sum(
            float(b.get("bet_amount", 0) or 0)
            for b in _existing_open_bets
            if (b.get("signal_source") or "") == "harvest"
        )
    except Exception:
        _harvest_exposure = 0.0
    _harvest_budget_remaining = max(0.0, eff_bankroll * HARVEST_AGGREGATE_PCT - _harvest_exposure)

    # Diversification gate: don't pile 25% into a single candidate if it's
    # the ONLY one we found this scan — cap per-bet at 10% until we have
    # HARVEST_MIN_CANDIDATES distinct slugs queued.
    _distinct_candidates = len({h.get("slug") for h in _harvest_list if h.get("slug")})
    _diversified = _distinct_candidates >= HARVEST_MIN_CANDIDATES
    _harvest_this_scan = 0  # time-window correlation cap (HARVEST_MAX_PER_SCAN)

    for hv in _harvest_list:
        if not await _can_bet_now(current_bet_limit) or open_count + bets_placed >= MAX_OPEN_BETS or exposure_cap_hit:
            break
        if _harvest_this_scan >= HARVEST_MAX_PER_SCAN:
            await _brain(
                f"[HARVEST] Per-scan cap reached ({HARVEST_MAX_PER_SCAN} bets) — "
                "pausing to avoid correlated settlement cluster"
            )
            break
        if _harvest_budget_remaining < 5:
            await _brain(
                f"[HARVEST] Aggregate cap reached (${_harvest_exposure:.0f} of "
                f"${eff_bankroll * HARVEST_AGGREGATE_PCT:.0f}) — pausing harvester"
            )
            break
        slug = hv.get("slug", "")
        if not slug or slug in open_slugs:
            continue
        side = hv.get("winning_side", "YES")
        price = hv.get("buy_price", 0.97)  # Already a float 0-1
        if hv.get("is_subjective"):        # Skip subjective criteria markets
            continue
        layers = hv.get("layers_passed", 0)
        if layers < 4:                     # Require ≥4 safety layers
            continue
        dtr = float(hv.get("days_to_resolution", 999) or 999)
        if dtr > HARVEST_MAX_DTR_DAYS:
            continue

        # DTR-tiered price floor: longer locks demand more safety margin.
        _min_price = HARVEST_MIN_PRICE_SLOW if dtr > 7 else HARVEST_MIN_PRICE
        if price < _min_price or price > 0.99:
            continue

        # DTR-tiered sizing: fast-settle markets (≤3d) recycle capital quickly
        # so we can afford a larger per-bet fraction. Diversification gate
        # still overrides when candidate pool is thin.
        if not _diversified:
            _per_bet_pct = 0.10
        elif dtr <= HARVEST_FAST_SETTLE_DAYS:
            _per_bet_pct = HARVEST_FAST_PER_BET_PCT
        else:
            _per_bet_pct = HARVEST_PER_BET_PCT

        harvest_notional = min(
            eff_bankroll * _per_bet_pct,
            _harvest_budget_remaining,
        )
        if harvest_notional < 5:
            continue

        placed = await place_bet(
            market=hv.get("market", "Unknown"),
            slug=slug, side=side, price=price, amount=harvest_notional,
            source="harvest", score=65,
            detail={"layers": layers, "profit_pct": hv.get("profit_pct"),
                    "days_to_resolution": dtr,
                    "diversified": _diversified,
                    "per_bet_pct": round(_per_bet_pct, 3)},
        )
        if placed:
            bets_placed += 1
            _harvest_this_scan += 1
            open_slugs.add(slug)
            # place_bet may have depth-capped the amount; use the intent here
            # for budget tracking since we can't see the final fill from here.
            _harvest_exposure += harvest_notional
            _harvest_budget_remaining = max(0.0, eff_bankroll * HARVEST_AGGREGATE_PCT - _harvest_exposure)
            profit_est = hv.get("profit_pct", 0)
            await _brain(
                f"[HARVEST] Near-certain: \"{hv.get('market','')[:40]}\" — "
                f"BUY {side} @ {price*100:.0f}c | est. +{profit_est:.1f}% | "
                f"{layers} layers | DTR {dtr:.1f}d | ${harvest_notional:.0f} "
                f"({_per_bet_pct*100:.0f}% pb, agg ${_harvest_exposure:.0f}/"
                f"${eff_bankroll * HARVEST_AGGREGATE_PCT:.0f})"
            )

    # ─── 11. WEATHER FORECAST SIGNALS ───
    weather = get_weather_signals() if await _sig_on("weather") else []
    for w in weather:
        if not await _can_bet_now(current_bet_limit) or open_count + bets_placed >= MAX_OPEN_BETS + MOONSHOT_EXTRA_SLOTS:
            break
        # When at normal cap, only moonshots (EV≥threshold) get through
        if open_count + bets_placed >= MAX_OPEN_BETS:
            if not (w.get("ev", 0) >= MOONSHOT_EV_THRESHOLD and w.get("probability", 0) >= 95):
                continue

        slug = w.get("slug", "")
        if not slug or slug in open_slugs:
            continue
        if now_ts - w.get("timestamp", 0) > 3600:  # Weather signals valid for 1 hour
            continue

        price = w.get("price", 0.5)
        w_ev_pre = w.get("ev", 0)
        w_prob_pre = w.get("probability", 0)
        is_moonshot_pre = w_ev_pre >= MOONSHOT_EV_THRESHOLD and w_prob_pre >= 95
        # Moonshots bypass the 10¢ price floor — they're designed for drastically mispriced
        # markets (3-9¢) where EV is extreme. MAX_ENTRY_PRICE ceiling still applies.
        if is_moonshot_pre:
            if price > MAX_ENTRY_PRICE:
                continue
        elif price < MIN_ENTRY_PRICE or price > MAX_ENTRY_PRICE:
            continue

        # Recheck hours_left from end_date — cached value is stale
        from app.services.weather_engine import MIN_HOURS as WE_MIN_HOURS, _hours_to_resolution
        real_hours = _hours_to_resolution(w.get("end_date", ""))
        if real_hours < WE_MIN_HOURS:
            await _brain(f"Skip weather bet {w.get('city')} — only {real_hours:.1f}h left (min {WE_MIN_HOURS}h)")
            continue

        # Theme check (weather is its own theme — limit concurrent weather bets)
        weather_open = sum(1 for s in open_slugs if "temperature" in s.lower() or "temp" in s.lower())
        if weather_open >= 30:  # Max 30 concurrent weather bets — polybot-arena playbook: weather bets are independent events, lean into breadth
            continue

        w_score = w.get("score", 40)
        w_ev = w.get("ev", 0)
        w_prob = w.get("probability", 0)
        w_mult = _source_size_multiplier("weather", source_stats)

        # Moonshot sizing: extreme EV + near-certain probability
        # Model disagreement and boundary risk already filtered in weather_engine.py —
        # any signal reaching here with EV>MOONSHOT_EV_THRESHOLD is genuinely mispriced.
        is_moonshot = w_ev >= MOONSHOT_EV_THRESHOLD and w_prob >= 95
        if is_moonshot:
            # 20% of bankroll, capped at 30% to prevent single-bet catastrophe
            amount = min(eff_bankroll * MOONSHOT_SIZE_PCT, eff_bankroll * 0.30)
            # Alert immediately on detection — fires even if at capacity
            if slug not in _moonshot_notified:
                _moonshot_notified.add(slug)
                _notify(
                    f"🚀 MOONSHOT DETECTED — {w.get('side','YES')} @ {price*100:.0f}¢",
                    f"{w.get('city')} {w.get('bucket')} | EV +{w_ev:.0f}% | prob {w_prob:.0f}%\n"
                    f"Auto-sizing ${amount:.0f} ({MOONSHOT_SIZE_PCT*100:.0f}% bankroll) — CHECK DASHBOARD",
                    priority="urgent",
                )
        else:
            vol_mult = _weather_volume_multiplier(w.get("volume", 0))
            amount = _kelly_size(w_score, price, eff_bankroll, w_mult * vol_mult)
            # Dynamic cap: scales with bankroll (max of hard cap or 4% of bankroll)
            dynamic_cap = max(MAX_WEATHER_BET, eff_bankroll * 0.04)
            amount = min(amount, dynamic_cap)

        if amount < 5:
            continue

        placed = await place_bet(
            market=w.get("market", "Unknown"),
            slug=slug, side=w.get("side", "YES"), price=price, amount=amount,
            source="weather", score=w_score,
            detail={"city": w.get("city"), "city_slug": w.get("city_slug"),
                    "forecast": w.get("forecast_temp"),
                    "ev": w_ev, "bucket": w.get("bucket"),
                    "source": w.get("forecast_source"),
                    "date": w.get("date", ""),
                    "end_date": w.get("end_date", ""),
                    "volume": w.get("volume", 0),
                    "moonshot": is_moonshot},
        )
        if placed:
            bets_placed += 1
            open_slugs.add(slug)
            if is_moonshot:
                await _brain(
                    f"🚀 MOONSHOT: {w.get('city')} {w.get('bucket')} — "
                    f"EV +{w_ev:.0f}%, prob {w_prob:.0f}% | "
                    f"${amount:.0f} @ {price*100:.0f}c ({w.get('forecast_source')})"
                )
            else:
                await _brain(
                    f"WEATHER BET: {w.get('city')} {w.get('bucket')} — "
                    f"forecast {w.get('forecast_temp')} ({w.get('forecast_source')}), "
                    f"EV +{w_ev}%, ${amount:.0f} @ {price*100:.0f}c"
                )

    # ─── 14. PANIC FADE ───
    _panic_list = get_panic_fade_signals() if await _sig_on("momentum") else []
    for pf in _panic_list:
        if not await _can_bet_now(current_bet_limit) or open_count + bets_placed >= MAX_OPEN_BETS or exposure_cap_hit:
            break
        slug = pf.get("slug", "")
        if not slug or slug in open_slugs or _is_noise_market(pf.get("market", "")):
            continue
        if now_ts - pf.get("timestamp", 0) > 900:  # 15min freshness
            continue
        price = pf.get("current_price", 20) / 100.0
        if price < MIN_ENTRY_PRICE or price > MAX_ENTRY_PRICE:
            continue
        pf_mult = _source_size_multiplier("momentum", source_stats)
        pf_score = pf.get("score", 40)
        amount = _kelly_size(pf_score, price, eff_bankroll, pf_mult)
        if amount < 5:
            continue
        placed = await place_bet(
            market=pf.get("market", "Unknown"), slug=slug, side="YES",
            price=price, amount=amount, source="panic_fade", score=pf_score,
            detail={"drop_pct": pf.get("drop_pct"), "peak_price": pf.get("peak_price"),
                    "rebound_target": pf.get("rebound_target")},
        )
        if placed:
            bets_placed += 1
            open_slugs.add(slug)
            await _brain(
                f"[PANIC FADE] \"{pf.get('market','')[:40]}\" — "
                f"crashed {pf.get('drop_pct')}% to {pf.get('current_price')}c | "
                f"target {pf.get('rebound_target')}c | ${amount:.0f}"
            )

    # ─── 15. FINAL PERIOD MOMENTUM ───
    _final_list = get_final_period_signals() if await _sig_on("sniper") else []
    for fp in _final_list:
        if not await _can_bet_now(current_bet_limit) or open_count + bets_placed >= MAX_OPEN_BETS or exposure_cap_hit:
            break
        slug = fp.get("slug", "")
        if not slug or slug in open_slugs or _is_noise_market(fp.get("market", "")):
            continue
        if now_ts - fp.get("timestamp", 0) > 600:  # 10min freshness (fast-moving)
            continue
        price = fp.get("current_price", 85) / 100.0
        if price < MIN_ENTRY_PRICE or price > MAX_ENTRY_PRICE:
            continue
        fp_mult = _source_size_multiplier("sniper", source_stats)
        fp_score = fp.get("score", 60)
        amount = _kelly_size(fp_score, price, eff_bankroll, fp_mult)
        if amount < 5:
            continue
        placed = await place_bet(
            market=fp.get("market", "Unknown"), slug=slug, side="YES",
            price=price, amount=amount, source="final_period", score=fp_score,
            detail={"hours_left": fp.get("hours_left"), "profit_pct": fp.get("profit_pct"),
                    "low_30min": fp.get("low_30min")},
        )
        if placed:
            bets_placed += 1
            open_slugs.add(slug)
            await _brain(
                f"[FINAL PERIOD] \"{fp.get('market','')[:40]}\" — "
                f"crossed 80c with {fp.get('hours_left')}h left | "
                f"now {fp.get('current_price')}c → +{fp.get('profit_pct'):.1f}% | ${amount:.0f}"
            )

    # ─── 16. RESULT REVERSAL LOTTERY ───
    # Buys near-zero side of a sports market that just crashed (still open).
    # Special: bypasses MIN/MAX_ENTRY_PRICE — these are 1-5¢ lottery tickets.
    # Bet size capped at $15 regardless of Kelly (highly uncertain probability).
    _reversal_list = get_result_reversal_signals() if await _sig_on("momentum") else []
    for rv in _reversal_list:
        if not await _can_bet_now(current_bet_limit) or open_count + bets_placed >= MAX_OPEN_BETS or exposure_cap_hit:
            break
        slug = rv.get("slug", "")
        if not slug or slug in open_slugs or _is_noise_market(rv.get("market", "")):
            continue
        if now_ts - rv.get("timestamp", 0) > 600:  # Must be very fresh (10min)
            continue
        buy_price = rv.get("buy_price", 0.0)
        if buy_price <= 0.005 or buy_price > 0.05:  # Enforce 0.5¢-5¢ window
            continue
        rv_score = rv.get("score", 40)
        # Kelly sizing but hard-capped at $15 — base probability is uncertain
        amount = min(15.0, _kelly_size(rv_score, buy_price, eff_bankroll, 1.0))
        if amount < 1.0:
            continue
        side = rv.get("side", "YES")
        placed = await place_bet(
            market=rv.get("market", "Unknown"), slug=slug, side=side,
            price=buy_price, amount=amount, source="result_reversal", score=rv_score,
            detail={"crash_from": rv.get("crash_from"), "crash_age_min": rv.get("crash_age_min"),
                    "ev": rv.get("ev"), "reversal_prob_pct": rv.get("reversal_prob_pct")},
        )
        if placed:
            bets_placed += 1
            open_slugs.add(slug)
            await _brain(
                f"[REVERSAL] \"{rv.get('market','')[:40]}\" — "
                f"{side} @ {rv.get('price')}c (crashed from {rv.get('crash_from')}c "
                f"{rv.get('crash_age_min', 0):.0f}min ago) | EV +{rv.get('ev')}% | ${amount:.0f}"
            )
            _notify(
                f"🚨 REVERSAL TICKET — {side} @ {rv.get('price')}¢",
                f"{rv.get('market','')[:60]}\n"
                f"Crashed from {rv.get('crash_from')}¢ → {rv.get('price')}¢ "
                f"({rv.get('crash_age_min', 0):.0f}min ago)\n"
                f"EV +{rv.get('ev')}% | ${amount:.0f} risked | CHECK NOW (10min window)",
                priority="urgent",
            )

    # ─── DAILY SNAPSHOT ───
    await _save_daily_snapshot()

    # ─── LOG PORTFOLIO SUMMARY ───
    portfolio = await get_portfolio()
    roi = portfolio["roi_pct"]
    sign = "+" if roi >= 0 else ""
    hr_count = await _bets_this_hour()
    await ws_manager.send_log(
        f"[SIM] Portfolio: ${portfolio['bankroll']:.0f} ({sign}{roi:.1f}%) | "
        f"{portfolio['open_bets']} open ({bets_placed} new) | "
        f"{portfolio['resolved']} resolved | "
        f"{portfolio['win_rate']:.0f}% win rate | "
        f"{hr_count}/{MAX_BETS_PER_HOUR} bets/hr",
        "info",
    )


# ══════════════════════════════════════════════════════════════
# DAILY SNAPSHOTS
# ══════════════════════════════════════════════════════════════

async def _save_daily_snapshot():
    """Save a daily snapshot of bankroll for the P&L curve."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    bankroll = await _get_bankroll()

    # Get win/loss counts — try Postgres first (on Railway all bets are there)
    total, wins, losses = 0, 0, 0
    try:
        pg_stats = await pg_store.get_stats()
        if pg_stats.get("total_bets", 0) > 0:
            total = pg_stats.get("total_bets", 0)
            wins = pg_stats.get("wins", 0)
            losses = pg_stats.get("losses", 0)
    except Exception:
        pass

    if total == 0:
        # Fallback to SQLite
        db_snap = await get_db()
        try:
            cursor = await db_snap.execute("SELECT COUNT(*) FROM polymarket_sim_bets")
            total = (await cursor.fetchone())[0]
            cursor = await db_snap.execute("SELECT COUNT(*) FROM polymarket_sim_bets WHERE status = 'won'")
            wins = (await cursor.fetchone())[0]
            cursor = await db_snap.execute("SELECT COUNT(*) FROM polymarket_sim_bets WHERE status = 'lost'")
            losses = (await cursor.fetchone())[0]
        finally:
            await db_snap.close()

    db = await get_db()
    try:
        await db.execute(
            """INSERT INTO polymarket_sim_snapshots (date, bankroll, total_bets, wins, losses)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(date) DO UPDATE SET bankroll=?, total_bets=?, wins=?, losses=?""",
            (today, round(bankroll, 2), total, wins, losses,
             round(bankroll, 2), total, wins, losses),
        )
        await db.commit()
    finally:
        await db.close()

    # Also save to Postgres (survives deploys)
    try:
        await pg_store.save_snapshot(bankroll)
    except Exception:
        pass

    # v3.20.10 — paper-engine daily digest removed. The simulator is
    # internal training data in signals-only mode; paper bankroll/WR/ROI
    # pushed to the user's phone every night was noise about fake money.
    # Local-time vars below still computed because the Reddit intel digest
    # uses the same window.
    try:
        from zoneinfo import ZoneInfo
        _tz_name = os.getenv("TELEGRAM_DIGEST_TZ", "Europe/Athens")
        try:
            _tz = ZoneInfo(_tz_name)
        except Exception:
            _tz = ZoneInfo("UTC")
        _cutoff_h = int(os.getenv("TELEGRAM_DIGEST_HOUR", "23"))
        _cutoff_m = int(os.getenv("TELEGRAM_DIGEST_MINUTE", "59"))
        _now_local = datetime.now(_tz)
        _local_date = _now_local.strftime("%Y-%m-%d")
    except Exception:
        from datetime import datetime as _dt
        from zoneinfo import ZoneInfo as _Zi
        _cutoff_h, _cutoff_m = 23, 59
        _now_local = _dt.now(_Zi("UTC"))
        _local_date = today

    # ─── REDDIT INTEL DIGEST (once per local day, fires before the recap) ───
    # Scans 10 prediction-market/trading subs via public JSON and pushes one
    # Sonnet-distilled Telegram message. Default REDDIT_INTEL_HOUR=22 — one hour
    # before the daily recap at 23:00.
    global _reddit_intel_last_date
    try:
        _ri_hour = int(os.getenv("REDDIT_INTEL_HOUR", "22"))
        _ri_min = int(os.getenv("REDDIT_INTEL_MINUTE", "0"))
        _ri_past = (_now_local.hour, _now_local.minute) >= (_ri_hour, _ri_min)
        # Only fire in the window [REDDIT_INTEL_HOUR, TELEGRAM_DIGEST_HOUR) to
        # avoid edge cases where the recap cutoff already passed.
        if _ri_past and (_now_local.hour, _now_local.minute) < (_cutoff_h, _cutoff_m) \
                and _reddit_intel_last_date != _local_date:
            try:
                from app.services.reddit_intel import run_daily_intel
                await run_daily_intel()
                _reddit_intel_last_date = _local_date
            except Exception as _re:
                logger.warning("reddit intel digest failed: %s", _re)
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════
# QUERY FUNCTIONS
# ══════════════════════════════════════════════════════════════

async def get_portfolio() -> dict:
    """Return bankroll, open bets count, and key stats.
    Reads from Postgres first (persists across deploys), falls back to SQLite.
    """
    bankroll = await _get_bankroll()

    # Try Postgres first for stats (survives deploys)
    pg_stats = await pg_store.get_stats()
    pg_open = await pg_store.get_open_bet_count()

    if pg_stats.get("total_bets", 0) > 0:
        # Postgres has data — use it
        total_wins = pg_stats.get("wins", 0)
        total_losses = pg_stats.get("losses", 0)
        total_decided = total_wins + total_losses
        win_rate = (total_wins / total_decided * 100) if total_decided > 0 else 0
        resolved = pg_stats.get("total_bets", 0)
        open_bets = pg_open or 0
        total_pnl = float(pg_stats.get("total_pnl", 0))
    else:
        # Fallback to SQLite
        db = await get_db()
        try:
            cursor = await db.execute("SELECT COUNT(*) FROM polymarket_sim_bets WHERE status = 'open'")
            open_bets = (await cursor.fetchone())[0]
            cursor = await db.execute("SELECT COUNT(*) FROM polymarket_sim_bets WHERE status IN ('won', 'lost', 'sold')")
            resolved = (await cursor.fetchone())[0]
            cursor = await db.execute("SELECT COUNT(*) FROM polymarket_sim_bets WHERE status IN ('won', 'sold') AND pnl > 0")
            total_wins = (await cursor.fetchone())[0]
            cursor = await db.execute("SELECT COUNT(*) FROM polymarket_sim_bets WHERE (status = 'lost' OR (status = 'sold' AND pnl <= 0))")
            total_losses = (await cursor.fetchone())[0]
            total_decided = total_wins + total_losses
            win_rate = (total_wins / total_decided * 100) if total_decided > 0 else 0
        finally:
            await db.close()

    pnl = bankroll - STARTING_BANKROLL
    roi = (pnl / STARTING_BANKROLL) * 100

    # Today's PnL — sum of pnl on sim_bets resolved since 00:00 UTC today.
    # Was hardcoded to 0 since forever (TODO that never landed). Postgres-first,
    # SQLite fallback. Never raises into the parent — defaults to 0 on failure.
    today_pnl = 0.0
    try:
        pg_pool = await pg_store._get_pool()
        if pg_pool:
            async with pg_pool.acquire() as conn:
                row = await conn.fetchrow("""
                    SELECT COALESCE(SUM(pnl), 0) AS total
                    FROM sim_bets
                    WHERE resolved_at >= date_trunc('day', NOW() AT TIME ZONE 'UTC')
                      AND status IN ('won', 'lost', 'sold')
                """)
                today_pnl = float(row["total"]) if row else 0.0
        else:
            db_t = await get_db()
            try:
                cur = await db_t.execute("""
                    SELECT COALESCE(SUM(pnl), 0)
                    FROM polymarket_sim_bets
                    WHERE resolved_at >= date('now')
                      AND status IN ('won', 'lost', 'sold')
                """)
                row = await cur.fetchone()
                today_pnl = float(row[0]) if row and row[0] is not None else 0.0
            finally:
                await db_t.close()
    except Exception as e:
        logger.warning(f"today_pnl calc failed (defaulting to 0): {e}")

    return {
        "bankroll": round(bankroll, 2),
        "starting": STARTING_BANKROLL,
        "pnl": round(pnl, 2),
        "roi_pct": round(roi, 1),
        "open_bets": open_bets,
        "max_open_bets": MAX_OPEN_BETS,
        "resolved": resolved,
        "wins": total_wins,
        "losses": total_losses,
        "win_rate": round(win_rate, 1),
        "today_pnl": round(today_pnl, 2),
        "bets_this_hour": await _bets_this_hour(),
        "max_bets_per_hour": MAX_BETS_PER_HOUR,
    }


async def get_open_bets() -> list[dict]:
    """Return all open bets. Tries Postgres first, falls back to SQLite."""
    # Try Postgres first (persists across deploys) — check pool availability, not list length
    try:
        pool = await pg_store._get_pool()
        if pool:
            pg_bets = await pg_store.get_open_bets()
            return [{
                "id": b.get("id"),
                "timestamp": b.get("timestamp", ""),
                "market": b.get("market", ""),
                "slug": b.get("market_slug", ""),
                "side": b.get("side", ""),
                "entry_price": float(b.get("entry_price", 0)),
                "bet_amount": float(b.get("bet_amount", 0)),
                "shares": float(b.get("shares", 0)),
                "source": b.get("signal_source", ""),
                "score": float(b.get("score", 0)),
                "signal_detail": b.get("signal_detail", ""),
            } for b in pg_bets]
    except Exception:
        pass

    # Fallback to SQLite
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT id, timestamp, market, market_slug, side, entry_price, bet_amount, shares, signal_source, score, signal_detail "
            "FROM polymarket_sim_bets WHERE status = 'open' ORDER BY timestamp DESC"
        )
        rows = await cursor.fetchall()
        return [{
            "id": r[0], "timestamp": r[1], "market": r[2], "slug": r[3],
            "side": r[4], "entry_price": r[5] or 0, "bet_amount": r[6] or 0,
            "shares": r[7], "source": r[8], "score": r[9] or 0,
            "signal_detail": r[10] or "",
        } for r in rows]
    finally:
        await db.close()


async def get_bet_history(limit: int = 50) -> list[dict]:
    """Return recent resolved bets. Tries Postgres first, falls back to SQLite."""
    pg_history = await pg_store.get_bet_history(limit)
    if pg_history:
        return [{
            "id": b.get("id"),
            "timestamp": b.get("timestamp", ""),
            "market": b.get("market", ""),
            "slug": b.get("market_slug", ""),
            "side": b.get("side", ""),
            "entry_price": float(b.get("entry_price", 0)),
            "exit_price": float(b.get("exit_price") or 0),
            "bet_amount": float(b.get("bet_amount", 0)),
            "pnl": float(b.get("pnl") or 0),
            "source": b.get("signal_source", ""),
            "status": b.get("status", ""),
            "resolved_at": b.get("resolved_at", ""),
            "score": float(b.get("score", 0)),
        } for b in pg_history]

    # Fallback to SQLite
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT id, timestamp, market, market_slug, side, entry_price, exit_price, "
            "bet_amount, pnl, signal_source, status, resolved_at, score "
            "FROM polymarket_sim_bets WHERE status != 'open' "
            "ORDER BY resolved_at DESC LIMIT ?",
            (limit,),
        )
        rows = await cursor.fetchall()
        return [{
            "id": r[0], "timestamp": r[1], "market": r[2], "slug": r[3],
            "side": r[4], "entry_price": r[5], "exit_price": r[6],
            "bet_amount": r[7], "pnl": r[8], "source": r[9],
            "status": r[10], "resolved_at": r[11], "score": r[12] or 0,
        } for r in rows]
    finally:
        await db.close()


async def get_pnl_curve() -> list[dict]:
    """Return daily bankroll snapshots. Tries Postgres first, falls back to SQLite."""
    pg_snaps = await pg_store.get_snapshots()
    if pg_snaps:
        return [{"date": s.get("date", ""), "bankroll": float(s.get("bankroll", 0)),
                 "total_bets": 0, "wins": 0, "losses": 0} for s in pg_snaps]

    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT date, bankroll, total_bets, wins, losses "
            "FROM polymarket_sim_snapshots ORDER BY date ASC"
        )
        rows = await cursor.fetchall()
        return [
            {"date": r[0], "bankroll": r[1], "total_bets": r[2], "wins": r[3], "losses": r[4]}
            for r in rows
        ]
    finally:
        await db.close()


async def get_performance_by_source() -> dict:
    """Breakdown by signal source. Tries Postgres first, falls back to SQLite."""
    pg_perf = await pg_store.get_performance_by_source()
    if pg_perf:
        result = {}
        for source, data in pg_perf.items():
            wins = int(data.get("wins", 0))
            losses = int(data.get("losses", 0))
            total = wins + losses
            result[source or "unknown"] = {
                "wins": wins,
                "losses": losses,
                "total": total,
                "win_rate": round(wins / total * 100, 1) if total > 0 else 0,
                "pnl": round(float(data.get("total_pnl", 0)), 2),
            }
        return result

    # Fallback to SQLite — query ALL sources dynamically
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT DISTINCT signal_source FROM polymarket_sim_bets WHERE status != 'open' AND signal_source IS NOT NULL"
        )
        source_rows = await cursor.fetchall()
        sources = [r[0] for r in source_rows if r[0]]
        if not sources:
            sources = ["mispricing", "copy", "consensus"]
        result = {}
        for src in sources:
            cursor = await db.execute(
                "SELECT COUNT(*) FROM polymarket_sim_bets "
                "WHERE signal_source = ? AND status IN ('won', 'sold') AND pnl > 0",
                (src,),
            )
            wins = (await cursor.fetchone())[0]
            cursor = await db.execute(
                "SELECT COUNT(*) FROM polymarket_sim_bets "
                "WHERE signal_source = ? AND (status = 'lost' OR (status = 'sold' AND pnl <= 0))",
                (src,),
            )
            losses = (await cursor.fetchone())[0]
            cursor = await db.execute(
                "SELECT COALESCE(SUM(pnl), 0) FROM polymarket_sim_bets "
                "WHERE signal_source = ? AND status IN ('won', 'lost', 'sold')",
                (src,),
            )
            pnl = (await cursor.fetchone())[0]
            total = wins + losses
            result[src] = {
                "wins": wins, "losses": losses, "total": total,
                "win_rate": round(wins / total * 100, 1) if total > 0 else 0,
                "pnl": round(pnl, 2),
            }
        return result
    finally:
        await db.close()
