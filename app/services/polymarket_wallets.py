# SPDX-License-Identifier: MIT

"""
Polymarket Copy-Trading Intelligence System v4

Four integrated components:
1. Multi-Source Mispricing Scanner — compares Polymarket vs Manifold + Kalshi + news
2. Composite Signal Scoring — 0-100 score combining edge, sources, wallet quality, time, news
3. Wallet Win-Rate Scoring — tracks wallet performance over time
4. Smart Copy Signals — generates copy signals from top-rated wallets

Data sources:
- Polymarket trades: data-api.polymarket.com/trades (free, no key)
- Polymarket markets: gamma-api.polymarket.com/markets (free, no key)
- Manifold Markets: api.manifold.markets/v0/search-markets (free, no key)
- Kalshi Markets: api.elections.kalshi.com/trade-api/v2/markets (free, no key)
- Google News RSS: news.google.com/rss/search (free, no key)
- Claude Haiku: confirmation on score >= 50 signals (optional, ~$0.001/call)

Scan interval: 5 minutes. Near-zero API cost.
"""

import asyncio
import json
import logging
import math
import re
import time
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import datetime, timezone
from urllib.parse import quote_plus

import requests

from app.config import settings
from app.services.signal_config import is_signal_enabled
from app.services.websocket_manager import ws_manager


async def _sig_scan_on(signal_id: str) -> bool:
    """Gate a scan-time detector on signal_config. Disabled signals skip scan entirely."""
    try:
        return await is_signal_enabled(signal_id)
    except Exception:
        return True

logger = logging.getLogger(__name__)


async def _brain(msg: str):
    """Send a brain narration message (human-readable bot thinking)."""
    await ws_manager.send_log(msg, "brain")

# ══════════════════════════════════════════════════════════════
# API ENDPOINTS
# ══════════════════════════════════════════════════════════════

TRADES_API = "https://data-api.polymarket.com/trades"
POLYMARKET_MARKETS_API = "https://gamma-api.polymarket.com/markets"
POLYMARKET_CLOB_API = "https://clob.polymarket.com"
MANIFOLD_SEARCH_API = "https://api.manifold.markets/v0/search-markets"
KALSHI_MARKETS_API = "https://api.elections.kalshi.com/trade-api/v2/markets"

# ══════════════════════════════════════════════════════════════
# NOISE FILTERS
# ══════════════════════════════════════════════════════════════

_noise_patterns: list[str] = [
    "up or down",
    "spread:",
    "over/under",
    "5-minute",
    "5 minute",
    "1-minute",
    "1 minute",
    "next 15",
    "next 30",
    "next candle",
    "above or below",
    "higher or lower",
    "bitcoin up",
    "bitcoin down",
    "ethereum up",
    "ethereum down",
    "solana up",
    "solana down",
    "btc up",
    "btc down",
    "eth up",
    "eth down",
    "sol up",
    "sol down",
    "price at",
    "close above",
    "close below",
    # "highest temperature" and "lowest temperature" removed — handled by weather_engine.py
    "map 1 winner",
    "map 2 winner",
    "map 3 winner",
    "game 1 winner",
    "game 2 winner",
    "game 3 winner",
    "end in a draw",
]

# ══════════════════════════════════════════════════════════════
# STATE
# ══════════════════════════════════════════════════════════════

# Component 1: Mispricing scanner
_mispricing_cache: list[dict] = []
_mispricing_last_scan: float = 0

# Feature 2: Rapid price move tracking
_price_history: dict[str, list[tuple[float, float]]] = {}  # slug -> [(timestamp, price), ...]
_rapid_moves_cache: list[dict] = []

# Feature 4: Correlated arbitrage cache
_correlated_arbs_cache: list[dict] = []

# Feature 6: Outcome group overround cache (sum > 100%)
_overround_arbs_cache: list[dict] = []

# Feature 7: Momentum / mean reversion signals
_momentum_signals_cache: list[dict] = []

# Signal 16: Panic fade (8%+ drop under 30c)
_panic_fade_cache: list[dict] = []

# Signal 18: Result reversal lottery (sports crash to near-zero, still open)
_result_reversals_cache: list[dict] = []

# Feature 8: Breaking news speed edge
_news_speed_signals_cache: list[dict] = []

# Feature 9: YES+NO arbitrage (guaranteed profit)
_yesno_arb_cache: list[dict] = []

# Feature 10: Orderbook depth signals
_orderbook_signals_cache: list[dict] = []

# Feature 11: Longshot bias exploitation
_longshot_signals_cache: list[dict] = []

# Feature 12: Leaderboard whale signals
_leaderboard_signals_cache: list[dict] = []
_leaderboard_wallets: set[str] = set()

# Feature 13: Resolution calendar sniper
_resolution_sniper_cache: list[dict] = []

# Signal 17: Final period momentum (crosses above 80c in last 24h of market life)
_final_period_cache: list[dict] = []

# Feature 14: Market chain cascade
_chain_signals_cache: list[dict] = []

# Feature 12: Settlement harvester (near-certain resolved markets)
_settled_bets_cache: list[dict] = []
_settled_bets_last_scan: float = 0

# Component 2: Wallet scoring (persists across scans)
_wallet_scores: dict[str, dict] = {}
# wallet -> {trades: int, wins: int, losses: int, pending: int,
#             win_rate: float, volume: float, last_seen: float,
#             markets: set, recent_trades: list[dict]}

# Component 3: Copy signals
_copy_signals: list[dict] = []

# Legacy state (kept for backward compat)
_recent_trades: list[dict] = []
_big_trades: list[dict] = []
_consensus: list[dict] = []
_last_scan_time: float = 0
_scan_error: str | None = None
_scanner_task: asyncio.Task | None = None

# Resolved market tracking (slug -> outcome: "YES" | "NO" | None)
_resolved_markets: dict[str, str | None] = {}

# Track which trades we have already seen (by trade ID or hash)
_seen_trade_ids: set[str] = set()


# ══════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════

def _is_noise(title: str) -> bool:
    """Return True if this market title matches a noise pattern."""
    lower = title.lower()
    for pat in _noise_patterns:
        if pat in lower:
            return True
    return False


def _truncate_addr(addr: str) -> str:
    """Truncate wallet address for display."""
    if not addr or len(addr) < 12:
        return addr or "???"
    return addr[:6] + "..." + addr[-4:]


def _extract_keywords(question: str) -> list[str]:
    """Extract meaningful search keywords from a market question."""
    # Remove common filler words
    stopwords = {
        "will", "the", "be", "a", "an", "in", "on", "at", "to", "by", "of",
        "or", "and", "is", "it", "this", "that", "for", "with", "from", "as",
        "not", "no", "yes", "do", "does", "did", "has", "have", "had", "was",
        "were", "been", "being", "are", "its", "their", "they", "them",
        "before", "after", "than", "more", "less", "over", "under",
    }
    # Clean question
    clean = re.sub(r'[^\w\s]', ' ', question)
    words = [w for w in clean.split() if w.lower() not in stopwords and len(w) > 2]
    # Take first 4 meaningful words
    return words[:4]


def _market_url(slug: str, event_slug: str = "") -> str:
    """Build Polymarket URL from slug.

    v3.20.3 — prefer the canonical `/event/{event_slug}/{slug}` form (200 OK)
    over the legacy `/market/{slug}` form (307 redirect). Telegram's in-app
    browser and some mobile-Safari setups fail silently on the 307 hop and
    land on the Polymarket homepage instead of the market. Callers that
    pass event_slug get the canonical URL; others keep the old behaviour.
    """
    if not slug:
        return ""
    if event_slug:
        return f"https://polymarket.com/event/{event_slug}/{slug}"
    return f"https://polymarket.com/market/{slug}"


def _event_slug_from_pm(pm: dict) -> str:
    """Extract the canonical event slug from a Gamma `markets` payload.

    Every market belongs to an event; Gamma embeds it in the `events`
    array. Returns "" if the payload is malformed or the field is missing.
    """
    try:
        events = pm.get("events") or []
        if events and isinstance(events, list):
            return (events[0] or {}).get("slug", "") or ""
    except Exception:
        pass
    return ""


def get_yes_price(market: dict) -> float:
    """Extract YES price from a market dict. Returns 0.0 on failure."""
    try:
        prices = json.loads(market.get("outcomePrices", "[]"))
        if prices and len(prices) >= 1:
            return float(prices[0])
    except (json.JSONDecodeError, ValueError, IndexError):
        pass
    return 0.0


# ══════════════════════════════════════════════════════════════
# FEATURE 5: RESOLUTION PREDICTION
# ══════════════════════════════════════════════════════════════

def _estimate_resolution_type(question: str) -> dict:
    """Estimate when and how a market will resolve based on the question text."""
    q = question.lower()

    # Sports: resolve within hours
    if any(w in q for w in ["vs.", "vs ", "game", "match", "fight"]):
        return {"type": "sports", "typical_hours": 4, "confidence": "high"}

    # Short-term crypto: resolve within hours/days
    if any(w in q for w in ["up or down", "bitcoin above", "price of"]):
        return {"type": "crypto_price", "typical_hours": 24, "confidence": "high"}

    # Monthly political: resolve within weeks
    if any(w in q for w in ["by april", "by may", "by june", "by end of"]):
        return {"type": "deadline", "typical_hours": 720, "confidence": "medium"}

    # Elections: resolve on specific dates
    if any(w in q for w in ["election", "win the 202", "nomination"]):
        return {"type": "election", "typical_hours": 4320, "confidence": "medium"}

    # Default: unknown timeline
    return {"type": "unknown", "typical_hours": None, "confidence": "low"}


# ══════════════════════════════════════════════════════════════
# FEATURE 2: ODDS MOVEMENT SPEED DETECTION
# ══════════════════════════════════════════════════════════════

def _detect_rapid_moves(markets: list) -> list:
    """Detect markets where price moved 5%+ in last 30 minutes."""
    global _rapid_moves_cache
    rapid_moves = []
    now = time.time()

    for market in markets:
        slug = market.get("slug", "")
        if not slug:
            continue
        current_price = get_yes_price(market)
        if current_price <= 0.0:
            continue

        # Update price history
        if slug not in _price_history:
            _price_history[slug] = []
        _price_history[slug].append((now, current_price))
        # Keep only last 2 hours
        _price_history[slug] = [(t, p) for t, p in _price_history[slug] if now - t < 7200]

        # Check if price moved 5%+ in last 30 min
        for ts, old_price in _price_history[slug]:
            if now - ts <= 1800 and now - ts >= 300:  # 5-30 min ago
                move = abs(current_price - old_price)
                if move >= 0.05:  # 5% move
                    direction = "UP" if current_price > old_price else "DOWN"
                    rapid_moves.append({
                        "market": market.get("question", ""),
                        "slug": slug,
                        "market_url": _market_url(slug),
                        "move_pct": round(move * 100, 1),
                        "direction": direction,
                        "old_price": round(old_price * 100, 1),
                        "new_price": round(current_price * 100, 1),
                        "minutes_ago": round((now - ts) / 60),
                    })
                    break  # One detection per market

    # v3.22.1 — evict stale slug keys. Without this, Polymarket's churn of
    # hundreds of new markets/day leaves _price_history growing by every
    # slug we've ever seen, each eventually holding an empty list — tens
    # of thousands of dead keys after weeks. We evict slugs whose most
    # recent sample is older than 2h (not seen in this scan AND no longer
    # in the trailing window).
    stale = [s for s, hist in _price_history.items() if not hist or (now - hist[-1][0]) > 7200]
    for s in stale:
        _price_history.pop(s, None)

    _rapid_moves_cache = rapid_moves
    return rapid_moves


# ══════════════════════════════════════════════════════════════
# FEATURE 3: MARKET MAKER DETECTION
# ══════════════════════════════════════════════════════════════

def _is_market_maker(wallet_stats: dict) -> bool:
    """Detect if a wallet is a market maker (buys AND sells roughly equally)."""
    buys = wallet_stats.get("buys", 0)
    sells = wallet_stats.get("sells", 0)
    total = buys + sells
    if total < 15:  # Need enough data to confirm MM behavior (was 5 — too aggressive)
        return False
    buy_ratio = buys / total
    # Market makers buy and sell roughly equally (35-65% split)
    return 0.35 <= buy_ratio <= 0.65


# ══════════════════════════════════════════════════════════════
# FEATURE 4: CORRELATED MARKET ARBITRAGE
# ══════════════════════════════════════════════════════════════

_CORRELATED_GROUPS = {
    "iran_invasion": ["enter iran", "invade iran", "forces enter iran", "boots on the ground iran"],
    "iran_ceasefire": ["iran ceasefire", "ceasefire iran"],
    "fed_rates": ["fed cut", "fed rate", "interest rate", "fed decrease"],
    "btc_price": ["bitcoin above", "btc above", "bitcoin reach", "bitcoin price"],
    "recession": ["recession", "gdp decline"],
    "trump": ["trump win", "trump 2028", "trump second term"],
}


def _detect_correlated_arbs(markets: list) -> list:
    """Find logically inconsistent pricing between related markets."""
    global _correlated_arbs_cache
    groups: dict[str, list] = {}

    for m in markets:
        question = m.get("question", "").lower()
        for group_name, keywords in _CORRELATED_GROUPS.items():
            if any(kw in question for kw in keywords):
                if group_name not in groups:
                    groups[group_name] = []
                groups[group_name].append(m)

    arbs = []
    for group_name, group_markets in groups.items():
        if len(group_markets) >= 2:
            prices = []
            for gm in group_markets:
                q = gm.get("question", "")
                p = get_yes_price(gm)
                if p > 0:
                    prices.append((q, p, gm))

            if len(prices) < 2:
                continue

            # Filter out near-resolved markets (price > 95% or < 5%) — likely expired deadlines
            prices = [(q, p, gm) for q, p, gm in prices if 0.05 < p < 0.95]
            if len(prices) < 2:
                continue

            prices.sort(key=lambda x: x[1], reverse=True)

            # If the highest and lowest price in a group differ by 30%+
            if prices[0][1] - prices[-1][1] >= 0.30:
                arbs.append({
                    "group": group_name,
                    "markets": [{
                        "question": q[:60],
                        "price": round(p * 100, 1),
                        "slug": gm.get("slug", ""),
                        "market_url": _market_url(gm.get("slug", "")),
                    } for q, p, gm in prices],
                    "spread": round((prices[0][1] - prices[-1][1]) * 100, 1),
                })

    _correlated_arbs_cache = arbs
    return arbs


# ══════════════════════════════════════════════════════════════
# FEATURE 6: OUTCOME GROUP OVERROUND ARBITRAGE
# ══════════════════════════════════════════════════════════════

def _detect_overround_arbs(markets: list) -> list:
    """Find outcome groups where individual YES prices sum to > 110%.

    Example: "Who will win the Masters?" has 30 player markets. If their YES
    prices sum to 135%, selling all of them guarantees 35% profit.
    More practically: find the most overpriced outcome and bet NO on it.
    """
    global _overround_arbs_cache

    # Group markets by common prefix (first 4+ words, excluding noise)
    groups: dict[str, list] = {}
    for m in markets:
        q = m.get("question", "")
        if _is_noise(q):
            continue
        # Extract group key: "Will X win" patterns
        for prefix in ["Will ", "will "]:
            if q.startswith(prefix):
                # Take the verb phrase as group: "Will X win the 2026 Masters"
                # Group key = everything after the subject
                parts = q.split()
                if len(parts) >= 5:
                    # Use last meaningful part as group: "win the 2026 Masters tournament"
                    group_key = " ".join(parts[3:7]).lower().strip("?")
                    if len(group_key) > 10:
                        groups.setdefault(group_key, []).append(m)
                break

    arbs = []
    for group_key, group_markets in groups.items():
        if len(group_markets) < 3:
            continue

        # Sum YES prices
        total_yes = 0
        market_prices = []
        for gm in group_markets:
            p = get_yes_price(gm)
            if p > 0.01:
                total_yes += p
                market_prices.append((gm, p))

        overround = total_yes - 1.0  # How much over 100%
        if overround > 0.10 and len(market_prices) >= 3:  # 10%+ overround
            # Find the most overpriced (highest YES price relative to group share)
            market_prices.sort(key=lambda x: x[1], reverse=True)
            most_overpriced = market_prices[0]
            gm, price = most_overpriced

            arbs.append({
                "group": group_key[:50],
                "overround_pct": round(overround * 100, 1),
                "market_count": len(market_prices),
                "total_yes_pct": round(total_yes * 100, 1),
                "best_no_bet": {
                    "market": gm.get("question", "")[:80],
                    "slug": gm.get("slug", ""),
                    "yes_price": round(price * 100, 1),
                    "no_price": round((1 - price) * 100, 1),
                    "market_url": _market_url(gm.get("slug", "")),
                },
                "score": min(80, 30 + overround * 100),  # Higher overround = higher score
                "timestamp": time.time(),
            })

    arbs.sort(key=lambda a: a["overround_pct"], reverse=True)
    _overround_arbs_cache = arbs[:10]
    return _overround_arbs_cache


# ══════════════════════════════════════════════════════════════
# FEATURE 7: MOMENTUM / MEAN REVERSION SIGNALS
# ══════════════════════════════════════════════════════════════

def _detect_momentum_signals(markets: list) -> list:
    """Detect mean-reversion opportunities from price history.

    Markets that drop 15%+ in 2 hours then stabilize often revert.
    This catches overreactions to news that gets corrected.
    """
    global _momentum_signals_cache
    signals = []
    now = time.time()

    for market in markets:
        slug = market.get("slug", "")
        if not slug or slug not in _price_history:
            continue
        if _is_noise(market.get("question", "")):
            continue

        history = _price_history[slug]
        if len(history) < 4:
            continue

        current_price = get_yes_price(market)
        if current_price <= 0.10 or current_price >= 0.90:
            continue

        # Find max price in last 2 hours
        two_hours_ago = now - 7200
        recent = [(t, p) for t, p in history if t > two_hours_ago]
        if len(recent) < 3:
            continue

        max_price = max(p for _, p in recent)
        min_price = min(p for _, p in recent)

        # Mean reversion: big drop then stabilizing (current near recent low)
        drop = max_price - current_price
        if drop >= 0.15 and current_price <= min_price + 0.03:
            # Price dropped 15%+ and is near the bottom — potential reversion
            expected_revert = (max_price + current_price) / 2  # Revert to midpoint
            edge = expected_revert - current_price

            if edge >= 0.08:  # 8%+ expected reversion
                signals.append({
                    "type": "mean_reversion",
                    "market": market.get("question", "")[:80],
                    "slug": slug,
                    "market_url": _market_url(slug),
                    "current_price": round(current_price * 100, 1),
                    "recent_high": round(max_price * 100, 1),
                    "drop_pct": round(drop * 100, 1),
                    "expected_revert": round(expected_revert * 100, 1),
                    "edge": round(edge * 100, 1),
                    "score": min(70, 30 + drop * 100),
                    "side": "YES",  # Buy the dip
                    "timestamp": now,
                })

        # Inverse: big spike then stabilizing (potential short)
        spike = current_price - min_price
        if spike >= 0.15 and current_price >= max_price - 0.03:
            expected_revert = (min_price + current_price) / 2
            edge = current_price - expected_revert

            if edge >= 0.08:
                signals.append({
                    "type": "mean_reversion",
                    "market": market.get("question", "")[:80],
                    "slug": slug,
                    "market_url": _market_url(slug),
                    "current_price": round(current_price * 100, 1),
                    "recent_low": round(min_price * 100, 1),
                    "spike_pct": round(spike * 100, 1),
                    "expected_revert": round(expected_revert * 100, 1),
                    "edge": round(edge * 100, 1),
                    "score": min(70, 30 + spike * 100),
                    "side": "NO",  # Sell the spike
                    "timestamp": now,
                })

    signals.sort(key=lambda s: s["edge"], reverse=True)
    _momentum_signals_cache = signals[:10]
    return _momentum_signals_cache


# ══════════════════════════════════════════════════════════════
# SIGNAL 16: PANIC FADE
# ══════════════════════════════════════════════════════════════

def _detect_panic_fade(markets: list) -> list:
    """Signal #16: Buy markets that crashed 8%+ in 30min and are now under 30c.

    Adapted from prediction-market-backtesting panic_fade strategy (backtested on
    real Polymarket tick data). Panic selling into pennies creates overreaction —
    the market hasn't resolved yet and buyers are fleeing irrationally.

    Complements Feature 7 (which needs 15%+ drops). This catches smaller 8-14%
    panics in low-priced markets where volatility is proportionally larger.
    """
    global _panic_fade_cache
    signals = []
    now = time.time()

    for market in markets:
        slug = market.get("slug", "")
        if not slug or slug not in _price_history:
            continue
        if _is_noise(market.get("question", "")):
            continue

        current_price = get_yes_price(market)
        # Only markets 10-30c — panic is proportionally large here.
        # 10c floor matches simulator MIN_ENTRY_PRICE so signals aren't silently discarded.
        if current_price < 0.10 or current_price > 0.30:
            continue

        # Get recent 30-minute price window
        history = _price_history[slug]
        recent = [(t, p) for t, p in history if now - t <= 1800]
        if len(recent) < 3:
            continue

        peak = max(p for _, p in recent)
        drop = peak - current_price
        # 8-14% drop: large enough to be a panic, small enough that Feature 7 missed it
        if drop < 0.08 or drop >= 0.15:
            continue

        volume = float(market.get("volume24hr", 0) or 0)
        if volume < 5000:
            continue

        # Rebound target: 60% mean-reversion toward the peak
        rebound_target = min(0.45, current_price + drop * 0.6)
        edge = rebound_target - current_price

        signals.append({
            "type": "panic_fade",
            "market": market.get("question", "")[:80],
            "slug": slug,
            "market_url": _market_url(slug),
            "side": "YES",
            "current_price": round(current_price * 100, 1),
            "peak_price": round(peak * 100, 1),
            "drop_pct": round(drop * 100, 1),
            "rebound_target": round(rebound_target * 100, 1),
            "edge": round(edge * 100, 1),
            "score": min(75, 35 + drop * 150),
            "volume_24h": round(volume, 0),
            "timestamp": now,
        })

    signals.sort(key=lambda s: s["drop_pct"], reverse=True)
    _panic_fade_cache = signals[:10]
    return _panic_fade_cache


# ══════════════════════════════════════════════════════════════
# SIGNAL 18: RESULT REVERSAL LOTTERY
# ══════════════════════════════════════════════════════════════

# Keywords identifying specific match/fight result markets (NOT season-level)
_SPORTS_REVERSAL_KEYWORDS = [
    "ufc", "mma", "boxing", "fight", "bout",
    " vs ", "vs.", "vs ",
    "tennis", "open:", "atp", "wta",
    "f1", "formula 1", "grand prix", "nascar",
    "nba game", "nfl game", "mlb game", "nhl game",
    "lol:", "dota:", "csgo:", "valorant:", "cs2:",
    "esports", "esport",
]

def _detect_result_reversals(markets: list) -> list:
    """Signal #18: Sports markets that just crashed to near-zero (≤5¢) but
    are still open on Polymarket — a real-money bet that the official result
    gets overturned (DQ, referee reversal, VAR, UFC overturn, etc.).

    The edge: Polymarket resolves on the OFFICIAL final result, not the
    initial announcement. When UFC overturns a decision 30 min later,
    a 1¢ position becomes $1.00. Base reversal probability ~4% across sports.

    Entry criteria:
    - Sports match-level market (contains "vs" or sport-specific keyword)
    - YES or NO side priced at 1-5¢ (was ≥40¢ in recent scan history)
    - Market still open and unresolved
    - Crash occurred within last 2 hours (Polymarket scan window)
    - EV ≥ 10% at assumed 4% reversal probability
    """
    global _result_reversals_cache
    signals = []
    now = time.time()

    REVERSAL_PROB = 0.04   # 4% base probability an official result gets overturned
    MAX_BUY_PRICE = 0.05   # Only buy at 5¢ or below
    MIN_CRASH_FROM = 0.40  # Must have crashed from at least 40¢ (was a live favourite)
    MIN_EV = 0.10          # Minimum 10% EV to signal
    MAX_CRASH_AGE = 7200   # Within 2-hour price history window

    for market in markets:
        question = market.get("question", "")
        slug = market.get("slug", "")
        if not slug or not question:
            continue

        # Must be open/active (not yet resolved by Polymarket)
        if market.get("closed") or market.get("archived"):
            continue

        # Skip noise (crypto price, map/game sub-markets, temperature, etc.)
        if _is_noise(question):
            continue

        # Must look like a specific sports match result market
        q_lower = question.lower()
        if not any(kw in q_lower for kw in _SPORTS_REVERSAL_KEYWORDS):
            continue

        # Need price history to detect the crash
        if slug not in _price_history or len(_price_history[slug]) < 2:
            continue

        current_yes = get_yes_price(market)
        if current_yes <= 0.0:
            continue

        # Check YES side (crashed to near-zero) and NO side (YES shot up = NO crashed)
        for side, buy_price in [
            ("YES", current_yes),
            ("NO", round(1.0 - current_yes, 4)),
        ]:
            if buy_price > MAX_BUY_PRICE or buy_price < 0.005:
                continue

            # Scan history for a prior high price (the other side was dominant)
            history = _price_history[slug]
            crash_from = 0.0
            crash_age_min = 999.0

            for ts, hist_yes in history:
                if now - ts > MAX_CRASH_AGE:
                    continue
                # For YES side: look for historical YES high that has now collapsed
                hist_side_price = hist_yes if side == "YES" else (1.0 - hist_yes)
                if hist_side_price >= MIN_CRASH_FROM:
                    crash_from = hist_side_price
                    crash_age_min = (now - ts) / 60.0
                    break  # Take the most recent historical high

            if crash_from < MIN_CRASH_FROM:
                continue  # No qualifying crash found

            # EV = REVERSAL_PROB × (payout - 1) - (1 - REVERSAL_PROB)
            ev = REVERSAL_PROB * (1.0 / buy_price - 1.0) - (1.0 - REVERSAL_PROB)
            if ev < MIN_EV:
                continue

            # Score: base on EV, boosted for fresher crashes (reversal decisions come fast)
            score = min(72, 30 + ev * 25)
            if crash_age_min < 20:
                score += 20   # Very fresh — reversal still being deliberated
            elif crash_age_min < 45:
                score += 15
            elif crash_age_min < 90:
                score += 10
            elif crash_age_min < 120:
                score += 5

            volume = float(market.get("volume", 0) or 0)

            signals.append({
                "type": "result_reversal",
                "market": question[:80],
                "slug": slug,
                "market_url": _market_url(slug),
                "side": side,
                "price": round(buy_price * 100, 2),          # cents (display)
                "buy_price": buy_price,                        # decimal (betting)
                "ev": round(ev * 100, 1),
                "score": round(score, 1),
                "crash_from": round(crash_from * 100, 1),      # what price it was at (cents)
                "crash_age_min": round(crash_age_min, 0),
                "reversal_prob_pct": round(REVERSAL_PROB * 100, 1),
                "volume": round(volume, 0),
                "timestamp": now,
            })
            break  # Only flag one side per market

    signals.sort(key=lambda s: s["score"], reverse=True)
    _result_reversals_cache = signals[:5]

    return _result_reversals_cache


# ══════════════════════════════════════════════════════════════
# FEATURE 8: BREAKING NEWS SPEED EDGE
# ══════════════════════════════════════════════════════════════

async def _tavily_search_news(query: str) -> list[str]:
    """Search Tavily for recent news articles. Returns list of content snippets.

    Optional — only runs when TAVILY_API_KEY is set. Free tier: 1000/month.
    Used to verify and score news speed signals with full article context.
    """
    from app.config import settings
    if not settings.TAVILY_API_KEY:
        return []
    try:
        loop = asyncio.get_running_loop()
        resp = await loop.run_in_executor(None, lambda: requests.post(
            "https://api.tavily.com/search",
            json={
                "api_key": settings.TAVILY_API_KEY,
                "query": query,
                "search_depth": "basic",
                "max_results": 5,
                "days": 3,
            },
            timeout=8,
        ))
        data = resp.json()
        return [
            r.get("content", "") or r.get("snippet", "")
            for r in data.get("results", [])
            if r.get("content") or r.get("snippet")
        ]
    except Exception as e:
        logger.debug(f"Tavily search failed: {e}")
        return []


async def _detect_news_speed_signals(markets: list) -> list:
    """Find markets where Polymarket moved fast but Manifold hasn't caught up.

    When news breaks, Polymarket reacts in minutes. Manifold/Kalshi lag 30-60 min.
    If we detect a recent rapid move AND Manifold still shows old price, that's a
    time-limited arbitrage window.
    """
    global _news_speed_signals_cache
    signals = []

    for rm in _rapid_moves_cache:
        slug = rm.get("slug", "")
        if not slug or rm.get("move_pct", 0) < 8:
            continue  # Only care about 8%+ moves

        question = rm.get("market", "")
        if _is_noise(question):
            continue

        # Search Manifold for this market
        keywords = _extract_keywords(question)
        if not keywords:
            continue

        manifold_results = await _search_manifold(" ".join(keywords))
        if not manifold_results:
            continue

        match = _match_markets(question, manifold_results)
        if not match:
            continue

        manifold_prob = match.get("probability")
        if manifold_prob is None:
            continue

        # Current Polymarket price after the rapid move
        poly_price = rm.get("new_price", 0) / 100.0
        manifold_price = manifold_prob

        # Check if Manifold is stale (hasn't caught up to Poly's move)
        speed_edge = abs(poly_price - manifold_price)
        if speed_edge < 0.10:  # Need 10%+ lag
            continue

        # Determine direction: trust Polymarket (it moved, Manifold is stale)
        if rm["direction"] == "UP":
            side = "YES"
        else:
            side = "NO"

        # Optional: Tavily news confirmation — boosts score if recent articles support the move
        base_score = min(80, 40 + speed_edge * 100)
        tavily_snippets = await _tavily_search_news(" ".join(keywords[:4]))
        tavily_confirmed = False
        if tavily_snippets:
            # Check if any snippet mentions keywords from the market question
            combined = " ".join(tavily_snippets).lower()
            keyword_hits = sum(1 for kw in keywords[:6] if kw.lower() in combined)
            if keyword_hits >= 2:
                tavily_confirmed = True
                base_score = min(90, base_score + 10)  # +10 for news confirmation

        signals.append({
            "type": "news_speed",
            "market": question[:80],
            "slug": slug,
            "market_url": _market_url(slug),
            "poly_price": rm.get("new_price", 0),
            "manifold_price": round(manifold_prob * 100, 1),
            "speed_edge": round(speed_edge * 100, 1),
            "rapid_move": rm.get("move_pct", 0),
            "direction": rm["direction"],
            "side": side,
            "tavily_confirmed": tavily_confirmed,
            "score": base_score,
            "timestamp": time.time(),
        })

    signals.sort(key=lambda s: s["speed_edge"], reverse=True)
    _news_speed_signals_cache = signals[:10]
    return _news_speed_signals_cache


# ══════════════════════════════════════════════════════════════
# FEATURE 9: YES+NO ARBITRAGE (GUARANTEED PROFIT)
# ══════════════════════════════════════════════════════════════

async def _detect_yesno_arbs(markets: list) -> list:
    """Find markets where YES price + NO price < $1.00.

    Academic research (IMDEA 2025) documented $39.59M in profits from this.
    If you can buy YES at 40c and NO at 55c, you spend 95c and are guaranteed
    $1.00 on resolution = 5c profit per share regardless of outcome.

    Uses the CLOB API for exact bid/ask prices (more precise than Gamma API).
    """
    global _yesno_arb_cache
    arbs = []
    loop = asyncio.get_running_loop()

    for market in markets:
        slug = market.get("slug", "")
        question = market.get("question", "")
        if not slug or _is_noise(question):
            continue

        # Get token IDs for YES and NO outcomes
        try:
            tokens_str = market.get("clobTokenIds", "[]")
            tokens = json.loads(tokens_str) if isinstance(tokens_str, str) else tokens_str
            if not tokens or len(tokens) < 2:
                continue
            yes_token = tokens[0]
            no_token = tokens[1]
        except (json.JSONDecodeError, IndexError):
            continue

        # Fetch best ask for YES and NO from CLOB
        try:
            # Fetch YES ask price
            resp_yes = await loop.run_in_executor(
                None,
                lambda yt=yes_token: requests.get(
                    f"{POLYMARKET_CLOB_API}/price",
                    params={"token_id": yt, "side": "BUY"},
                    timeout=8,
                ),
            )
            # Fetch NO ask price
            resp_no = await loop.run_in_executor(
                None,
                lambda nt=no_token: requests.get(
                    f"{POLYMARKET_CLOB_API}/price",
                    params={"token_id": nt, "side": "BUY"},
                    timeout=8,
                ),
            )
            if resp_yes.status_code != 200 or resp_no.status_code != 200:
                continue

            # /price endpoint returns {"price": "0.45"} format
            yes_data = resp_yes.json()
            no_data = resp_no.json()
            yes_ask = float(yes_data.get("price", 1.0) or 1.0)
            no_ask = float(no_data.get("price", 1.0) or 1.0)

            if yes_ask >= 1.0 or no_ask >= 1.0 or yes_ask <= 0 or no_ask <= 0:
                continue  # Invalid response
        except Exception as e:
            logger.debug(f"YES+NO arb price fetch failed: {e}")
            continue

        combined = yes_ask + no_ask
        if combined >= 0.98:  # Need at least 2% profit margin
            continue

        profit_pct = round((1.0 - combined) * 100, 1)
        arbs.append({
            "type": "yesno_arb",
            "market": question[:80],
            "slug": slug,
            "market_url": _market_url(slug),
            "yes_ask": round(yes_ask * 100, 1),
            "no_ask": round(no_ask * 100, 1),
            "combined": round(combined * 100, 1),
            "profit_pct": profit_pct,
            "score": min(90, 50 + profit_pct * 5),  # Higher margin = higher score
            "timestamp": time.time(),
        })

        await asyncio.sleep(0.2)  # Rate limit

    arbs.sort(key=lambda a: a["profit_pct"], reverse=True)
    _yesno_arb_cache = arbs[:10]

    if arbs:
        logger.info(f"YES+NO arb: found {len(arbs)} opportunities (best: {arbs[0]['profit_pct']}% profit)")

    return _yesno_arb_cache


# ══════════════════════════════════════════════════════════════
# FEATURE 10: CLOB ORDERBOOK DEPTH ANALYSIS
# ══════════════════════════════════════════════════════════════

async def compute_walls(token_id: str) -> dict:
    """v3.20.0b — Polymonit-style orderbook wall analysis.

    Fetches the L2 orderbook for a CLOB token and returns:
      - top_walls: top 5 visible orders by USD value (price * size), each
        tagged side ('bid' or 'ask') and pct_of_depth
      - bid_depth_usd / ask_depth_usd: total $ resting on each side
      - imbalance_pct: ((bid - ask) / total) * 100 — positive = buy pressure,
        negative = sell pressure. Capped at ±100.
      - top5_pct_of_depth: how concentrated the book is in the 5 biggest walls
      - spread_cents: ask - bid in cents

    Returns empty dict on any failure (graceful — never raises into caller).
    Cheap (one CLOB call) so safe to call per-market on demand.
    """
    if not token_id:
        return {}
    loop = asyncio.get_running_loop()
    try:
        resp = await loop.run_in_executor(
            None,
            lambda: requests.get(
                f"{POLYMARKET_CLOB_API}/book",
                params={"token_id": token_id},
                timeout=8,
            ),
        )
        if resp.status_code != 200:
            return {}
        book = resp.json() or {}
    except Exception as e:
        logger.debug("compute_walls fetch failed for %s: %s", token_id, e)
        return {}

    bids = book.get("bids", []) or []
    asks = book.get("asks", []) or []

    def _norm(side, level):
        try:
            price = float(level.get("price", 0))
            size = float(level.get("size", 0))
            usd = price * size
            return {"side": side, "price": price, "size": size, "usd": usd}
        except Exception:
            return None

    bid_levels = [x for x in (_norm("bid", b) for b in bids) if x and x["usd"] > 0]
    ask_levels = [x for x in (_norm("ask", a) for a in asks) if x and x["usd"] > 0]

    bid_depth = sum(x["usd"] for x in bid_levels)
    ask_depth = sum(x["usd"] for x in ask_levels)
    total_depth = bid_depth + ask_depth
    if total_depth <= 0:
        return {}

    # Top 5 walls across both sides by USD value
    all_walls = sorted(bid_levels + ask_levels, key=lambda x: -x["usd"])[:5]
    top5_usd = sum(w["usd"] for w in all_walls)
    for w in all_walls:
        w["pct_of_depth"] = round((w["usd"] / total_depth * 100), 1) if total_depth > 0 else 0

    # Spread: best bid vs best ask
    best_bid = max((x["price"] for x in bid_levels), default=0)
    best_ask = min((x["price"] for x in ask_levels), default=0)
    spread_cents = round((best_ask - best_bid) * 100, 2) if (best_bid > 0 and best_ask > 0) else None

    imbalance_pct = round(((bid_depth - ask_depth) / total_depth * 100), 1)

    return {
        "token_id": token_id,
        "top_walls": all_walls,
        "bid_depth_usd": round(bid_depth, 2),
        "ask_depth_usd": round(ask_depth, 2),
        "total_depth_usd": round(total_depth, 2),
        "imbalance_pct": imbalance_pct,  # +100 = all bids, -100 = all asks
        "top5_pct_of_depth": round((top5_usd / total_depth * 100), 1),
        "spread_cents": spread_cents,
        "best_bid": best_bid,
        "best_ask": best_ask,
    }


def summarize_walls(walls: dict) -> str:
    """One-line natural-language summary suitable for Telegram + alert cards.
    Empty string if walls dict is empty OR top_walls is missing/empty —
    without this second guard the `Book:` line in Telegram entry alerts
    would read "Wall: $0 bid-side support @ 0c" on thin markets where
    compute_walls returned a valid depth dict but zero qualifying walls."""
    if not walls:
        return ""
    top_walls = walls.get("top_walls") or []
    if not top_walls:
        return ""
    imb = walls.get("imbalance_pct", 0)
    top5 = walls.get("top5_pct_of_depth", 0)
    biggest = top_walls[0]
    biggest_side = biggest.get("side", "?")
    biggest_usd = biggest.get("usd", 0)
    biggest_price = biggest.get("price", 0)
    side_label = "sell-side resistance" if biggest_side == "ask" else "bid-side support"
    return (f"Wall: ${biggest_usd:,.0f} {side_label} @ {biggest_price*100:.0f}c · "
            f"top 5 = {top5:.0f}% of depth · imbalance {imb:+.0f}%")


async def _detect_orderbook_signals(markets: list) -> list:
    """Detect buy/sell imbalances from the CLOB orderbook.

    When bid depth is 10x ask depth, big money is accumulating → price will rise.
    When ask depth is 10x bid depth, smart money is dumping → price will fall.
    """
    global _orderbook_signals_cache
    signals = []
    loop = asyncio.get_running_loop()

    # Only check top markets by volume (orderbook calls are expensive)
    for market in markets[:20]:
        slug = market.get("slug", "")
        question = market.get("question", "")
        if not slug or _is_noise(question):
            continue

        try:
            tokens_str = market.get("clobTokenIds", "[]")
            tokens = json.loads(tokens_str) if isinstance(tokens_str, str) else tokens_str
            if not tokens:
                continue
            yes_token = tokens[0]
        except (json.JSONDecodeError, IndexError):
            continue

        # Fetch orderbook for YES token
        try:
            resp = await loop.run_in_executor(
                None,
                lambda yt=yes_token: requests.get(
                    f"{POLYMARKET_CLOB_API}/book",
                    params={"token_id": yt},
                    timeout=8,
                ),
            )
            if resp.status_code != 200:
                continue
            book = resp.json()
        except Exception:
            continue

        # Sum bid and ask depth (total $ on each side)
        bids = book.get("bids", [])
        asks = book.get("asks", [])
        bid_depth = sum(float(b.get("size", 0)) * float(b.get("price", 0)) for b in bids)
        ask_depth = sum(float(a.get("size", 0)) * float(a.get("price", 0)) for a in asks)

        if bid_depth < 100 and ask_depth < 100:
            continue  # Too thin, skip

        # Calculate imbalance ratio
        total = bid_depth + ask_depth
        if total < 200:
            continue

        bid_ratio = bid_depth / total
        current_price = get_yes_price(market)

        if bid_ratio >= 0.75 and current_price < 0.70:
            # Bids dominate → accumulation → buy YES
            signals.append({
                "type": "orderbook_buy",
                "market": question[:80],
                "slug": slug,
                "market_url": _market_url(slug),
                "bid_depth": round(bid_depth, 0),
                "ask_depth": round(ask_depth, 0),
                "bid_ratio": round(bid_ratio * 100, 1),
                "current_price": round(current_price * 100, 1),
                "side": "YES",
                "score": min(70, 30 + (bid_ratio - 0.5) * 100),
                "timestamp": time.time(),
            })
        elif bid_ratio <= 0.25 and current_price > 0.30:
            # Asks dominate → distribution → buy NO
            signals.append({
                "type": "orderbook_sell",
                "market": question[:80],
                "slug": slug,
                "market_url": _market_url(slug),
                "bid_depth": round(bid_depth, 0),
                "ask_depth": round(ask_depth, 0),
                "bid_ratio": round(bid_ratio * 100, 1),
                "current_price": round(current_price * 100, 1),
                "side": "NO",
                "score": min(70, 30 + (0.5 - bid_ratio) * 100),
                "timestamp": time.time(),
            })

        await asyncio.sleep(0.3)  # Rate limit CLOB API

    signals.sort(key=lambda s: s["score"], reverse=True)
    _orderbook_signals_cache = signals[:10]
    return _orderbook_signals_cache


# ══════════════════════════════════════════════════════════════
# FEATURE 11: LONGSHOT BIAS EXPLOITATION
# ══════════════════════════════════════════════════════════════

def _detect_longshot_signals(markets: list) -> list:
    """Exploit the favorite-longshot bias: cheap contracts are overpriced.

    Academic research (Becker 2025, Snowberg & Wolfers):
    - Contracts at 1c are overpriced by ~57% (real prob ~0.43%)
    - Contracts at 5c are overpriced by ~16% (real prob ~4.18%)
    - Contracts at 10c are overpriced by ~10%

    Strategy: Bet NO on markets where YES is priced below 12c.
    The YES buyers are paying for lottery tickets; we sell them.
    Only bet on markets with enough volume (not illiquid garbage).

    Exclusion: weather/temperature markets are handled by weather_engine.py,
    which has real forecast alpha. Shorting cheap temperature buckets here
    would cancel out our own weather_engine longs on the same market.
    """
    global _longshot_signals_cache
    signals = []

    for market in markets:
        question = market.get("question", "")
        slug = market.get("slug", "")
        if not slug or _is_noise(question):
            continue
        # Skip weather markets — weather_engine owns this space with forecast edge
        ql = question.lower()
        if "temperature" in ql or "highest temp" in ql or "lowest temp" in ql:
            continue

        yes_price = get_yes_price(market)
        volume = float(market.get("volume24hr", 0) or 0)

        # Target: YES price between 2-12% (the overpriced zone)
        # Requires decent volume ($25K+) so it's not illiquid junk
        if 0.02 <= yes_price <= 0.12 and volume >= 25000:
            no_price = 1.0 - yes_price
            # Theoretical edge based on academic data
            if yes_price <= 0.03:
                edge_pct = 40.0  # ~57% overpriced at 1-3c
            elif yes_price <= 0.06:
                edge_pct = 15.0  # ~16% overpriced at 4-6c
            elif yes_price <= 0.12:
                edge_pct = 8.0   # ~10% overpriced at 7-12c
            else:
                continue

            signals.append({
                "type": "longshot_bias",
                "market": question[:80],
                "slug": slug,
                "market_url": _market_url(slug),
                "yes_price": round(yes_price * 100, 1),
                "no_price": round(no_price * 100, 1),
                "edge_pct": edge_pct,
                "volume_24h": round(volume, 0),
                "side": "NO",  # Always bet NO (sell the overpriced YES)
                "score": min(75, 35 + edge_pct),
                "timestamp": time.time(),
            })

    signals.sort(key=lambda s: s["edge_pct"], reverse=True)
    _longshot_signals_cache = signals[:10]
    return _longshot_signals_cache


# ══════════════════════════════════════════════════════════════
# FEATURE 12: SETTLEMENT HARVESTER
# Near-certain markets where outcome is effectively determined.
# Buy the winning side at 95-99c for small guaranteed profit.
# ══════════════════════════════════════════════════════════════

async def _detect_settled_bets(markets: list) -> list:
    """Find markets where the outcome is effectively determined but not yet formally resolved.

    Strategy: buy the winning side at 95-99c and collect 1-5% when it settles to $1.00.
    Risk is near-zero when ALL layers pass:
      1. Extreme price (one side >= 95%)
      2. Deadline expired or event outcome publicly known
      3. Cross-platform consensus (Manifold/Kalshi also show extreme price)
      4. No whale buying the cheap side (contrarian warning)
      5. AI verifies outcome is determined
      6. Objective resolution criteria (not subjective)
    """
    global _settled_bets_cache, _settled_bets_last_scan

    await ws_manager.send_log("[HARVEST] Scanning for settled bet opportunities...", "info")

    candidates = []
    for market in markets:
        question = market.get("question", "")
        slug = market.get("slug", "")
        if not slug or _is_noise(question):
            continue

        yes_price = get_yes_price(market)
        volume = float(market.get("volume24hr", 0) or 0)

        # Layer 1: Extreme price — one side must be >= 95%
        if yes_price < 0.95 and yes_price > 0.05:
            continue

        # Need minimum volume ($10K) — no dead markets
        if volume < 10000:
            continue

        # Determine winning side and profit
        if yes_price >= 0.95:
            winning_side = "YES"
            buy_price = yes_price
            profit_per_dollar = 1.0 - yes_price
        else:
            winning_side = "NO"
            buy_price = 1.0 - yes_price
            profit_per_dollar = yes_price  # NO pays $1 when YES is worth 0

        profit_pct = round(profit_per_dollar * 100, 2)
        profit_on_10 = round(profit_per_dollar * (10.0 / buy_price), 2)

        # Skip if profit is too tiny (< 0.3%) or too large (> 8%, might be risky)
        if profit_pct < 0.3 or profit_pct > 8.0:
            continue

        # Layer 2: Check deadline — prefer expired or expiring within 48h
        days = _days_to_resolution(market)
        deadline_passed = days is not None and days <= 0
        deadline_imminent = days is not None and 0 < days <= 2
        # Markets with no deadline can still qualify if price is extreme enough
        no_deadline = days is None

        # Layer 4: Check for whale contrarian activity (buying the cheap side)
        contrarian_whale = False
        cheap_side = "NO" if winning_side == "YES" else "YES"
        for addr, ws in _wallet_scores.items():
            if ws["win_rate"] >= 65 and ws["trades"] >= 20:
                for t in ws["recent_trades"]:
                    t_slug = t.get("slug", "")
                    t_title = t.get("title", "")
                    if t_slug == slug or (question[:30].lower() in t_title.lower()):
                        # This wallet traded this market — check if buying cheap side
                        if t.get("side") == "BUY" and cheap_side == "YES":
                            contrarian_whale = True
                        elif t.get("side") == "SELL" and cheap_side == "NO":
                            contrarian_whale = True
                        break

        # If a smart whale is betting against the obvious outcome, skip
        if contrarian_whale:
            continue

        candidates.append({
            "market": market,
            "question": question,
            "slug": slug,
            "winning_side": winning_side,
            "buy_price": buy_price,
            "profit_pct": profit_pct,
            "profit_on_10": profit_on_10,
            "volume": volume,
            "days_to_resolution": days,
            "deadline_passed": deadline_passed,
            "deadline_imminent": deadline_imminent,
            "no_deadline": no_deadline,
        })

    if not candidates:
        _settled_bets_cache = []
        _settled_bets_last_scan = time.time()
        return []

    # Layer 3: Cross-platform consensus check (batch)
    # Fetch Manifold data for each candidate
    verified = []
    for cand in candidates[:20]:  # Limit API calls
        question = cand["question"]
        keywords = _extract_keywords(question)
        if not keywords:
            continue

        manifold_confirms = False
        manifold_prob = None
        search_term = " ".join(keywords)
        manifold_results = await _search_manifold(search_term)
        if manifold_results:
            match = _match_markets(question, manifold_results)
            if match:
                mp = match.get("probability")
                if mp is not None:
                    manifold_prob = mp
                    # Manifold must also show extreme price in same direction
                    if cand["winning_side"] == "YES" and mp >= 0.90:
                        manifold_confirms = True
                    elif cand["winning_side"] == "NO" and mp <= 0.10:
                        manifold_confirms = True

        # Layer 6: Check resolution criteria objectivity
        resolution_type = _estimate_resolution_type(question)
        has_clear_deadline = any(w in question.lower() for w in [
            "by ", "before ", "in 2026", "in 2025", "by april", "by may",
            "by june", "by july", "by august", "by september", "by october",
            "by november", "by december", "by january", "by february", "by march",
        ])
        # Subjective markets are risky — lower confidence
        subjective_markers = ["considered", "deemed", "successful", "good", "bad",
                              "significant", "major", "effective", "approval rating"]
        is_subjective = any(m in question.lower() for m in subjective_markers)

        # Calculate confidence layers
        layers_passed = 1  # Layer 1 (extreme price) always passes to get here
        if cand["deadline_passed"]:
            layers_passed += 1  # Layer 2: deadline expired
        elif cand["deadline_imminent"]:
            layers_passed += 0.5
        if manifold_confirms:
            layers_passed += 1  # Layer 3: cross-platform
        # Layer 4 (no contrarian whale) already filtered above = +1
        layers_passed += 1
        if not is_subjective:
            layers_passed += 1  # Layer 6: objective criteria
        if has_clear_deadline:
            layers_passed += 0.5

        # Minimum 3.5 layers to qualify (price + no whale + at least one more)
        if layers_passed < 3.5:
            continue

        cand["manifold_confirms"] = manifold_confirms
        cand["manifold_prob"] = round(manifold_prob * 100, 1) if manifold_prob is not None else None
        cand["is_subjective"] = is_subjective
        cand["has_clear_deadline"] = has_clear_deadline
        cand["layers_passed"] = layers_passed
        cand["resolution_type"] = resolution_type["type"]
        verified.append(cand)

        await asyncio.sleep(0.3)

    # Layer 5: AI verification on top candidates
    ai_verified = []
    for cand in verified[:10]:
        ai_reasoning = await _ai_verify_settlement(
            cand["question"], cand["winning_side"], cand["buy_price"],
            cand["deadline_passed"], cand["manifold_confirms"],
        )
        if ai_reasoning is None:
            # AI unavailable — still include but note it
            cand["ai_verified"] = False
            cand["ai_reasoning"] = "AI verification unavailable — proceed with caution"
            ai_verified.append(cand)
        elif ai_reasoning.startswith("SAFE"):
            cand["ai_verified"] = True
            cand["ai_reasoning"] = ai_reasoning[5:].strip(" :—-")
            cand["layers_passed"] += 1
            ai_verified.append(cand)
        # RISKY results are dropped

    # Build final output
    results = []
    for cand in ai_verified:
        buy_price_cents = round(cand["buy_price"] * 100, 1)
        results.append({
            "market": cand["question"][:100],
            "slug": cand["slug"],
            "market_url": _market_url(cand["slug"]),
            "winning_side": cand["winning_side"],
            "buy_price_cents": buy_price_cents,
            "profit_pct": cand["profit_pct"],
            "profit_on_10": cand["profit_on_10"],
            "volume_24h": round(cand["volume"], 0),
            "days_to_resolution": round(cand["days_to_resolution"], 2) if cand["days_to_resolution"] is not None else None,
            "deadline_passed": cand["deadline_passed"],
            "deadline_imminent": cand["deadline_imminent"],
            "manifold_confirms": cand["manifold_confirms"],
            "manifold_prob": cand["manifold_prob"],
            "ai_verified": cand["ai_verified"],
            "ai_reasoning": cand["ai_reasoning"],
            "layers_passed": cand["layers_passed"],
            "is_subjective": cand["is_subjective"],
            "resolution_type": cand["resolution_type"],
            "timestamp": time.time(),
        })

    # Sort by layers_passed (safety) then by profit_pct
    results.sort(key=lambda x: (x["layers_passed"], x["profit_pct"]), reverse=True)
    _settled_bets_cache = results[:15]
    _settled_bets_last_scan = time.time()

    if results:
        await ws_manager.send_log(
            f"[HARVEST] Found {len(results)} settlement opportunities "
            f"(best: {results[0]['winning_side']} @ {results[0]['buy_price_cents']}c "
            f"= {results[0]['profit_pct']}% profit, "
            f"{results[0]['layers_passed']:.0f} safety layers)",
            "success",
        )
    else:
        await ws_manager.send_log("[HARVEST] No settlement opportunities found this cycle", "info")

    return results


async def _ai_verify_settlement(question: str, winning_side: str, buy_price: float,
                                 deadline_passed: bool, manifold_confirms: bool) -> str | None:
    """Use Claude to verify that a near-certain market outcome is truly determined.

    Returns:
    - "SAFE: <reasoning>" if outcome is confirmed
    - "RISKY: <reasoning>" if there's doubt
    - None if AI is unavailable
    """
    if not settings.ANTHROPIC_API_KEY:
        return None

    try:
        import anthropic
        loop = asyncio.get_running_loop()

        client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)
        price_pct = round(buy_price * 100, 1)
        today = datetime.now(timezone.utc).strftime("%B %d, %Y")
        prompt = (
            f"TODAY'S DATE: {today}\n\n"
            f"Prediction market: \"{question}\"\n"
            f"Current price: {winning_side} at {price_pct}%.\n"
            f"Has the deadline/event date in the question ALREADY PASSED (based on today {today})? "
            f"{'YES — deadline has passed.' if deadline_passed else 'NO — deadline has NOT passed yet.'}\n"
            f"Other platforms agree: {'YES' if manifold_confirms else 'NO/UNKNOWN'}.\n\n"
            f"I want to buy {winning_side} at {price_pct}c to collect when it resolves to $1.00.\n"
            f"Is this outcome ALREADY DETERMINED or effectively certain?\n"
            f"IMPORTANT: Check the date in the question against today's date ({today}). "
            f"If the deadline is in the FUTURE, the outcome is NOT yet determined.\n\n"
            f"Reply SAFE or RISKY followed by a 1-sentence explanation.\n"
            f"SAFE = outcome is determined and irreversible.\n"
            f"RISKY = deadline hasn't passed, outcome could still change, or resolution is ambiguous."
        )

        resp = await loop.run_in_executor(
            None,
            lambda: client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=100,
                messages=[{"role": "user", "content": prompt}],
            ),
        )
        text = resp.content[0].text.strip() if resp.content else ""
        logger.info(f"Settlement AI check: {text[:80]} — {question[:50]}")
        return text

    except Exception as e:
        logger.debug(f"Settlement AI verification failed: {e}")
        return None


# ══════════════════════════════════════════════════════════════
# FEATURE 12: LEADERBOARD WHALE TRACKING
# ══════════════════════════════════════════════════════════════

LEADERBOARD_API = "https://data-api.polymarket.com/leaderboard"
_leaderboard_last_fetch: float = 0

# Hand-curated whales: wallets we treat as top-signal even if they don't crack
# the global leaderboard. Always merged into _leaderboard_wallets after fetch.
# Verified 2026-04-16 — each entry is the profile's proxyWallet from polymarket.com
# (HTML-scrape, address-frequency-count method: profile owner dominates at ~27x,
# counterparties appear exactly 1x per position).
MANUAL_WHALE_WALLETS: set[str] = {
    # @beefslayer — 1,541 trades, 67.7% WR, +$58K PnL, weather-range specialist
    # on NYC/Seattle/Chicago/Atlanta temperature buckets. Rank #10119 so misses
    # the top-100 leaderboard, but his deep-longshot edge is exactly what we
    # want to copy.
    "0x331bf91c132af9d921e1908ca0979363fc47193f",

    # Featured bots from polybot-arena.com (r/polymarketAnalysis recommendation).
    # Author claims: "best-performing bots spread across hundreds of small
    # positions" and "timing matters more than direction — top bots enter
    # within seconds of price dislocations." Copying them with a short lag
    # converts our 10-min scan cadence into tradeable signals.
    "0x37c94ea1b44e01b18a1ce3ab6f8002bd6b9d7e6d",  # @abrak25
    "0xd84c2b6d65dc596f49c7b6aadd6d74ca91e407b9",  # @BoneReader
    "0xe00740bce98a594e26861838885ab310ec3b548c",  # @distinct-baguette
    "0x0f863d92dd2b960e3eb6a23a35fd92a91981404e",  # @Qualitative
    "0x70ec235a31eb35f243e2618d6ea3b5b8962bbb5d",  # @vague-sourdough
    "0x2d8b401d2f0e6937afebf18e19e11ca568a5260a",  # @vidarx
    "0x63ce342161250d705dc0b16df89036c8e5f9ba9a",  # @0x8dxd

    # @HondaCivic — 88% WR across 3,114 trades, +$48.4K in 90 days. Surfaced via
    # r/polymarketAnalysis post about polycoolapp.com (alternative to polybot-arena).
    # Wallet verified 2026-04-16 via polymarket.com/profile/@HondaCivic HTML scrape:
    # this address dominates the page (34 occurrences vs 1 for each counterparty).
    "0x15ceffed7bf820cd2d90f90ea24ae9909f5cd5fa",
}


async def _fetch_leaderboard_wallets():
    """Fetch top 100 most profitable wallets from Polymarket leaderboard.

    These are the proven winners — wallets that turned small stakes into
    massive profits. Tracking their trades is the highest-signal copy strategy.
    """
    global _leaderboard_wallets, _leaderboard_last_fetch

    # Only refresh leaderboard every 6 hours
    if time.time() - _leaderboard_last_fetch < 21600 and _leaderboard_wallets:
        return

    loop = asyncio.get_running_loop()
    try:
        resp = await loop.run_in_executor(
            None,
            lambda: requests.get(
                LEADERBOARD_API,
                params={"limit": 100, "window": "all"},
                timeout=15,
                headers={"Accept": "application/json"},
            ),
        )
        if resp.status_code != 200:
            return

        data = resp.json()
        entries = data if isinstance(data, list) else data.get("leaderboard", [])

        new_wallets = set()
        for entry in entries:
            addr = entry.get("address") or entry.get("proxyWallet") or entry.get("wallet", "")
            if addr:
                new_wallets.add(addr.lower())

        if new_wallets:
            # Merge hand-curated whales so we copy them even if they miss top-100
            new_wallets.update(w.lower() for w in MANUAL_WHALE_WALLETS)
            _leaderboard_wallets = new_wallets
            _leaderboard_last_fetch = time.time()
            logger.info(
                f"Leaderboard: loaded {len(new_wallets)} top wallet addresses "
                f"(incl. {len(MANUAL_WHALE_WALLETS)} hand-curated)"
            )

    except Exception as e:
        logger.debug(f"Leaderboard fetch failed: {e}")
        # Leaderboard fetch failed — still wire up manual whales so we don't
        # lose them on API outages
        if not _leaderboard_wallets and MANUAL_WHALE_WALLETS:
            _leaderboard_wallets = {w.lower() for w in MANUAL_WHALE_WALLETS}
            logger.info(
                f"Leaderboard fallback: using {len(_leaderboard_wallets)} "
                "hand-curated whales only"
            )


def _generate_leaderboard_signals(trades: list[dict]) -> list[dict]:
    """Generate signals when leaderboard whales make new trades.

    Different from regular copy signals: these wallets are PROVEN profitable
    from the global leaderboard, not just our local scoring.
    """
    global _leaderboard_signals_cache
    signals = []

    if not _leaderboard_wallets:
        return signals

    for t in trades:
        wallet = (t.get("proxyWallet", "") or "").lower()
        if wallet not in _leaderboard_wallets:
            continue

        side = t.get("side", "")
        title = t.get("title", "Unknown")
        size = float(t.get("size", 0) or 0)
        price = float(t.get("price", 0) or 0)
        slug = t.get("slug", "")

        if not wallet or _is_noise(title) or size < 20 or price < 0.10 or price > 0.90:
            continue

        signals.append({
            "type": "leaderboard",
            "market": title[:80],
            "slug": slug,
            "market_url": _market_url(slug),
            "wallet": _truncate_addr(wallet),
            "side": "YES" if side == "BUY" else "NO",
            "price": round(price * 100, 1),
            "size": round(size, 2),
            "score": min(75, 45 + size * 0.05),  # Bigger bet = higher conviction
            "timestamp": time.time(),
        })

    signals.sort(key=lambda s: s["size"], reverse=True)
    _leaderboard_signals_cache = signals[:15]
    return _leaderboard_signals_cache


# ══════════════════════════════════════════════════════════════
# FEATURE 13: RESOLUTION CALENDAR SNIPER
# ══════════════════════════════════════════════════════════════

async def _detect_resolution_sniper(markets: list) -> list:
    """Find markets resolving within 48 hours where outcome is near-certain.

    Markets at 90%+ YES or 10%- YES that resolve within 2 days are
    extremely high-probability bets. The price will converge to $1 on resolution.
    """
    global _resolution_sniper_cache
    signals = []

    for market in markets:
        question = market.get("question", "")
        slug = market.get("slug", "")
        if not slug or _is_noise(question):
            continue

        yes_price = get_yes_price(market)
        days = _days_to_resolution(market)

        # Target: resolving within 48h AND price is extreme (high certainty)
        if days is None or days > 2:
            continue

        volume = float(market.get("volume24hr", 0) or 0)
        if volume < 10000:
            continue

        if yes_price >= 0.85:
            # Market nearly certain YES — buy YES to collect $1 on resolution
            profit_per_dollar = (1.0 - yes_price) / yes_price
            signals.append({
                "type": "resolution_sniper",
                "market": question[:80],
                "slug": slug,
                "market_url": _market_url(slug),
                "side": "YES",
                "price": round(yes_price * 100, 1),
                "days_left": round(days, 1),
                "profit_pct": round(profit_per_dollar * 100, 1),
                "volume_24h": round(volume, 0),
                "score": min(80, 50 + (yes_price - 0.80) * 200),
                "timestamp": time.time(),
            })
        elif yes_price <= 0.15:
            # Market nearly certain NO — buy NO to collect $1 on resolution
            no_price = 1.0 - yes_price
            profit_per_dollar = (1.0 - no_price) / no_price
            signals.append({
                "type": "resolution_sniper",
                "market": question[:80],
                "slug": slug,
                "market_url": _market_url(slug),
                "side": "NO",
                "price": round(no_price * 100, 1),
                "days_left": round(days, 1),
                "profit_pct": round(profit_per_dollar * 100, 1),
                "volume_24h": round(volume, 0),
                "score": min(80, 50 + (0.20 - yes_price) * 200),
                "timestamp": time.time(),
            })

    signals.sort(key=lambda s: s["score"], reverse=True)
    _resolution_sniper_cache = signals[:10]
    return _resolution_sniper_cache


# ══════════════════════════════════════════════════════════════
# SIGNAL 17: FINAL PERIOD MOMENTUM
# ══════════════════════════════════════════════════════════════

def _detect_final_period_momentum(markets: list) -> list:
    """Signal #17: Buy markets that just crossed above 80c in the final 24 hours.

    Adapted from prediction-market-backtesting final_period_momentum strategy
    (backtested on real Polymarket/Kalshi tick data). Near resolution, a market
    crossing upward through 80c has strong momentum toward 95-100c as traders
    price in near-certainty. Complements Resolution Sniper (which needs 85%+
    already established) — this catches the dynamic upward break.
    """
    global _final_period_cache
    signals = []
    now = time.time()

    for market in markets:
        slug = market.get("slug", "")
        question = market.get("question", "")
        if not slug or _is_noise(question):
            continue

        # Must be resolving within 24 hours (final period)
        days = _days_to_resolution(market)
        if days is None or days > 1.0:
            continue

        current_price = get_yes_price(market)
        # Looking for markets at 78-92c — just crossed (not already at 95%+)
        if current_price < 0.78 or current_price > 0.92:
            continue

        if slug not in _price_history:
            continue

        # Must have been below 80c at some point in last 30 minutes (fresh cross)
        history = _price_history[slug]
        recent = [(t, p) for t, p in history if now - t <= 1800]
        if len(recent) < 2:
            continue

        min_recent = min(p for _, p in recent)
        if min_recent >= 0.80:
            continue  # Already above 80c — not a fresh breakout

        volume = float(market.get("volume24hr", 0) or 0)
        if volume < 5000:
            continue

        hours_left = round(days * 24, 1)
        profit_to_resolution = (1.0 - current_price) / current_price

        signals.append({
            "type": "final_period_momentum",
            "market": question[:80],
            "slug": slug,
            "market_url": _market_url(slug),
            "side": "YES",
            "current_price": round(current_price * 100, 1),
            "low_30min": round(min_recent * 100, 1),
            "hours_left": hours_left,
            "profit_pct": round(profit_to_resolution * 100, 1),
            "score": min(85, 55 + (current_price - 0.78) * 200),
            "volume_24h": round(volume, 0),
            "timestamp": now,
        })

    signals.sort(key=lambda s: s["score"], reverse=True)
    _final_period_cache = signals[:10]
    return _final_period_cache


# ══════════════════════════════════════════════════════════════
# FEATURE 14: CONDITIONAL MARKET CHAIN CASCADE
# ══════════════════════════════════════════════════════════════

# Markets that should move together (if one moves, others should follow)
_CHAIN_GROUPS = {
    "iran_conflict": [
        "iran conflict", "iran ceasefire", "strait of hormuz", "oil price",
        "crude oil", "invade iran", "iran military", "kharg island",
    ],
    "us_election_2028": [
        "2028 democratic", "2028 republican", "win the 2028", "2028 nomination",
        "gavin newsom", "trump 2028", "desantis",
    ],
    "fed_policy": [
        "fed cut", "fed rate", "interest rate", "fed decrease", "no change in fed",
        "recession", "unemployment", "inflation",
    ],
    "ukraine_war": [
        "ukraine ceasefire", "russia ukraine", "crimea", "zelensky",
    ],
}


def _detect_chain_signals(markets: list) -> list:
    """Detect when one market in a chain moves but related markets haven't caught up.

    Example: "Iran conflict ends" drops 20% but "Strait of Hormuz normalizes"
    hasn't moved yet. The Hormuz market should follow — that's the edge.
    """
    global _chain_signals_cache
    signals = []

    # Group markets by chain
    chain_markets: dict[str, list] = {}
    for m in markets:
        q = m.get("question", "").lower()
        for chain_name, keywords in _CHAIN_GROUPS.items():
            if any(kw in q for kw in keywords):
                chain_markets.setdefault(chain_name, []).append(m)
                break

    # For each chain, check if any market has a rapid move but others don't
    for chain_name, group in chain_markets.items():
        if len(group) < 2:
            continue

        # Find markets with recent rapid moves
        movers = []
        stale = []
        for m in group:
            slug = m.get("slug", "")
            if slug in _price_history and len(_price_history[slug]) >= 2:
                history = _price_history[slug]
                recent = history[-1][1]
                oldest = history[0][1]
                move = abs(recent - oldest)
                if move >= 0.08:  # 8%+ move
                    movers.append((m, recent - oldest))
                else:
                    stale.append(m)

        if not movers or not stale:
            continue

        # Stale markets = haven't caught up to the chain move
        avg_direction = sum(d for _, d in movers) / len(movers)  # Positive = chain moving YES

        for m in stale:
            slug = m.get("slug", "")
            question = m.get("question", "")
            if _is_noise(question):
                continue

            current_price = get_yes_price(m)
            if current_price <= 0.10 or current_price >= 0.90:
                continue

            # If chain is moving UP and this market hasn't → buy YES
            # If chain is moving DOWN and this market hasn't → buy NO
            if avg_direction > 0.05:
                side = "YES"
            elif avg_direction < -0.05:
                side = "NO"
            else:
                continue

            signals.append({
                "type": "chain_cascade",
                "market": question[:80],
                "slug": slug,
                "market_url": _market_url(slug),
                "chain": chain_name,
                "side": side,
                "current_price": round(current_price * 100, 1),
                "chain_move": round(avg_direction * 100, 1),
                "lagging_by": round(abs(avg_direction) * 100, 1),
                "score": min(65, 30 + abs(avg_direction) * 200),
                "timestamp": time.time(),
            })

    signals.sort(key=lambda s: s["score"], reverse=True)
    _chain_signals_cache = signals[:10]
    return _chain_signals_cache


# ══════════════════════════════════════════════════════════════
# KALSHI MARKET FETCHING
# ══════════════════════════════════════════════════════════════

async def _fetch_kalshi_markets() -> list[dict]:
    """Fetch active Kalshi markets for cross-reference."""
    loop = asyncio.get_running_loop()
    try:
        resp = await loop.run_in_executor(
            None,
            lambda: requests.get(
                KALSHI_MARKETS_API,
                params={"limit": 100, "status": "open"},
                timeout=10,
                headers={"Accept": "application/json"},
            ),
        )
        resp.raise_for_status()
        data = resp.json()
        markets = data.get("markets", [])
        return markets if isinstance(markets, list) else []
    except Exception as e:
        logger.debug(f"Kalshi markets fetch failed: {e}")
        return []


def _match_kalshi(poly_question: str, kalshi_markets: list[dict]) -> dict | None:
    """Find best matching Kalshi market by keyword overlap (same logic as Manifold)."""
    poly_words = set(re.sub(r'[^\w\s]', ' ', poly_question.lower()).split())
    poly_words -= {"will", "the", "be", "a", "an", "in", "on", "at", "to", "by", "of", "or", "and", "is"}

    best_match = None
    best_score = 0

    for m in kalshi_markets:
        title = m.get("title", "")
        m_words = set(re.sub(r'[^\w\s]', ' ', title.lower()).split())
        m_words -= {"will", "the", "be", "a", "an", "in", "on", "at", "to", "by", "of", "or", "and", "is"}

        if not poly_words or not m_words:
            continue

        overlap = len(poly_words & m_words)
        union = len(poly_words | m_words)
        if union == 0:
            continue

        jaccard = overlap / union
        if overlap >= 3 and jaccard > 0.40 and jaccard > best_score:
            best_score = jaccard
            best_match = m

    return best_match


# ══════════════════════════════════════════════════════════════
# GOOGLE NEWS CATALYST DETECTION
# ══════════════════════════════════════════════════════════════

async def _check_news_catalyst(question: str, direction: str) -> bool:
    """Check Google News RSS for recent headlines that match the market topic.

    Returns True only if a recent headline (last 6 hours) contains at least 2 of the
    market's keywords. This prevents false positives from unrelated news.
    """
    loop = asyncio.get_running_loop()
    try:
        keywords = _extract_keywords(question)
        if not keywords:
            return False
        query = quote_plus(" ".join(keywords[:4]))
        url = f"https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"

        resp = await loop.run_in_executor(
            None,
            lambda: requests.get(url, timeout=8, headers={"User-Agent": "Mozilla/5.0 (compatible; GKBot/1.0)"}),
        )
        if resp.status_code != 200:
            return False

        root = ET.fromstring(resp.content)
        items = root.findall(".//item")
        if not items:
            return False

        now = time.time()
        six_hours_ago = now - 6 * 3600
        kw_lower = {k.lower() for k in keywords}

        for item in items[:5]:
            # Check recency
            is_recent = False
            pub_date_el = item.find("pubDate")
            if pub_date_el is not None and pub_date_el.text:
                try:
                    from email.utils import parsedate_to_datetime
                    dt = parsedate_to_datetime(pub_date_el.text)
                    is_recent = dt.timestamp() > six_hours_ago
                except Exception:
                    pass

            if not is_recent:
                continue

            # Check keyword overlap — headline must share 2+ keywords with market
            title_el = item.find("title")
            if title_el is None or not title_el.text:
                continue
            headline_words = {w.lower() for w in re.sub(r'[^\w\s]', ' ', title_el.text).split() if len(w) > 2}
            overlap = kw_lower & headline_words
            if len(overlap) >= 2:
                return True

        return False

    except Exception as e:
        logger.debug(f"News catalyst check failed for '{question[:40]}': {e}")
        return False


# ══════════════════════════════════════════════════════════════
# COMPOSITE SIGNAL SCORING (0-100)
# ══════════════════════════════════════════════════════════════

def _score_signal(
    edge_pct: float,
    sources_count: int,
    wallet_win_rate: float | None,
    days_to_resolution: float | None,
    has_news_catalyst: bool,
    resolution_type: str | None = None,
) -> float:
    """Score a mispricing signal on a 0-100 scale.

    Components:
    - Multi-source edge (0-30 pts): bigger edge = more conviction
    - Source agreement (0-20 pts): 2+ sources agreeing is strong
    - Wallet quality (0-20 pts): if a top wallet is also trading this market
    - Time decay (0-20 pts): prefer markets resolving in 1-7 days
      * Enhanced by resolution type prediction: sports=20, deadline=10, election=0
    - News catalyst (0-10 pts): recent breaking news supports edge
    """
    score = 0.0

    # Multi-source edge (0-30 pts) — 3 points per 1% edge, capped at 30
    score += min(30.0, abs(edge_pct) * 3.0)

    # Source agreement (0-20 pts) — 10 per agreeing source
    score += min(20.0, sources_count * 10.0)

    # Wallet quality (0-20 pts)
    if wallet_win_rate is not None and wallet_win_rate >= 50.0:
        score += min(20.0, (wallet_win_rate - 50.0) * 2.0)

    # Time decay — prefer soon-resolving but don't kill long-dated high-edge bets (0-20 pts)
    # Long-dated markets often have the MOST edge because they're hardest to price.
    # Big edge (10%+) gets minimum 10 pts regardless of timeframe.
    time_pts_assigned = False
    if days_to_resolution is not None:
        if 1 <= days_to_resolution <= 7:
            score += 20.0
        elif 7 < days_to_resolution <= 30:
            score += 15.0
        elif 30 < days_to_resolution <= 90:
            # Long-dated but still worth considering if edge is big
            score += 10.0 if abs(edge_pct) >= 10.0 else 5.0
        # > 90 days: 0 pts (filtered out upstream anyway)
        time_pts_assigned = True

    if not time_pts_assigned and resolution_type is not None:
        if resolution_type == "sports":
            score += 20.0
        elif resolution_type in ("crypto_price", "deadline"):
            score += 15.0
        elif resolution_type == "election":
            # Elections are long-dated but can have massive edge
            score += 10.0 if abs(edge_pct) >= 10.0 else 5.0

    # News catalyst (0-10 pts)
    if has_news_catalyst:
        score += 10.0

    return round(score, 1)


# ══════════════════════════════════════════════════════════════
# CLAUDE HAIKU CONFIRMATION (score >= 50 only)
# ══════════════════════════════════════════════════════════════

async def _claude_confirm_signal(market: str, edge: float, sources: list[str]) -> bool:
    """Quick Claude Haiku check on high-scoring signals. Returns True if confirmed.

    Only called when score >= 50 and ANTHROPIC_API_KEY is set.
    Cost: ~$0.001 per call, max ~5/day.
    """
    if not settings.ANTHROPIC_API_KEY:
        return False

    try:
        import anthropic
        loop = asyncio.get_running_loop()

        client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)
        sources_str = ", ".join(sources)
        today = datetime.now(timezone.utc).strftime("%B %d, %Y")
        prompt = (
            f"Today's date: {today}\n"
            f"Prediction market question: \"{market}\"\n"
            f"Sources ({sources_str}) suggest {abs(edge):.1f}% mispricing. "
            f"Edge direction: {'YES underpriced' if edge > 0 else 'NO underpriced'}.\n"
            f"In ONE sentence, does this mispricing seem plausible? Reply CONFIRM or REJECT with a brief reason."
        )

        resp = await loop.run_in_executor(
            None,
            lambda: client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=80,
                messages=[{"role": "user", "content": prompt}],
            ),
        )
        text = resp.content[0].text.strip().upper() if resp.content else ""
        confirmed = text.startswith("CONFIRM")
        logger.info(f"Claude signal check: {'CONFIRMED' if confirmed else 'REJECTED'} — {market[:50]}")
        return confirmed

    except Exception as e:
        logger.debug(f"Claude confirmation failed: {e}")
        return False


# ══════════════════════════════════════════════════════════════
# TIME-TO-RESOLUTION HELPER
# ══════════════════════════════════════════════════════════════

def _days_to_resolution(market: dict) -> float | None:
    """Calculate days until market closes. Returns None if no end date."""
    end_date = market.get("endDate", "") or market.get("end_date_iso", "")
    if not end_date:
        return None
    try:
        end_dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
        days = (end_dt - datetime.now(timezone.utc)).total_seconds() / 86400
        return max(0, days)
    except (ValueError, TypeError):
        return None


# ══════════════════════════════════════════════════════════════
# COMPONENT 1: MULTI-SOURCE MISPRICING SCANNER
# ══════════════════════════════════════════════════════════════

async def _fetch_polymarket_markets() -> list[dict]:
    """Fetch active Polymarket markets sorted by 24h volume."""
    loop = asyncio.get_running_loop()
    try:
        resp = await loop.run_in_executor(
            None,
            lambda: requests.get(
                POLYMARKET_MARKETS_API,
                params={
                    "limit": 100,
                    "active": "true",
                    "order": "volume24hr",
                    "ascending": "false",
                },
                timeout=15,
                headers={"Accept": "application/json"},
            ),
        )
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else []
    except Exception as e:
        logger.error(f"Polymarket markets fetch failed: {e}")
        return []


async def _search_manifold(keyword: str) -> list[dict]:
    """Search Manifold Markets for matching markets."""
    loop = asyncio.get_running_loop()
    try:
        resp = await loop.run_in_executor(
            None,
            lambda: requests.get(
                MANIFOLD_SEARCH_API,
                params={"term": keyword, "limit": 20},
                timeout=10,
                headers={"Accept": "application/json"},
            ),
        )
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else []
    except Exception as e:
        logger.debug(f"Manifold search failed for '{keyword}': {e}")
        return []


def _match_markets(poly_question: str, manifold_markets: list[dict]) -> dict | None:
    """Find the best matching Manifold market for a Polymarket question.

    Uses word overlap scoring. Returns the best match if overlap is sufficient.
    """
    poly_words = set(re.sub(r'[^\w\s]', ' ', poly_question.lower()).split())
    poly_words -= {"will", "the", "be", "a", "an", "in", "on", "at", "to", "by", "of", "or", "and", "is"}

    best_match = None
    best_score = 0

    for m in manifold_markets:
        m_question = m.get("question", "")
        m_words = set(re.sub(r'[^\w\s]', ' ', m_question.lower()).split())
        m_words -= {"will", "the", "be", "a", "an", "in", "on", "at", "to", "by", "of", "or", "and", "is"}

        if not poly_words or not m_words:
            continue

        overlap = len(poly_words & m_words)
        union = len(poly_words | m_words)
        if union == 0:
            continue

        jaccard = overlap / union
        # Require at least 3 word overlap and 30% Jaccard similarity
        if overlap >= 3 and jaccard > 0.40 and jaccard > best_score:
            best_score = jaccard
            best_match = m

    return best_match


async def _scan_mispricing():
    """Multi-source mispricing scan: Polymarket vs Manifold + Kalshi + news catalysts."""
    global _mispricing_cache, _mispricing_last_scan

    await ws_manager.send_log("[EDGE] Multi-source mispricing scan starting...", "info")
    await _brain("Connecting to Polymarket, Manifold, and Kalshi to compare prices across platforms...")

    # Fetch all source markets in parallel
    poly_markets, kalshi_markets = await asyncio.gather(
        _fetch_polymarket_markets(),
        _fetch_kalshi_markets(),
    )

    if not poly_markets:
        await ws_manager.send_log("[EDGE] No Polymarket markets fetched", "warning")
        await _brain("Could not reach Polymarket API. Retrying next cycle.")
        return

    kalshi_count = len(kalshi_markets)
    await ws_manager.send_log(
        f"[EDGE] Sources loaded: Polymarket ({len(poly_markets)}), Kalshi ({kalshi_count})", "info"
    )
    await _brain(f"Loaded {len(poly_markets)} Polymarket markets and {kalshi_count} Kalshi markets. Scanning for price disagreements...")

    # Feature 2: Detect rapid price moves across all markets
    rapid_moves = _detect_rapid_moves(poly_markets)
    if rapid_moves:
        for rm in rapid_moves[:3]:
            await ws_manager.send_log(
                f"[ALERT] Rapid move: \"{rm['market'][:50]}\" {'+' if rm['direction'] == 'UP' else ''}"
                f"{rm['move_pct']}% in {rm['minutes_ago']} min "
                f"({rm['old_price']}% -> {rm['new_price']}%)",
                "warning",
            )

    # Feature 4: Detect correlated market arbitrage
    corr_arbs = _detect_correlated_arbs(poly_markets)
    if corr_arbs:
        for arb in corr_arbs[:2]:
            top = arb["markets"][0] if arb["markets"] else {}
            bot = arb["markets"][-1] if arb["markets"] else {}
            await ws_manager.send_log(
                f"[ARB] Correlated: \"{arb['group']}\" group -- "
                f"\"{top.get('question', '')[:30]}\" {top.get('price', 0)}% vs "
                f"\"{bot.get('question', '')[:30]}\" {bot.get('price', 0)}% "
                f"({arb['spread']}% spread)",
                "warning",
            )

    # Build rapid-move slug lookup for score boosting
    rapid_move_slugs = {}
    for rm in rapid_moves:
        rapid_move_slugs[rm["slug"]] = rm

    mispriced = []
    checked = 0

    # Focus on mid-range odds markets (interesting for mispricing)
    candidates = []
    for pm in poly_markets:
        if _is_noise(pm.get("question", "")):
            continue
        try:
            prices = json.loads(pm.get("outcomePrices", "[]"))
            if prices and len(prices) >= 1:
                yes_price = float(prices[0])
                if 0.10 <= yes_price <= 0.90:
                    candidates.append((pm, yes_price))
        except (json.JSONDecodeError, ValueError, IndexError):
            continue

    # Scan top 50 by volume (was 25 — missed 50% of opportunities)
    candidates = candidates[:50]

    for pm, poly_prob in candidates:
        question = pm.get("question", "")
        keywords = _extract_keywords(question)
        if not keywords:
            continue

        search_term = " ".join(keywords)
        manifold_results = await _search_manifold(search_term)
        checked += 1

        # Collect probabilities from multiple sources
        sources = []
        source_names = []
        manifold_prob = None
        kalshi_prob = None

        # Source 1: Manifold
        if manifold_results:
            match = _match_markets(question, manifold_results)
            if match:
                mp = match.get("probability")
                if mp is not None and 0.03 <= mp <= 0.97:
                    manifold_prob = mp
                    sources.append(mp)
                    source_names.append("Manifold")

        # Source 2: Kalshi
        if kalshi_markets:
            kalshi_match = _match_kalshi(question, kalshi_markets)
            if kalshi_match:
                # Kalshi API uses 'yes_bid_dollars' and 'yes_ask_dollars' (dollar format, e.g. 0.67)
                yes_bid = kalshi_match.get("yes_bid_dollars") or kalshi_match.get("yes_bid")
                yes_ask = kalshi_match.get("yes_ask_dollars") or kalshi_match.get("yes_ask")
                if yes_bid is not None and yes_ask is not None:
                    try:
                        bid_f = float(yes_bid)
                        ask_f = float(yes_ask)
                        # If values > 1, assume cents; otherwise dollars
                        if bid_f > 1.0 or ask_f > 1.0:
                            bid_norm = bid_f / 100.0
                            ask_norm = ask_f / 100.0
                        else:
                            bid_norm = bid_f
                            ask_norm = ask_f
                        # Skip if bid-ask spread > 20% — too illiquid for reliable pricing
                        if ask_norm - bid_norm > 0.20:
                            kp = None
                        else:
                            kp = (bid_norm + ask_norm) / 2.0
                        if kp is not None and 0.03 <= kp <= 0.97:
                            kalshi_prob = kp
                            sources.append(kp)
                            source_names.append("Kalshi")
                    except (ValueError, TypeError):
                        pass

        if not sources:
            continue

        # Composite fair value: average of all sources
        fair_value = sum(sources) / len(sources)
        edge = round((fair_value - poly_prob) * 100, 1)

        # Brain: narrate the comparison
        src_str = " + ".join(f"{n} {s*100:.0f}%" for n, s in zip(source_names, sources))
        await _brain(
            f"Comparing: \"{question[:55]}\" \u2014 "
            f"Polymarket {poly_prob*100:.0f}% vs {src_str} = "
            f"{abs(edge):.1f}% {'edge' if abs(edge) >= 8 else 'gap (too small)'}"
        )

        # Require 8%+ edge — 5% was too loose, vanishes in thin markets
        if abs(edge) < 8.0:
            continue

        # Time-to-resolution
        days = _days_to_resolution(pm)

        # Skip markets > 365 days out (too far for useful signals)
        # Markets 90-365 days can still have valid edges — scoring handles the time decay
        if days is not None and days > 365:
            continue

        # News catalyst check (only for markets with 5%+ edge to save API calls)
        direction = "BUY YES" if edge > 0 else "BUY NO"
        has_news = await _check_news_catalyst(question, direction)

        # Check if any scored wallet is trading this market
        slug = pm.get("slug", "")
        best_wallet_wr = None
        for addr, ws in _wallet_scores.items():
            if ws["win_rate"] >= 65 and ws["trades"] >= 20:
                for t in ws["recent_trades"]:
                    if t.get("slug") == slug or (question[:30].lower() in (t.get("title", "").lower())):
                        if best_wallet_wr is None or ws["win_rate"] > best_wallet_wr:
                            best_wallet_wr = ws["win_rate"]
                        break

        # Resolution type prediction (Feature 5)
        res_type = _estimate_resolution_type(question)

        # Compute composite score
        score = _score_signal(
            edge_pct=abs(edge),
            sources_count=len(sources),
            wallet_win_rate=best_wallet_wr,
            days_to_resolution=days,
            has_news_catalyst=has_news,
            resolution_type=res_type["type"],
        )

        # Feature 2: Boost score for rapid moves in same direction as our edge
        rapid_move_boost = False
        if slug in rapid_move_slugs:
            rm = rapid_move_slugs[slug]
            # If price moving UP and our edge says YES (edge > 0), or DOWN and NO (edge < 0)
            if (rm["direction"] == "UP" and edge > 0) or (rm["direction"] == "DOWN" and edge < 0):
                score = min(100.0, score + 10.0)
                rapid_move_boost = True

        # Claude confirmation for high-scoring signals (score >= 50)
        claude_confirmed = False
        if score >= 50 and settings.ANTHROPIC_API_KEY:
            await _brain(f"Score {score:.0f}/100 \u2014 asking AI to verify: \"{question[:45]}\"...")
            claude_confirmed = await _claude_confirm_signal(question, edge, source_names)
            if claude_confirmed:
                score = min(100.0, score + 10.0)
                await _brain(f"AI CONFIRMED \u2714 Score boosted to {score:.0f}/100")
            else:
                await _brain(f"AI no boost \u2718 Score stays {score:.0f}/100 (still bettable if score \u2265 threshold)")

        # Brain: narrate the edge found
        direction = "YES underpriced" if edge > 0 else "NO underpriced"
        news_str = " + breaking news" if has_news else ""
        wallet_str = f" + whale ({best_wallet_wr:.0f}% WR)" if best_wallet_wr else ""
        boost_str = " + rapid move confirms" if rapid_move_boost else ""
        await _brain(
            f"\u2705 EDGE FOUND: \"{question[:45]}\" \u2014 {abs(edge):.1f}% ({direction}) | "
            f"Score: {score:.0f}/100{news_str}{wallet_str}{boost_str}"
        )

        poly_volume = float(pm.get("volume24hr", 0) or 0)

        entry = {
            "market": question[:100],
            "slug": slug,
            "event_slug": _event_slug_from_pm(pm),
            "market_url": _market_url(slug, _event_slug_from_pm(pm)),
            "poly_prob": round(poly_prob * 100, 1),
            "manifold_prob": round(manifold_prob * 100, 1) if manifold_prob is not None else None,
            "kalshi_prob": round(kalshi_prob * 100, 1) if kalshi_prob is not None else None,
            "fair_value": round(fair_value * 100, 1),
            "edge": edge,
            "abs_edge": abs(edge),
            "direction": direction,
            "score": score,
            "sources": source_names,
            "sources_count": len(sources),
            "has_news": has_news,
            "days_to_resolution": round(days, 1) if days is not None else None,
            "claude_confirmed": claude_confirmed,
            "rapid_move_boost": rapid_move_boost,
            "resolution_type": res_type["type"],
            "wallet_win_rate": best_wallet_wr,
            "poly_volume_24h": poly_volume,
            "manifold_question": "",
            "manifold_volume": 0,
            "timestamp": time.time(),
        }
        mispriced.append(entry)

        # Small delay between API calls
        await asyncio.sleep(0.3)

    # Sort by score descending (not just edge)
    mispriced.sort(key=lambda x: x["score"], reverse=True)
    _mispricing_cache = mispriced[:20]
    _mispricing_last_scan = time.time()

    # Log findings with new score format
    if mispriced:
        await ws_manager.send_log(
            f"[EDGE] Found {len(mispriced)} mispriced markets (checked {checked})", "success"
        )
        for m in mispriced[:3]:
            sign = "+" if m["edge"] > 0 else ""
            sources_str = " + ".join(m["sources"])
            parts = []
            if m.get("poly_prob") is not None:
                parts.append(f"Poly {m['poly_prob']}%")
            if m.get("manifold_prob") is not None:
                parts.append(f"Manifold {m['manifold_prob']}%")
            if m.get("kalshi_prob") is not None:
                parts.append(f"Kalshi {m['kalshi_prob']}%")
            probs_str = ", ".join(parts)
            fair_str = f"fair value {m['fair_value']}%"
            news_str = " +NEWS" if m["has_news"] else ""
            days_str = f" ({m['days_to_resolution']:.0f}d)" if m.get("days_to_resolution") else ""

            await ws_manager.send_log(
                f"[EDGE] Multi-source: \"{m['market'][:50]}\" -> {probs_str} = {fair_str} ({sign}{m['edge']}% edge){news_str}{days_str}",
                "success",
            )
            # Score breakdown
            edge_pts = min(30, abs(m["edge"]) * 3)
            src_pts = m["sources_count"] * 10
            time_pts = 20 if m.get("days_to_resolution") and 1 <= m["days_to_resolution"] <= 7 else (10 if m.get("days_to_resolution") and m["days_to_resolution"] <= 30 else 0)
            news_pts = 10 if m["has_news"] else 0
            wr_pts = min(20, (m["wallet_win_rate"] - 50) * 2) if m.get("wallet_win_rate") and m["wallet_win_rate"] >= 50 else 0
            await ws_manager.send_log(
                f"[SCORE] \"{m['market'][:40]}\" scored {m['score']}/100 "
                f"(edge:{edge_pts:.0f} + sources:{src_pts:.0f} + time:{time_pts:.0f} + news:{news_pts:.0f} + wallet:{wr_pts:.0f})",
                "info",
            )
    else:
        await ws_manager.send_log(
            f"[EDGE] No mispricings >5% found (checked {checked} markets)", "info"
        )


# ══════════════════════════════════════════════════════════════
# COMPONENT 2: WALLET WIN-RATE SCORING
# ══════════════════════════════════════════════════════════════

async def _fetch_recent_trades() -> list[dict]:
    """Fetch recent trades from Polymarket data API."""
    loop = asyncio.get_running_loop()
    try:
        resp = await loop.run_in_executor(
            None,
            lambda: requests.get(
                TRADES_API,
                params={"limit": 100},
                timeout=15,
                headers={"Accept": "application/json"},
            ),
        )
        resp.raise_for_status()
        trades = resp.json()
        return trades if isinstance(trades, list) else []
    except Exception as e:
        logger.error(f"Wallet tracker fetch failed: {e}")
        return []


def _is_resolved(price: float) -> bool:
    """Check if a market appears to be resolved (price at 0% or 100%)."""
    return price <= 0.02 or price >= 0.98


def _update_wallet_score(wallet: str, trade: dict):
    """Update a wallet's running score with a new trade."""
    if wallet not in _wallet_scores:
        _wallet_scores[wallet] = {
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "pending": 0,
            "buys": 0,
            "sells": 0,
            "is_market_maker": False,
            "win_rate": 0.0,
            "volume": 0.0,
            "last_seen": 0,
            "markets": set(),
            "recent_trades": [],
        }

    ws = _wallet_scores[wallet]
    size = float(trade.get("size", 0) or 0)
    price = float(trade.get("price", 0) or 0)
    side = trade.get("side", "")
    title = trade.get("title", "")
    ts = trade.get("timestamp", 0)

    ws["trades"] += 1
    ws["volume"] += size
    ws["markets"].add(title)

    # Feature 3: Track buy/sell counts for MM detection
    if side == "BUY":
        ws["buys"] = ws.get("buys", 0) + 1
    elif side == "SELL":
        ws["sells"] = ws.get("sells", 0) + 1
    ws["is_market_maker"] = _is_market_maker(ws)
    if ts and ts > ws["last_seen"]:
        ws["last_seen"] = ts

    # Keep last 50 trades for this wallet
    ws["recent_trades"].append({
        "side": side,
        "title": title,
        "size": size,
        "price": price,
        "slug": trade.get("slug", ""),
        "timestamp": ts,
    })
    if len(ws["recent_trades"]) > 50:
        ws["recent_trades"] = ws["recent_trades"][-50:]

    # Score resolved trades: if price is near 0 or 1, we can infer outcome
    # BUY at low price (< 0.15) on a market that later resolves YES = good
    # BUY at high price (> 0.85) and market resolves YES = good
    # This is approximate — real scoring happens over many scans
    if _is_resolved(price):
        # This is a trade on a resolved/nearly-resolved market
        if side == "BUY" and price >= 0.98:
            # Buying at 98%+ means market already resolved YES — skip (no edge)
            pass
        elif side == "BUY" and price <= 0.02:
            # Buying at 2% means market already resolved NO — skip
            pass
        elif side == "SELL":
            # Selling resolved positions — skip scoring
            pass
    else:
        # Active market — count as pending
        ws["pending"] += 1

    # Recalculate win rate
    total_decided = ws["wins"] + ws["losses"]
    ws["win_rate"] = round(ws["wins"] / total_decided * 100, 1) if total_decided > 0 else 0.0


def _score_historical_trades():
    """Score wallets using actual market resolution status.

    For each wallet's recent trades, check if the market has resolved (price at
    extremes: >= 0.95 or <= 0.05). This is much more reliable than comparing
    entry price to latest trade price, which was just measuring drift.
    """
    # Group all trades by market slug, find the latest price for each
    market_latest: dict[str, dict] = {}  # slug -> {price, ts}
    for ws in _wallet_scores.values():
        for t in ws["recent_trades"]:
            slug = t.get("slug", "")
            title = t.get("title", "")
            key = slug or title
            if not key:
                continue
            ts = t.get("timestamp", 0) or 0
            price = t.get("price", 0)
            if key not in market_latest or ts > market_latest[key]["ts"]:
                market_latest[key] = {"price": price, "ts": ts}

    # Score each wallet's trades against actual resolution
    # IMPORTANT: ADD to existing scores (from pre-seed), don't replace them
    for wallet, ws in _wallet_scores.items():
        live_wins = 0
        live_losses = 0
        for t in ws["recent_trades"]:
            slug = t.get("slug", "")
            title = t.get("title", "")
            key = slug or title
            if not key:
                continue

            entry_price = t.get("price", 0)
            latest = market_latest.get(key, {}).get("price", entry_price)
            side = t.get("side", "")

            resolved_yes = latest >= 0.98
            resolved_no = latest <= 0.02

            if not resolved_yes and not resolved_no:
                continue

            if side == "BUY":
                if resolved_yes:
                    live_wins += 1
                elif resolved_no:
                    live_losses += 1
            elif side == "SELL":
                if resolved_no:
                    live_wins += 1
                elif resolved_yes:
                    live_losses += 1

        # Keep pre-seeded scores, add live scores on top
        # Use _preseed_wins/_preseed_losses to track pre-seeded base
        if "_preseed_wins" not in ws:
            ws["_preseed_wins"] = ws.get("wins", 0)
            ws["_preseed_losses"] = ws.get("losses", 0)

        ws["wins"] = ws["_preseed_wins"] + live_wins
        ws["losses"] = ws["_preseed_losses"] + live_losses
        total = ws["wins"] + ws["losses"]
        ws["win_rate"] = round(ws["wins"] / total * 100, 1) if total > 0 else 0.0

    # Log scoring progress
    scored = [(addr, ws) for addr, ws in _wallet_scores.items() if ws["wins"] + ws["losses"] > 0]
    if scored:
        top_scored = sorted(scored, key=lambda x: x[1]["win_rate"], reverse=True)[:3]
        logger.info(f"Wallet scoring: {len(scored)} wallets scored, top: " +
                    ", ".join(f"{_truncate_addr(a)}={ws['win_rate']:.0f}% ({ws['wins']}W/{ws['losses']}L)"
                             for a, ws in top_scored))


def _analyze_trades(trades: list[dict]) -> dict:
    """Analyze trades: wallet stats, consensus, big trades (legacy compat + new scoring)."""
    market_activity: dict = defaultdict(lambda: {
        "buys": [], "sells": [], "total_volume": 0, "wallets": set()
    })
    big_trades = []

    for t in trades:
        wallet = t.get("proxyWallet", "")
        side = t.get("side", "")
        title = t.get("title", "Unknown")
        size = float(t.get("size", 0) or 0)
        price = float(t.get("price", 0) or 0)
        slug = t.get("slug", "")
        ts = t.get("timestamp", 0)

        if not wallet or not title:
            continue

        # Skip noise markets
        if _is_noise(title):
            continue

        # Skip dust trades
        if size < 5:
            continue

        # Skip resolved markets
        if _is_resolved(price):
            continue

        # Build a trade ID for dedup
        trade_id = f"{wallet}:{title}:{size}:{ts}"

        # Update wallet scoring (Component 2)
        if trade_id not in _seen_trade_ids:
            _seen_trade_ids.add(trade_id)
            # Cap seen trades to prevent unbounded memory growth
            if len(_seen_trade_ids) > 10000:
                # Keep only the most recent half
                _seen_trade_ids.clear()
            _update_wallet_score(wallet, t)

        # Track market-level activity for consensus
        ma = market_activity[title]
        ma["wallets"].add(wallet)
        ma["total_volume"] += size
        if slug:
            ma["slug"] = slug
        if side == "BUY":
            ma["buys"].append({"wallet": wallet, "size": size, "price": price})
        elif side == "SELL":
            ma["sells"].append({"wallet": wallet, "size": size, "price": price})

        # Track big trades ($50+)
        if size >= 50:
            big_trades.append({
                "wallet": _truncate_addr(wallet),
                "wallet_full": wallet,
                "side": side,
                "title": title,
                "size": round(size, 2),
                "price": round(price, 4),
                "slug": slug,
                "market_url": _market_url(slug),
                "timestamp": ts,
            })

    # Feature 3: Build set of market maker wallets to exclude from consensus
    mm_wallets = set()
    for addr, ws in _wallet_scores.items():
        if ws.get("is_market_maker", False):
            mm_wallets.add(addr)

    # Build consensus: markets where 2+ unique wallets are buying same side
    # Exclude detected market makers from consensus counting
    consensus = []
    for title, ma in market_activity.items():
        buy_wallets = set(b["wallet"] for b in ma["buys"]) - mm_wallets
        sell_wallets = set(s["wallet"] for s in ma["sells"]) - mm_wallets

        if len(buy_wallets) >= 2:
            valid_buys = [b for b in ma["buys"] if b["wallet"] not in mm_wallets]
            total_buy = sum(b["size"] for b in valid_buys)
            # Volume-weighted average buy price
            avg_buy_price = sum(b["price"] * b["size"] for b in valid_buys) / total_buy if total_buy > 0 else 0.50
            scored_wallets = [w for w in buy_wallets if w in _wallet_scores and _wallet_scores[w]["win_rate"] > 0]
            consensus.append({
                "title": title,
                "signal": "BUY",
                "whale_count": len(buy_wallets),
                "scored_whale_count": len(scored_wallets),
                "total_trades": len(valid_buys),
                "total_volume": round(total_buy, 2),
                "avg_price": round(avg_buy_price, 4),
                "wallets": [_truncate_addr(w) for w in sorted(buy_wallets)[:5]],
                "slug": ma.get("slug", ""),
                "market_url": _market_url(ma.get("slug", "")),
            })
        if len(sell_wallets) >= 2:
            valid_sells = [s for s in ma["sells"] if s["wallet"] not in mm_wallets]
            total_sell = sum(s["size"] for s in valid_sells)
            avg_sell_price = sum(s["price"] * s["size"] for s in valid_sells) / total_sell if total_sell > 0 else 0.50
            scored_wallets = [w for w in sell_wallets if w in _wallet_scores and _wallet_scores[w]["win_rate"] > 0]
            consensus.append({
                "title": title,
                "signal": "SELL",
                "whale_count": len(sell_wallets),
                "scored_whale_count": len(scored_wallets),
                "total_trades": len(valid_sells),
                "total_volume": round(total_sell, 2),
                "avg_price": round(avg_sell_price, 4),
                "wallets": [_truncate_addr(w) for w in sorted(sell_wallets)[:5]],
                "slug": ma.get("slug", ""),
                "market_url": _market_url(ma.get("slug", "")),
            })

    consensus.sort(key=lambda c: c["whale_count"] * c["total_volume"], reverse=True)
    big_trades.sort(key=lambda t: t["size"], reverse=True)

    return {
        "consensus": consensus[:15],
        "big_trades": big_trades[:30],
    }


# ══════════════════════════════════════════════════════════════
# COMPONENT 3: SMART COPY SIGNALS
# ══════════════════════════════════════════════════════════════

def _generate_copy_signals(trades: list[dict]):
    """Generate copy signals when high-rated wallets make new trades."""
    global _copy_signals

    new_signals = []
    hypothetical_bankroll = 1000.0  # For Kelly sizing

    # Rank wallets by composite score: win_rate * sqrt(trade_count)
    ranked = _get_ranked_wallets()

    # Build a lookup: wallet_addr -> rank
    wallet_rank_map = {}
    for i, w in enumerate(ranked):
        wallet_rank_map[w["address"]] = i + 1

    # Pre-scan: detect spread-betting wallets (buying 3+ competing outcomes)
    # e.g., buying YES on Pistons + Celtics + Thunder + Nuggets to win Finals
    wallet_buy_markets: dict[str, set] = defaultdict(set)
    for t in trades:
        w = t.get("proxyWallet", "")
        s = t.get("side", "")
        title = t.get("title", "Unknown")
        if w and s == "BUY":
            wallet_buy_markets[w].add(title)

    # Wallets buying 5+ different markets in one batch are likely spreading
    spread_wallets = {w for w, markets in wallet_buy_markets.items() if len(markets) >= 5}

    for t in trades:
        wallet = t.get("proxyWallet", "")
        side = t.get("side", "")
        title = t.get("title", "Unknown")
        size = float(t.get("size", 0) or 0)
        price = float(t.get("price", 0) or 0)
        slug = t.get("slug", "")
        ts = t.get("timestamp", 0)

        if not wallet or _is_noise(title) or size < 5 or _is_resolved(price):
            continue

        # Skip spread-betting wallets (buying all outcomes, not directional)
        if wallet in spread_wallets:
            continue

        # Only generate signals for wallets with: win_rate >= 65%, 20+ trades tracked
        # (matches simulator thresholds — no wasted signals)
        ws = _wallet_scores.get(wallet)
        if not ws or ws["trades"] < 20 or ws["win_rate"] < 65.0:
            continue

        # Require enough resolved trades for reliable win rate
        resolved = ws.get("wins", 0) + ws.get("losses", 0)
        if resolved < 5:
            continue

        # Feature 3: Exclude market makers from copy signals
        if ws.get("is_market_maker", False):
            continue

        rank = wallet_rank_map.get(wallet, 999)
        win_rate = ws["win_rate"]

        # Look up edge from mispricing cache
        edge = None
        for mp in _mispricing_cache:
            if mp.get("slug") == slug or (title and title[:30].lower() in mp.get("market", "").lower()):
                edge = mp["edge"]
                break

        # Simple Kelly criterion for suggested size
        # Kelly = (win_rate * payout - loss_rate) / payout
        # With prediction markets: payout ~ (1/price - 1) for YES bets
        if price > 0.05 and price < 0.95:
            payout_ratio = (1.0 / price) - 1.0 if side == "BUY" else (1.0 / (1.0 - price)) - 1.0
            wr = win_rate / 100.0
            kelly = (wr * payout_ratio - (1.0 - wr)) / payout_ratio if payout_ratio > 0 else 0
            kelly = max(0, min(kelly, 0.10))  # Cap at 10% of bankroll
            suggested = round(hypothetical_bankroll * kelly, 2)
        else:
            suggested = 0

        signal = {
            "signal": "COPY",
            "wallet": _truncate_addr(wallet),
            "wallet_full": wallet,
            "wallet_score": win_rate,
            "wallet_rank": rank,
            "wallet_trades": ws["trades"],
            "action": f"{side} YES" if side == "BUY" else f"{side} NO" if side == "SELL" else side,
            "market": title[:100],
            "slug": slug,
            "market_url": _market_url(slug),
            "size": round(size, 2),
            "price": round(price * 100, 1),
            "suggested_size": suggested,
            "edge": edge,
            "timestamp": ts or time.time(),
        }
        new_signals.append(signal)

    # Add new signals, keep last 50
    if new_signals:
        _copy_signals.extend(new_signals)
        _copy_signals.sort(key=lambda s: s.get("timestamp", 0), reverse=True)
        if len(_copy_signals) > 50:
            _copy_signals[:] = _copy_signals[:50]

    return new_signals


def _get_ranked_wallets() -> list[dict]:
    """Rank wallets by: win_rate * sqrt(trade_count). Returns sorted list."""
    ranked = []
    for addr, ws in _wallet_scores.items():
        if ws["trades"] < 3:
            continue
        composite = ws["win_rate"] * math.sqrt(ws["trades"])
        buy_ratio = ws.get("buys", 0) / max(1, ws.get("buys", 0) + ws.get("sells", 0))
        ranked.append({
            "address": addr,
            "trades": ws["trades"],
            "wins": ws["wins"],
            "losses": ws["losses"],
            "win_rate": ws["win_rate"],
            "volume": round(ws["volume"], 2),
            "markets": len(ws["markets"]),
            "last_seen": ws["last_seen"],
            "composite_score": round(composite, 1),
            "is_market_maker": ws.get("is_market_maker", False),
            "buy_ratio": round(buy_ratio * 100, 0),
        })
    ranked.sort(key=lambda x: x["composite_score"], reverse=True)
    return ranked


# ══════════════════════════════════════════════════════════════
# SCAN ORCHESTRATION
# ══════════════════════════════════════════════════════════════

async def _scan_wallets():
    """Fetch recent trades, update wallet scores, generate copy signals."""
    global _recent_trades, _big_trades, _consensus
    global _last_scan_time, _scan_error

    try:
        await ws_manager.send_log("[WALLETS] Fetching latest Polymarket trades...", "info")
        trades = await _fetch_recent_trades()

        if not trades:
            _scan_error = "No trades returned"
            await ws_manager.send_log("[WALLETS] No trades found", "warning")
            return

        _scan_error = None
        _recent_trades = trades

        # Analyze trades: consensus, big trades, update wallet scores
        result = _analyze_trades(trades)
        _consensus = result["consensus"]
        _big_trades = result["big_trades"]

        # Score historical trades retroactively
        _score_historical_trades()

        # Generate copy signals from top wallets
        new_signals = _generate_copy_signals(trades)

        # Generate leaderboard whale signals
        lb_signals = _generate_leaderboard_signals(trades)
        if lb_signals:
            await ws_manager.send_log(
                f"[WHALE] Leaderboard whale trading: {lb_signals[0]['wallet']} "
                f"{lb_signals[0]['side']} ${lb_signals[0]['size']:.0f} on \"{lb_signals[0]['market'][:35]}\"",
                "success",
            )

        _last_scan_time = time.time()

        # Count scored wallets and market makers
        scored_count = sum(1 for ws in _wallet_scores.values() if ws["trades"] >= 10)
        mm_count = sum(1 for ws in _wallet_scores.values() if ws.get("is_market_maker", False))
        total_wallets = len(_wallet_scores)

        # Feature 3: Log newly detected market makers
        for addr, ws in _wallet_scores.items():
            if ws.get("is_market_maker", False) and ws["trades"] >= 10:
                buy_ratio = ws.get("buys", 0) / max(1, ws.get("buys", 0) + ws.get("sells", 0))
                if ws["trades"] == 10:  # Log only when first detected (at threshold)
                    logger.info(f"[MM] Wallet {_truncate_addr(addr)} detected as market maker "
                               f"({buy_ratio*100:.0f}% buy ratio) -- excluded from signals")

        summary = (
            f"[WALLETS] Scan: {len(trades)} trades, {total_wallets} wallets tracked "
            f"({scored_count} scored, {mm_count} MM excluded), {len(_consensus)} consensus, "
            f"{len(new_signals)} new copy signals"
        )
        logger.info(summary)
        await ws_manager.send_log(summary, "success")

        # Log copy signals
        for s in new_signals[:3]:
            edge_str = f" | {s['edge']:+.1f}% edge" if s.get("edge") else ""
            await ws_manager.send_log(
                f"[COPY] Wallet {s['wallet']} ({s['wallet_score']:.0f}% win rate) -> "
                f"{s['action']} \"{s['market'][:40]}\" ${s['size']:.0f}{edge_str}",
                "success",
            )

        # Log consensus with scored wallets
        scored_consensus = [c for c in _consensus if c.get("scored_whale_count", 0) >= 2]
        for c in scored_consensus[:2]:
            await ws_manager.send_log(
                f"[CONSENSUS] {c['whale_count']} wallets ({c['scored_whale_count']} scored) "
                f"all {c['signal'].lower()}ing \"{c['title'][:40]}\" (${c['total_volume']:.0f})",
                "success",
            )

        # Log big trades
        for t in _big_trades[:3]:
            # Include win rate if wallet is scored
            wr_str = ""
            for addr, ws in _wallet_scores.items():
                if _truncate_addr(addr) == t["wallet"] and ws["win_rate"] > 0:
                    wr_str = f" [{ws['win_rate']:.0f}% WR]"
                    break
            await ws_manager.send_log(
                f"[WALLETS] BIG TRADE: {t['wallet']}{wr_str} {t['side']} "
                f"${t['size']:.0f} on \"{t['title'][:35]}\"",
                "info",
            )

    except Exception as e:
        _scan_error = str(e)
        logger.error(f"Wallet tracker error: {e}", exc_info=True)
        await ws_manager.send_log(f"[WALLETS] Error: {e}", "error")


async def scan_all():
    """Run all intelligence scans. Called every cycle."""
    from app.services.polymarket_simulator import WEATHER_ONLY_MODE

    # Run mispricing + new signal scans every 10 min
    # Skip entirely in WEATHER_ONLY_MODE — these signals aren't used and clutter Bot Brain
    should_scan_mispricing = (time.time() - _mispricing_last_scan) > 600 and not WEATHER_ONLY_MODE

    if should_scan_mispricing:
        try:
            await _scan_mispricing()
        except Exception as e:
            logger.error(f"Mispricing scan error: {e}", exc_info=True)
            await ws_manager.send_log(f"[EDGE] Mispricing scan error: {e}", "error")

        # Run new signal generators (reuse fetched market data from mispricing cache)
        try:
            poly_markets = await _fetch_polymarket_markets()
            if poly_markets:
                # Feature 6: Overround arbitrage (disabled by default — not true arb)
                overrounds = _detect_overround_arbs(poly_markets) if await _sig_scan_on("overround") else []
                if overrounds:
                    await ws_manager.send_log(
                        f"[OVERROUND] Found {len(overrounds)} overround arbs "
                        f"(best: {overrounds[0]['overround_pct']}% overround)",
                        "warning",
                    )

                # Feature 7: Momentum / mean reversion
                momentum = _detect_momentum_signals(poly_markets) if await _sig_scan_on("momentum") else []
                if momentum:
                    await ws_manager.send_log(
                        f"[MOMENTUM] Found {len(momentum)} mean-reversion signals "
                        f"(best: {momentum[0]['edge']}% edge)",
                        "warning",
                    )

                # Signal 16: Panic fade — gated on momentum (same category)
                panic_fade = _detect_panic_fade(poly_markets) if await _sig_scan_on("momentum") else []
                if panic_fade:
                    await ws_manager.send_log(
                        f"[PANIC] {len(panic_fade)} panic-fade opportunities "
                        f"(best: {panic_fade[0]['drop_pct']}% drop → {panic_fade[0]['current_price']}c, "
                        f"target {panic_fade[0]['rebound_target']}c)",
                        "warning",
                    )

                # Feature 8: Breaking news speed edge
                news_speed = await _detect_news_speed_signals(poly_markets)
                if news_speed:
                    await ws_manager.send_log(
                        f"[SPEED] Found {len(news_speed)} news speed edges "
                        f"(Manifold lagging {news_speed[0]['speed_edge']}% behind Poly)",
                        "warning",
                    )

                # Feature 9: YES+NO arbitrage (guaranteed profit)
                yesno_arbs = await _detect_yesno_arbs(poly_markets[:15])  # Limit CLOB calls
                if yesno_arbs:
                    await ws_manager.send_log(
                        f"[ARB] Found {len(yesno_arbs)} YES+NO arb: buy both sides for "
                        f"{yesno_arbs[0]['combined']}c → guaranteed {yesno_arbs[0]['profit_pct']}% profit",
                        "success",
                    )

                # Feature 10: Orderbook depth signals
                ob_signals = await _detect_orderbook_signals(poly_markets) if await _sig_scan_on("orderbook") else []
                if ob_signals:
                    # bid_ratio is share of depth on the bid side.
                    #   ≥75% → bids dominate → BUY YES signal.
                    #   ≤25% → asks dominate → BUY NO signal.
                    # Previously logged just the raw bid_ratio which looked wrong
                    # (e.g. "5.7% bid ratio" is actually a strong sell-side signal).
                    best = ob_signals[0]
                    br = best['bid_ratio']
                    if br >= 75:
                        desc = f"bids dominate {br:.0f}% → BUY YES"
                    else:
                        desc = f"asks dominate {100 - br:.0f}% → BUY NO"
                    await ws_manager.send_log(
                        f"[BOOK] Found {len(ob_signals)} orderbook imbalances (best: {desc})",
                        "warning",
                    )

                # Feature 11: Longshot bias exploitation
                longshot = _detect_longshot_signals(poly_markets)
                if longshot:
                    await ws_manager.send_log(
                        f"[LONGSHOT] Found {len(longshot)} overpriced longshots "
                        f"(best: YES at {longshot[0]['yes_price']}c, {longshot[0]['edge_pct']}% edge)",
                        "warning",
                    )

                # Signal 18: Result reversal lottery — gated on momentum (same reversion category)
                reversals = _detect_result_reversals(poly_markets) if await _sig_scan_on("momentum") else []
                if reversals:
                    await ws_manager.send_log(
                        f"[REVERSAL] {len(reversals)} sports result reversal "
                        f"(best: {reversals[0]['side']} '{reversals[0]['market'][:35]}' "
                        f"@ {reversals[0]['price']}c, crashed from {reversals[0]['crash_from']}c, "
                        f"{reversals[0]['crash_age_min']:.0f}min ago, EV +{reversals[0]['ev']}%)",
                        "warning",
                    )

                # Feature 12: Settlement harvester — RE-ENABLED
                harvested = await get_settled_bets()
                if harvested:
                    await ws_manager.send_log(
                        f"[HARVEST] {len(harvested)} settlement opportunities "
                        f"(best: {harvested[0].get('side')} @ {harvested[0].get('price')}c, "
                        f"{harvested[0].get('profit_pct', 0):.1f}% EV)",
                        "warning",
                    )

                # Feature 13: Resolution calendar sniper
                sniper = await _detect_resolution_sniper(poly_markets)
                if sniper:
                    await ws_manager.send_log(
                        f"[SNIPER] {len(sniper)} near-certain markets resolving within 48h "
                        f"(best: {sniper[0]['side']} at {sniper[0]['price']}c, {sniper[0]['days_left']:.0f}d left)",
                        "warning",
                    )

                # Signal 17: Final period momentum (fresh cross above 80c in last 24h)
                final_period = _detect_final_period_momentum(poly_markets)
                if final_period:
                    await ws_manager.send_log(
                        f"[FINAL] {len(final_period)} markets just crossed 80c with "
                        f"{final_period[0]['hours_left']:.0f}h left "
                        f"(best: {final_period[0]['current_price']}c → {final_period[0]['profit_pct']:.1f}% to resolution)",
                        "warning",
                    )

                # Feature 14: Market chain cascade
                chains = _detect_chain_signals(poly_markets)
                if chains:
                    await ws_manager.send_log(
                        f"[CHAIN] {len(chains)} lagging markets in {chains[0]['chain']} chain "
                        f"({chains[0]['lagging_by']}% behind, suggesting {chains[0]['side']})",
                        "warning",
                    )
        except Exception as e:
            logger.error(f"New signal scan error: {e}", exc_info=True)

    # Fetch leaderboard whales (every 6 hours, lightweight)
    await _fetch_leaderboard_wallets()

    # Self-learning calibration (every 30 min)
    try:
        from app.services.self_learner import run_calibration
        await run_calibration()
    except Exception as e:
        logger.debug(f"Self-learner error: {e}")

    # Opus deep research (every 4 hours — 6x/day)
    try:
        from app.services.ai_analyst import run_deep_research
        from app.services.polymarket_simulator import get_open_bets
        from app.services.weather_engine import get_weather_signals
        open_bets = await get_open_bets()
        await run_deep_research(
            open_bets=open_bets,
            mispricing_edges=list(_mispricing_cache),
            weather_signals=get_weather_signals(),  # Cached from last 30-min scan
            rapid_moves=list(_rapid_moves_cache),
        )
    except Exception as e:
        logger.debug(f"Deep research error: {e}")

    # Weather forecast signals (scans every 30 min internally)
    try:
        from app.services.weather_engine import scan_weather_markets
        await scan_weather_markets()
    except Exception as e:
        logger.error(f"Weather engine error: {e}", exc_info=True)

    # Always run wallet scan
    await _scan_wallets()

    # Run paper trading simulator: check resolutions then place new bets
    if settings.POLYMARKET_SIM_ENABLED:
        try:
            from app.services.polymarket_simulator import check_resolutions, auto_bet_on_signals
            await check_resolutions()
            await auto_bet_on_signals()
        except Exception as e:
            logger.error(f"Simulator error: {e}", exc_info=True)

        # v3.20.7 — the v3.16.2 live-resolution poller was removed with the
        # rest of the real-money executor layer. This is now a signals-only
        # bot; no real orders ever fire, so there's nothing to poll for.

        # v3.19.0 — Intelligence Officer pivot. Push high-conviction opportunities
        # to the user's phone via Telegram so they can act manually from their
        # non-geoblocked home/work IPs. Throttled, deduped, never raises.
        try:
            from app.services.opportunity_alerts import scan_and_alert
            await scan_and_alert()
        except Exception as e:
            logger.warning(f"Opportunity alerts scan error: {e}")
            await ws_manager.send_log(f"[SIM] Error: {e}", "error")

        # v3.20.4 — exit monitor. Walks all tracked positions (auto-registered
        # by _send_alert), polls current CLOB price, fires Telegram EXIT push
        # when target/stop/time-decay rules trigger. Cheap (one Gamma call per
        # open position, capped at 50/cycle) and completely independent of
        # the paper path — never raises.
        try:
            from app.services.exit_monitor import check_exit_triggers
            fired = await check_exit_triggers()
            if fired:
                await ws_manager.send_log(
                    f"[EXIT] Fired {fired} exit alert(s) this cycle", "brain"
                )
        except Exception as e:
            logger.warning(f"Exit monitor scan error: {e}")

    # Log overall system summary
    scored_wallets = sum(1 for ws in _wallet_scores.values() if ws["wins"] + ws["losses"] > 0)
    qualified_wallets = sum(1 for ws in _wallet_scores.values() if ws["win_rate"] >= 60 and ws["trades"] >= 10)
    await ws_manager.send_log(
        f"[SYSTEM] Polymarket: {len(_mispricing_cache)} edges | "
        f"{len(_wallet_scores)} wallets ({scored_wallets} scored, {qualified_wallets} qualified) | "
        f"{len(_copy_signals)} copy signals | "
        f"{len(_consensus)} market activity",
        "info"
    )


_preseed_done = False


async def _preseed_wallet_scores():
    """Pre-seed wallet scores from recently resolved markets.

    Solves the cold-start problem: instead of waiting 24-48h for live resolutions,
    fetch trade history from already-resolved markets and retroactively score wallets.
    """
    global _preseed_done
    if _preseed_done:
        return
    _preseed_done = True

    await ws_manager.send_log("[WALLETS] Pre-seeding wallet scores from resolved markets...", "info")
    loop = asyncio.get_running_loop()

    try:
        # Fetch recently resolved markets
        resp = await loop.run_in_executor(
            None,
            lambda: requests.get(
                POLYMARKET_MARKETS_API,
                params={"limit": 50, "closed": "true", "order": "volume24hr", "ascending": "false"},
                timeout=15,
                headers={"Accept": "application/json"},
            ),
        )
        resp.raise_for_status()
        resolved = resp.json() if isinstance(resp.json(), list) else []
    except Exception as e:
        logger.warning(f"Pre-seed: failed to fetch resolved markets: {e}")
        return

    if not resolved:
        return

    seeded = 0
    for market in resolved[:30]:
        slug = market.get("slug", "")
        question = market.get("question", "")
        if not slug or _is_noise(question):
            continue

        # Determine resolution
        try:
            prices = json.loads(market.get("outcomePrices", "[]"))
            yes_final = float(prices[0]) if prices else 0.5
        except (json.JSONDecodeError, ValueError, IndexError):
            continue

        if yes_final < 0.98 and yes_final > 0.02:
            continue  # Not clearly resolved

        # Fetch trades on this resolved market
        try:
            resp = await loop.run_in_executor(
                None,
                lambda s=slug: requests.get(
                    TRADES_API,
                    params={"market": s, "limit": 100},
                    timeout=10,
                    headers={"Accept": "application/json"},
                ),
            )
            trades = resp.json() if resp.status_code == 200 and isinstance(resp.json(), list) else []
        except Exception:
            continue

        # Score each wallet's trade against actual resolution
        for t in trades:
            wallet = t.get("proxyWallet", "")
            side = t.get("side", "")
            price = float(t.get("price", 0) or 0)
            size = float(t.get("size", 0) or 0)
            if not wallet or not side or size < 5 or price < 0.05 or price > 0.95:
                continue

            # Initialize wallet if new
            if wallet not in _wallet_scores:
                _wallet_scores[wallet] = {
                    "trades": 0, "wins": 0, "losses": 0, "pending": 0,
                    "win_rate": 0.0, "volume": 0.0, "last_seen": time.time(),
                    "markets": set(), "recent_trades": [], "buys": 0, "sells": 0,
                    "is_market_maker": False,
                }

            ws = _wallet_scores[wallet]
            ws["trades"] += 1
            ws["volume"] += size
            ws["markets"].add(question[:50])
            if side == "BUY":
                ws["buys"] += 1
            else:
                ws["sells"] += 1

            # Score against resolution
            resolved_yes = yes_final >= 0.98
            if side == "BUY" and resolved_yes:
                ws["wins"] += 1
            elif side == "BUY" and not resolved_yes:
                ws["losses"] += 1
            elif side == "SELL" and not resolved_yes:
                ws["wins"] += 1
            elif side == "SELL" and resolved_yes:
                ws["losses"] += 1

            total = ws["wins"] + ws["losses"]
            ws["win_rate"] = round(ws["wins"] / total * 100, 1) if total > 0 else 0.0
            ws["is_market_maker"] = _is_market_maker(ws)  # Compute after buys/sells tallied

        seeded += 1
        await asyncio.sleep(0.3)  # Rate limit

    scored = sum(1 for ws in _wallet_scores.values() if ws["wins"] + ws["losses"] > 0)
    qualified = sum(1 for ws in _wallet_scores.values() if ws["win_rate"] >= 65 and ws["trades"] >= 20)
    await ws_manager.send_log(
        f"[WALLETS] Pre-seeded from {seeded} resolved markets: "
        f"{len(_wallet_scores)} wallets, {scored} scored, {qualified} qualified",
        "success",
    )


async def _run_loop():
    """Background loop -- scans every 5 minutes.

    v3.22.1 — any unhandled exception from scan_all (Gamma/CLOB 5xx, JSON
    decode, asyncpg blip, dict-changed-size-during-iteration on the shared
    _wallet_scores caches) used to escape this loop and permanently kill
    the scanner — the process would stay up healthy but no signals would
    fire until the next redeploy. Wrap the body, log with traceback, and
    back off on error so we also don't tight-spin a failing API.
    """
    await asyncio.sleep(5)  # Initial delay

    # Pre-seed wallet scores on first run (solves cold-start)
    try:
        await _preseed_wallet_scores()
    except Exception:
        logger.exception("_preseed_wallet_scores failed; continuing without preseed")

    while True:
        try:
            if settings.POLYMARKET_WALLET_TRACKER_ENABLED:
                await scan_all()
            await asyncio.sleep(300)  # 5 min
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("scan_all crashed; backing off 60s and retrying")
            await asyncio.sleep(60)


# ══════════════════════════════════════════════════════════════
# PUBLIC API
# ══════════════════════════════════════════════════════════════

async def compute_walls_for_slug(slug: str, side: str = "YES") -> dict:
    """v3.20.0b — Wall analysis by market slug (resolves token_id via Gamma API
    then calls compute_walls). Convenience wrapper for callers that have a slug
    not a token_id (e.g. mispricing signals, opportunity alerts). Empty dict on
    any failure."""
    if not slug:
        return {}
    loop = asyncio.get_running_loop()
    try:
        resp = await loop.run_in_executor(
            None,
            lambda: requests.get(
                "https://gamma-api.polymarket.com/markets",
                params={"slug": slug},
                timeout=8,
            ),
        )
        if resp.status_code != 200:
            return {}
        data = resp.json()
        market = (data[0] if isinstance(data, list) and data else data) if data else None
        if not market:
            return {}
        raw = market.get("clobTokenIds", "")
        ids = json.loads(raw) if isinstance(raw, str) else raw
        if not ids or len(ids) < 2:
            return {}
        token_id = ids[0] if str(side).upper() == "YES" else ids[1]
        return await compute_walls(token_id)
    except Exception as e:
        logger.debug("compute_walls_for_slug failed for %s/%s: %s", slug, side, e)
        return {}


def get_mispricing() -> list[dict]:
    """Return mispriced markets (Polymarket vs Manifold)."""
    return list(_mispricing_cache)


def get_top_wallets(limit: int = 20) -> list[dict]:
    """Top wallets ranked by composite score (win_rate * sqrt(trades))."""
    ranked = _get_ranked_wallets()
    return ranked[:limit]


def get_whale_activity() -> list[dict]:
    """Recent big trades ($50+)."""
    return list(_big_trades)


def get_consensus() -> list[dict]:
    """Markets where multiple wallets agree -- strongest signals."""
    return list(_consensus)


def get_copy_signals() -> list[dict]:
    """Current copy signals from top-rated wallets."""
    return list(_copy_signals)


def get_wallet_trades(address: str, limit: int = 50) -> list[dict]:
    """Filter recent trades for a specific wallet."""
    return [
        t for t in _recent_trades
        if t.get("proxyWallet", "").lower() == address.lower()
    ][:limit]


def get_whale_activity_for_slug(slug: str, limit: int = 15) -> list[dict]:
    """v3.20.0c — recent activity from any tracked wallet on a specific
    market. Used by the per-market deep-dive page. Annotates each trade
    with whether the wallet is in MANUAL_WHALE_WALLETS or _leaderboard_wallets
    so the dashboard can show "tracked whale" badge."""
    if not slug:
        return []
    s = slug.lower()
    out = []
    manual = {w.lower() for w in MANUAL_WHALE_WALLETS}
    leaderboard = {w.lower() for w in (_leaderboard_wallets or set())}
    for t in _recent_trades:
        # Each trade record has slug or eventSlug (varies by source)
        trade_slug = (t.get("slug") or t.get("eventSlug") or "").lower()
        if trade_slug != s:
            continue
        wallet = (t.get("proxyWallet") or "").lower()
        out.append({
            "wallet": wallet,
            "side": t.get("side") or "?",
            "size_usd": float(t.get("usdcSize") or t.get("size") or 0) * float(t.get("price") or 0),
            "price": float(t.get("price") or 0),
            "timestamp": t.get("timestamp") or t.get("ts"),
            "is_manual_whale": wallet in manual,
            "is_leaderboard_whale": wallet in leaderboard,
        })
        if len(out) >= limit:
            break
    return out


def generate_market_insights(market: dict, walls: dict, whale_activity: list) -> list[str]:
    """v3.20.0c — auto-generate natural-language insights from market data,
    walls, and whale activity. Returns list of one-line strings for the
    deep-dive page's 'Signal Readout' section (Polymonit-style)."""
    insights = []
    if not walls and not whale_activity:
        return ["No deep data available for this market right now."]

    # Wall-based insights
    if walls:
        imb = walls.get("imbalance_pct", 0)
        top5 = walls.get("top5_pct_of_depth", 0)
        spread = walls.get("spread_cents")
        if abs(imb) >= 50:
            side_label = "buy-side support" if imb > 0 else "sell-side resistance"
            insights.append(f"{side_label.capitalize()} dominates — {abs(imb):.0f}% of visible depth on one side")
        if top5 >= 70:
            insights.append(f"Book is whale-concentrated — top 5 walls control {top5:.0f}% of depth")
        if spread is not None and spread <= 1.0:
            insights.append(f"Tight spread ({spread:.1f}c) — book is efficient for this outcome")
        elif spread is not None and spread >= 5.0:
            insights.append(f"Wide spread ({spread:.1f}c) — thin liquidity, slippage risk")
        biggest = (walls.get("top_walls") or [{}])[0]
        if biggest.get("usd", 0) >= 50000:
            side = biggest.get("side", "?")
            usd = biggest.get("usd", 0)
            price = biggest.get("price", 0)
            side_word = "Sell" if side == "ask" else "Buy"
            insights.append(f"Largest wall sits on {side_word} — ${usd:,.0f} rests at {price*100:.0f}c")

    # Whale activity insights
    if whale_activity:
        n_whales = len({w["wallet"] for w in whale_activity})
        sides = [w["side"] for w in whale_activity]
        buys = sides.count("BUY")
        sells = sides.count("SELL")
        if n_whales >= 3:
            if buys > sells * 2:
                insights.append(f"{n_whales} tracked whales recently — {buys} BUYs vs {sells} SELLs (smart money accumulating)")
            elif sells > buys * 2:
                insights.append(f"{n_whales} tracked whales recently — {sells} SELLs vs {buys} BUYs (smart money distributing)")
            else:
                insights.append(f"{n_whales} tracked whales recently — mixed flow ({buys} BUY / {sells} SELL)")

    if not insights:
        insights.append("Market structure looks normal — no extreme signals on book or whale flow.")
    return insights


def get_rapid_moves() -> list[dict]:
    """Return markets with rapid price movements (5%+ in 30 min)."""
    return list(_rapid_moves_cache)


def get_correlated_arbs() -> list[dict]:
    """Return correlated market arbitrage opportunities."""
    return list(_correlated_arbs_cache)


def get_overround_arbs() -> list[dict]:
    """Return outcome group overround opportunities."""
    return list(_overround_arbs_cache)


def get_momentum_signals() -> list[dict]:
    """Return mean-reversion / momentum signals."""
    return list(_momentum_signals_cache)


def get_news_speed_signals() -> list[dict]:
    """Return breaking news speed edge signals."""
    return list(_news_speed_signals_cache)


def get_yesno_arbs() -> list[dict]:
    """Return YES+NO arbitrage opportunities (guaranteed profit)."""
    return list(_yesno_arb_cache)


def get_orderbook_signals() -> list[dict]:
    """Return orderbook depth imbalance signals."""
    return list(_orderbook_signals_cache)


def get_longshot_signals() -> list[dict]:
    """Return longshot bias exploitation signals (sell overpriced YES)."""
    return list(_longshot_signals_cache)


def get_leaderboard_signals() -> list[dict]:
    """Return leaderboard whale trade signals."""
    return list(_leaderboard_signals_cache)


def get_resolution_sniper() -> list[dict]:
    """Return near-certain markets about to resolve."""
    return list(_resolution_sniper_cache)


def get_panic_fade_signals() -> list[dict]:
    """Return panic-fade signals (8-14% crash under 30c)."""
    return list(_panic_fade_cache)


def get_result_reversal_signals() -> list[dict]:
    """Return result-reversal lottery signals (sports crash to near-zero, still open)."""
    return list(_result_reversals_cache)


def get_final_period_signals() -> list[dict]:
    """Return final-period momentum signals (crossed above 80c in last 24h)."""
    return list(_final_period_cache)


def get_chain_signals() -> list[dict]:
    """Return market chain cascade signals."""
    return list(_chain_signals_cache)


def get_settled_bets() -> list[dict]:
    """Return settlement harvester opportunities (near-certain bets)."""
    return list(_settled_bets_cache)


# ══════════════════════════════════════════════════════════════
# FEATURE 15: CROSS-MARKET PROBABILITY VALIDATION
# ══════════════════════════════════════════════════════════════

# Conditional relationships: if A implies B, then P(A) <= P(B)
_CONDITIONAL_PAIRS = [
    # (broader market keywords, narrower market keywords, relationship)
    # "Trump wins 2028" requires "Trump wins nomination" → P(win) <= P(nomination)
    (["trump win", "trump 2028 president"], ["trump.*nomination", "trump.*republican"], "subset"),
    # "Ukraine ceasefire by April" implies "ceasefire by June" → P(april) <= P(june)
    (["ceasefire by april", "ceasefire.*april"], ["ceasefire by june", "ceasefire.*june"], "subset"),
    (["ceasefire by june"], ["ceasefire by december", "ceasefire.*december"], "subset"),
    # "Iran conflict ends by April" implies "ends by June"
    (["conflict ends by april", "conflict ends.*april"], ["conflict ends by june", "conflict ends.*june"], "subset"),
    # "Fed cuts in May" implies "Fed cuts in 2026"
    (["fed cut.*may", "rate cut.*may"], ["fed cut.*2026", "rate cut.*2026"], "subset"),
]


def _detect_conditional_mispricing(markets: list) -> list:
    """Find markets where conditional probability is violated.

    If P(Trump wins 2028) = 40% but P(Trump wins nomination) = 30%,
    that's impossible — you can't win without being nominated.
    P(win) MUST be <= P(nomination). If it isn't, there's an arb.
    """
    signals = []

    # Build price lookup
    market_prices: dict[str, tuple[float, dict]] = {}
    for m in markets:
        q = m.get("question", "").lower()
        p = get_yes_price(m)
        if p > 0.01:
            market_prices[q] = (p, m)

    for narrow_kws, broad_kws, rel in _CONDITIONAL_PAIRS:
        # Find matching markets
        narrow_matches = []
        broad_matches = []
        for q, (p, m) in market_prices.items():
            for kw in narrow_kws:
                if kw in q:
                    narrow_matches.append((q, p, m))
                    break
            for kw in broad_kws:
                if kw in q:
                    broad_matches.append((q, p, m))
                    break

        if not narrow_matches or not broad_matches:
            continue

        for nq, np, nm in narrow_matches:
            for bq, bp, bm in broad_matches:
                if rel == "subset" and np > bp + 0.05:
                    # Violation! Narrow event is priced HIGHER than broad event
                    # Narrow should be <= Broad. Bet NO on narrow or YES on broad.
                    violation = np - bp
                    signals.append({
                        "type": "conditional_violation",
                        "narrow_market": nm.get("question", "")[:60],
                        "narrow_slug": nm.get("slug", ""),
                        "narrow_price": round(np * 100, 1),
                        "broad_market": bm.get("question", "")[:60],
                        "broad_slug": bm.get("slug", ""),
                        "broad_price": round(bp * 100, 1),
                        "violation_pct": round(violation * 100, 1),
                        "side": "NO",  # Bet NO on the overpriced narrow market
                        "bet_slug": nm.get("slug", ""),
                        "bet_market": nm.get("question", "")[:80],
                        "bet_price": round((1 - np) * 100, 1),
                        "market_url": _market_url(nm.get("slug", "")),
                        "score": min(80, 40 + violation * 200),
                        "timestamp": time.time(),
                    })

    return signals


def get_tracker_status() -> dict:
    """Return current tracker status."""
    scored_count = sum(1 for ws in _wallet_scores.values() if ws["trades"] >= 10)
    mm_count = sum(1 for ws in _wallet_scores.values() if ws.get("is_market_maker", False))
    return {
        "enabled": _scanner_task is not None and not _scanner_task.done() if _scanner_task else False,
        "last_scan": _last_scan_time,
        "wallets_tracked": len(_wallet_scores),
        "wallets_scored": scored_count,
        "wallets_mm": mm_count,
        "activity_count": len(_big_trades),
        "consensus_count": len(_consensus),
        "mispricing_count": len(_mispricing_cache),
        "copy_signal_count": len(_copy_signals),
        "rapid_moves_count": len(_rapid_moves_cache),
        "correlated_arbs_count": len(_correlated_arbs_cache),
        "settled_bets_count": len(_settled_bets_cache),
        "mispricing_last_scan": _mispricing_last_scan,
        "error": _scan_error,
    }


def start_wallet_tracker():
    """Start background task."""
    global _scanner_task
    if _scanner_task and not _scanner_task.done():
        return
    _scanner_task = asyncio.create_task(_run_loop())
    logger.info("Copy-trading intelligence system v3 started (5-min interval)")
