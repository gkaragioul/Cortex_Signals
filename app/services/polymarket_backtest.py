"""
Polymarket Historical Backtest Engine

Fetches resolved markets from the last 30 days and retroactively tests
whether our scoring system would have been profitable.

Runs on-demand only (not every cycle) since it's API-intensive.
Results are stored in memory, not persisted to DB.
"""

import asyncio
import json
import logging
import time

import requests

from app.services.polymarket_wallets import (
    _score_signal,
    _extract_keywords,
    _match_markets,
    _match_kalshi,
    _is_noise,
    _estimate_resolution_type,
)
from app.services.websocket_manager import ws_manager

logger = logging.getLogger(__name__)

POLYMARKET_MARKETS_API = "https://gamma-api.polymarket.com/markets"
MANIFOLD_SEARCH_API = "https://api.manifold.markets/v0/search-markets"
KALSHI_MARKETS_API = "https://api.elections.kalshi.com/trade-api/v2/markets"

# ══════════════════════════════════════════════════════════════
# STATE
# ══════════════════════════════════════════════════════════════

_backtest_results: dict | None = None
_backtest_running: bool = False
_backtest_last_run: float = 0


# ══════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════

def _get_yes_price(market: dict) -> float | None:
    """Extract YES price from a market dict."""
    try:
        prices = json.loads(market.get("outcomePrices", "[]"))
        if prices and len(prices) >= 1:
            return float(prices[0])
    except (json.JSONDecodeError, ValueError, IndexError):
        pass
    return None


TRADES_API = "https://data-api.polymarket.com/trades"


async def _fetch_pre_resolution_price(slug: str, resolution: str) -> float | None:
    """Fetch actual trade prices from 1-7 days before resolution.

    Uses the Polymarket trades API to get real prices instead of guessing.
    Returns the median trade price, or None if no usable data.
    """
    if not slug:
        return None

    loop = asyncio.get_running_loop()
    try:
        resp = await loop.run_in_executor(
            None,
            lambda: requests.get(
                TRADES_API,
                params={"market": slug, "limit": 50},
                timeout=10,
                headers={"Accept": "application/json"},
            ),
        )
        if resp.status_code != 200:
            return None

        trades = resp.json()
        if not trades or not isinstance(trades, list):
            return None

        # Collect prices from trades, excluding extreme ones (near resolution)
        prices = []
        for t in trades:
            price = float(t.get("price", 0) or 0)
            if 0.10 <= price <= 0.90:
                prices.append(price)

        if not prices:
            # Fallback: use a conservative estimate based on resolution
            if resolution == "YES":
                return 0.65  # Assume market was leaning YES
            else:
                return 0.35  # Assume market was leaning NO

        # Median price is more robust than mean against outliers
        prices.sort()
        median = prices[len(prices) // 2]
        return round(median, 3)

    except Exception as e:
        logger.debug(f"Backtest: Failed to fetch trades for {slug}: {e}")
        return None


async def _fetch_resolved_markets(limit: int = 200) -> list[dict]:
    """Fetch recently resolved Polymarket markets sorted by volume."""
    loop = asyncio.get_running_loop()
    try:
        resp = await loop.run_in_executor(
            None,
            lambda: requests.get(
                POLYMARKET_MARKETS_API,
                params={
                    "closed": "true",
                    "limit": limit,
                    "order": "volume24hr",
                    "ascending": "false",
                },
                timeout=20,
                headers={"Accept": "application/json"},
            ),
        )
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else []
    except Exception as e:
        logger.error(f"Backtest: failed to fetch resolved markets: {e}")
        return []


async def _search_manifold(keyword: str) -> list[dict]:
    """Search Manifold Markets for matching markets."""
    loop = asyncio.get_running_loop()
    try:
        resp = await loop.run_in_executor(
            None,
            lambda: requests.get(
                MANIFOLD_SEARCH_API,
                params={"term": keyword, "limit": 10},
                timeout=10,
                headers={"Accept": "application/json"},
            ),
        )
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else []
    except Exception as e:
        logger.debug(f"Backtest: Manifold search failed for '{keyword}': {e}")
        return []


async def _fetch_kalshi_markets() -> list[dict]:
    """Fetch Kalshi markets for cross-reference."""
    loop = asyncio.get_running_loop()
    try:
        resp = await loop.run_in_executor(
            None,
            lambda: requests.get(
                KALSHI_MARKETS_API,
                params={"limit": 50, "status": "open"},
                timeout=10,
                headers={"Accept": "application/json"},
            ),
        )
        resp.raise_for_status()
        data = resp.json()
        markets = data.get("markets", [])
        return markets if isinstance(markets, list) else []
    except Exception as e:
        logger.debug(f"Backtest: Kalshi fetch failed: {e}")
        return []


# ══════════════════════════════════════════════════════════════
# BACKTEST ENGINE
# ══════════════════════════════════════════════════════════════

async def run_backtest(days: int = 30) -> dict:
    """Fetch resolved markets and simulate our scoring system retroactively.

    For each resolved market:
    - Determine if it resolved YES or NO
    - Estimate what the price was before resolution (heuristic)
    - Cross-reference against Manifold/Kalshi for mispricing
    - Score the signal using our composite scorer
    - Determine if we would have bet (score >= 30)
    - Determine if we would have won

    Returns a summary dict with stats and individual bet details.
    """
    global _backtest_results, _backtest_running, _backtest_last_run

    if _backtest_running:
        return {"error": "Backtest already running", "status": "running"}

    _backtest_running = True
    await ws_manager.send_log("[BACKTEST] Starting 30-day historical backtest...", "info")

    try:
        # Fetch resolved markets and Kalshi markets in parallel
        resolved_markets, kalshi_markets = await asyncio.gather(
            _fetch_resolved_markets(200),
            _fetch_kalshi_markets(),
        )

        if not resolved_markets:
            _backtest_running = False
            return {"error": "No resolved markets fetched", "status": "failed"}

        await ws_manager.send_log(
            f"[BACKTEST] Fetched {len(resolved_markets)} resolved markets, analyzing...",
            "info",
        )

        hypothetical_bets = []
        markets_analyzed = 0
        bet_amount = 20.0  # Fixed $20 bet for backtesting simplicity

        for market in resolved_markets:
            question = market.get("question", "")
            if not question or _is_noise(question):
                continue

            # Check resolution — API uses 'closed' + prices at 0/1 to indicate resolved
            # The 'resolved' field is often None even for closed markets
            try:
                prices = json.loads(market.get("outcomePrices", "[]"))
                yes_final = float(prices[0]) if prices else 0.5
            except (json.JSONDecodeError, ValueError, IndexError):
                continue

            # Determine resolution from final prices (1.0 = YES won, 0.0 = NO won)
            if yes_final >= 0.95:
                resolution = "YES"
            elif yes_final <= 0.05:
                resolution = "NO"
            else:
                continue  # Not clearly resolved

            # Skip extreme markets (resolved near 0% or 100% -- no edge opportunity)
            volume = float(market.get("volume", 0) or market.get("volume24hr", 0) or 0)
            if volume < 10000:
                continue

            markets_analyzed += 1

            # Fetch actual trade history to get a realistic pre-resolution price
            # Uses the Polymarket trades API to find the median price 1-7 days before close
            slug = market.get("slug", "")
            estimated_pre_price = await _fetch_pre_resolution_price(slug, resolution)
            if estimated_pre_price is None:
                continue

            # Skip if estimated price is too extreme
            if estimated_pre_price < 0.10 or estimated_pre_price > 0.90:
                continue

            # Cross-reference with Manifold
            keywords = _extract_keywords(question)
            if not keywords:
                continue

            search_term = " ".join(keywords)
            manifold_results = await _search_manifold(search_term)

            sources = []
            source_names = []
            manifold_prob = None
            kalshi_prob = None

            if manifold_results:
                match = _match_markets(question, manifold_results)
                if match:
                    mp = match.get("probability")
                    if mp is not None and 0.03 <= mp <= 0.97:
                        manifold_prob = mp
                        sources.append(mp)
                        source_names.append("Manifold")

            if kalshi_markets:
                kalshi_match = _match_kalshi(question, kalshi_markets)
                if kalshi_match:
                    yes_bid = kalshi_match.get("yes_bid_dollars") or kalshi_match.get("yes_bid")
                    yes_ask = kalshi_match.get("yes_ask_dollars") or kalshi_match.get("yes_ask")
                    if yes_bid is not None and yes_ask is not None:
                        try:
                            bid_f = float(yes_bid)
                            ask_f = float(yes_ask)
                            if bid_f > 1.0 or ask_f > 1.0:
                                kp = (bid_f + ask_f) / 200.0
                            else:
                                kp = (bid_f + ask_f) / 2.0
                            if 0.03 <= kp <= 0.97:
                                kalshi_prob = kp
                                sources.append(kp)
                                source_names.append("Kalshi")
                        except (ValueError, TypeError):
                            pass

            if not sources:
                continue

            # Composite fair value from other sources
            fair_value = sum(sources) / len(sources)
            edge = round((fair_value - estimated_pre_price) * 100, 1)

            if abs(edge) < 8.0:
                continue

            # Resolution type for time-decay scoring
            res_type = _estimate_resolution_type(question)
            if res_type["type"] == "sports":
                days_to_res = 0.2  # ~4 hours
            elif res_type["type"] == "crypto_price":
                days_to_res = 1.0
            elif res_type["type"] == "deadline":
                days_to_res = 15.0
            else:
                days_to_res = None

            # Score the signal
            score = _score_signal(
                edge_pct=abs(edge),
                sources_count=len(sources),
                wallet_win_rate=None,  # No wallet data in backtest
                days_to_resolution=days_to_res,
                has_news_catalyst=False,  # Can't check news retroactively
            )

            # Would we have bet? (score >= 30)
            would_bet = score >= 30

            if would_bet:
                # Determine our side: if edge > 0, we'd buy YES; if edge < 0, we'd buy NO
                our_side = "YES" if edge > 0 else "NO"
                won = (our_side == resolution)

                if won:
                    # Payout: shares * $1 - cost
                    entry_price = estimated_pre_price if our_side == "YES" else (1.0 - estimated_pre_price)
                    shares = bet_amount / entry_price
                    pnl = shares * 1.0 - bet_amount
                else:
                    pnl = -bet_amount

                hypothetical_bets.append({
                    "market": question[:100],
                    "slug": market.get("slug", ""),
                    "resolution": resolution,
                    "our_side": our_side,
                    "won": won,
                    "score": round(score, 1),
                    "edge": edge,
                    "estimated_price": round(estimated_pre_price * 100, 1),
                    "fair_value": round(fair_value * 100, 1),
                    "sources": source_names,
                    "bet_amount": bet_amount,
                    "pnl": round(pnl, 2),
                    "volume": volume,
                })

            # Rate-limit API calls
            await asyncio.sleep(0.3)

        # Calculate summary stats
        total_bets = len(hypothetical_bets)
        wins = sum(1 for b in hypothetical_bets if b["won"])
        losses = total_bets - wins
        win_rate = (wins / total_bets * 100) if total_bets > 0 else 0
        total_pnl = sum(b["pnl"] for b in hypothetical_bets)
        total_wagered = total_bets * bet_amount
        roi = (total_pnl / total_wagered * 100) if total_wagered > 0 else 0

        # Sort bets by P&L for best/worst
        hypothetical_bets.sort(key=lambda b: b["pnl"], reverse=True)

        results = {
            "status": "completed",
            "timestamp": time.time(),
            "days": days,
            "markets_analyzed": markets_analyzed,
            "total_bets": total_bets,
            "wins": wins,
            "losses": losses,
            "win_rate": round(win_rate, 1),
            "total_pnl": round(total_pnl, 2),
            "total_wagered": round(total_wagered, 2),
            "roi_pct": round(roi, 1),
            "bet_amount": bet_amount,
            "best_bets": hypothetical_bets[:10],
            "worst_bets": hypothetical_bets[-10:][::-1] if len(hypothetical_bets) >= 10 else [],
            "all_bets": hypothetical_bets,
            "score_distribution": {
                "30_40": sum(1 for b in hypothetical_bets if 30 <= b["score"] < 40),
                "40_50": sum(1 for b in hypothetical_bets if 40 <= b["score"] < 50),
                "50_60": sum(1 for b in hypothetical_bets if 50 <= b["score"] < 60),
                "60_plus": sum(1 for b in hypothetical_bets if b["score"] >= 60),
            },
        }

        _backtest_results = results
        _backtest_last_run = time.time()

        pnl_sign = "+" if total_pnl >= 0 else ""
        await ws_manager.send_log(
            f"[BACKTEST] 30-day backtest: {total_bets} hypothetical bets, "
            f"{win_rate:.0f}% win rate, {pnl_sign}${total_pnl:.0f} P&L, "
            f"{roi:.1f}% ROI ({markets_analyzed} markets analyzed)",
            "success" if total_pnl >= 0 else "warning",
        )

        return results

    except Exception as e:
        logger.error(f"Backtest error: {e}", exc_info=True)
        await ws_manager.send_log(f"[BACKTEST] Error: {e}", "error")
        return {"error": str(e), "status": "failed"}

    finally:
        _backtest_running = False


# ══════════════════════════════════════════════════════════════
# PUBLIC API
# ══════════════════════════════════════════════════════════════

def get_backtest_results() -> dict | None:
    """Return cached backtest results (None if never run)."""
    return _backtest_results


def is_backtest_running() -> bool:
    """Check if a backtest is currently in progress."""
    return _backtest_running


async def _backtest_loop():
    """Auto-run backtest on startup and every 24 hours."""
    await asyncio.sleep(30)  # Wait for other systems to initialize

    while True:
        try:
            if not _backtest_running:
                logger.info("Auto-backtest: starting scheduled 30-day backtest")
                await ws_manager.send_log("[BACKTEST] Auto-running 30-day backtest...", "info")
                await run_backtest(30)
        except Exception as e:
            logger.error(f"Auto-backtest error: {e}")
        await asyncio.sleep(86400)  # 24 hours


_backtest_task = None

def start_backtest_loop():
    """Start the auto-backtest background task."""
    global _backtest_task
    if _backtest_task and not _backtest_task.done():
        return
    _backtest_task = asyncio.create_task(_backtest_loop())
    logger.info("Backtest auto-loop started (runs on startup + every 24h)")
