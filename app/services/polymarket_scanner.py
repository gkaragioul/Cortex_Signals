# SPDX-License-Identifier: MIT

"""
Polymarket Intelligence Scanner v3 — Full trader-grade analysis system.

Tier 1: Market quality filters (broken odds, liquidity, expiry, odds movement)
Tier 2: Real data sources (sports stats, polling, FRED economics)
Tier 3: Kelly criterion sizing, multi-pass analysis, bankroll tracking, auto-resolution

Runs as async background task. Does NOT import from crypto trading services.
"""

import asyncio
import json
import logging
import math
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from urllib.parse import quote_plus

import requests
import anthropic

from app.config import settings
from app.services.websocket_manager import ws_manager

logger = logging.getLogger(__name__)

# ── Cache ──
_cache: list[dict] = []
_cache_ts: float = 0

# ── Scanner state ──
_last_scan_time: float = 0
_next_scan_time: float = 0
_markets_analyzed: int = 0
_scanner_task: asyncio.Task | None = None
_scan_progress: dict = {"scanning": False, "current": 0, "total": 0, "current_market": ""}
_enabled: bool = True

# ── Historical tracking ──
_prediction_history: list[dict] = []

# ── Cost tracking ──
_session_cost: float = 0.0
_session_calls: int = 0

# ── Bankroll tracking (Tier 3) ──
_bankroll: float = 1000.0  # Hypothetical starting bankroll
_bankroll_history: list[dict] = []  # [{timestamp, bankroll, action, market}]

# ── Odds history for movement detection (Tier 1) ──
_odds_history: dict = {}  # slug -> [{timestamp, yes_price}]

# ── Category data cache (Tier 2) — ESPN/FRED/RCP don't change hourly ──
_category_data_cache: dict = {}  # category -> {"data": str, "ts": float}
_CATEGORY_CACHE_TTL = 14400  # 4 hours


# ══════════════════════════════════════════════════════════════
# PUBLIC API (called by router)
# ══════════════════════════════════════════════════════════════

def get_cached_recommendations() -> list[dict]:
    """Return cached recommendations, auto-removing any that have resolved since last scan."""
    if not _cache:
        return []

    # Quick check: re-validate cached picks against Polymarket API
    # This runs on every dashboard poll (~60s) but is lightweight (1 API call)
    valid = []
    for rec in _cache:
        slug = rec.get("market_url", "").split("/")[-1]
        if slug and slug in _resolved_slugs:
            continue  # Already known to be resolved
        valid.append(rec)
    return valid


# ── Track resolved slugs to filter stale picks between scans ──
_resolved_slugs: set = set()


def get_scanner_status() -> dict:
    return {
        "enabled": _enabled and settings.POLYMARKET_ENABLED,
        "last_scan": _last_scan_time,
        "next_scan": _next_scan_time,
        "markets_analyzed": _markets_analyzed,
        "progress": dict(_scan_progress),
    }


def get_cost_summary() -> dict:
    try:
        from app.services.ai_analyst import _daily_calls, _daily_opus_calls, MAX_DAILY_HAIKU_CALLS, MAX_DAILY_OPUS_CALLS
        haiku_cost = round(_daily_calls * 0.0002, 4)
        opus_cost  = round(_daily_opus_calls * 0.03, 4)
        return {
            "session_cost": round(_session_cost, 4),
            "session_calls": _session_calls,
            "haiku_calls_today": _daily_calls,
            "haiku_calls_max": MAX_DAILY_HAIKU_CALLS,
            "sonnet_calls_today": _daily_opus_calls,
            "sonnet_calls_max": MAX_DAILY_OPUS_CALLS,
            "estimated_cost_usd": round(haiku_cost + opus_cost, 4),
            "budget_usd": 0.50,
        }
    except Exception:
        return {"session_cost": round(_session_cost, 4), "session_calls": _session_calls}


def get_bankroll_summary() -> dict:
    return {
        "bankroll": round(_bankroll, 2),
        "starting": 1000.0,
        "pnl": round(_bankroll - 1000.0, 2),
        "pnl_pct": round((_bankroll - 1000.0) / 1000.0 * 100, 1),
        "total_bets": len([h for h in _bankroll_history if h.get("action") == "bet"]),
        "resolved": len([h for h in _bankroll_history if h.get("action") in ("win", "loss")]),
    }


def toggle_scanner() -> bool:
    global _enabled
    _enabled = not _enabled
    logger.info(f"Polymarket scanner {'ENABLED' if _enabled else 'DISABLED'} by user")
    # Fire-and-forget log to dashboard (safe to ignore if event loop not running)
    try:
        asyncio.get_event_loop().create_task(
            ws_manager.send_log(f"[POLYMARKET] Scanner {'ENABLED' if _enabled else 'DISABLED'} by user",
                                "success" if _enabled else "warning"))
    except Exception:
        pass
    return _enabled


# ══════════════════════════════════════════════════════════════
# TIER 1: MARKET QUALITY FILTERS
# ══════════════════════════════════════════════════════════════

async def _fetch_markets() -> list[dict]:
    """Fetch and pre-filter markets from Polymarket Gamma API."""
    url = "https://gamma-api.polymarket.com/markets"
    params = {"active": "true", "limit": 200, "order": "volume24hr", "ascending": "false"}
    try:
        loop = asyncio.get_event_loop()
        resp = await loop.run_in_executor(
            None, lambda: requests.get(url, params=params, timeout=15))
        resp.raise_for_status()
        markets = resp.json()
        filtered = _apply_quality_filters(markets)
        logger.info(f"Polymarket: {len(markets)} raw → {len(filtered)} after quality filters")
        await ws_manager.send_log(f"[POLYMARKET] Fetched {len(filtered)} markets (filtered from {len(markets)})", "info")
        return filtered
    except Exception as e:
        logger.error(f"Polymarket fetch failed: {e}")
        await ws_manager.send_log(f"[POLYMARKET] Market fetch FAILED: {e}", "error")
        return []


def _apply_quality_filters(markets: list[dict]) -> list[dict]:
    """Tier 1: Remove garbage markets that would produce false signals."""
    filtered = []
    for m in markets:
        vol = float(m.get("volume24hr") or m.get("volume24Hr") or 0)
        question = m.get("question", "")

        # Filter 0: Reject already-resolved markets
        if m.get("resolved") or m.get("closed"):
            logger.debug(f"Filtered out '{question[:40]}': already resolved")
            continue

        # Filter 1: Volume filter — category-aware
        # Sports/esports at low volume = garbage. But tech/legal/weather at $1K can be legit.
        category = _classify_market(question)
        low_vol_ok = category in ("tech", "entertainment", "legal", "weather",
                                   "commodities", "politics_us", "politics_intl",
                                   "economics", "geopolitics", "general")
        min_vol = 1_000 if low_vol_ok else 5_000
        if vol < min_vol:
            continue

        # Filter 2: Extract odds, reject broken markets
        try:
            outcome_prices = json.loads(m.get("outcomePrices", "[]"))
            yes_price = float(outcome_prices[0]) * 100 if outcome_prices else -1
        except (ValueError, IndexError, TypeError):
            yes_price = -1

        if yes_price < 0:
            continue

        # Filter 3: Reject 95%+ or 5%- odds (almost certainly resolved or data artifacts)
        if yes_price >= 95 or yes_price <= 5:
            logger.debug(f"Filtered out '{question[:40]}': odds {yes_price:.0f}% (too extreme)")
            continue

        # Filter 4: Reject markets expiring in < 2 hours (no time for edge to materialize)
        end_date = m.get("endDate") or m.get("end_date_iso", "")
        if end_date:
            try:
                end_dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
                hours_to_expiry = (end_dt - datetime.now(timezone.utc)).total_seconds() / 3600
                if hours_to_expiry < 2:
                    logger.debug(f"Filtered out '{question[:40]}': expires in {hours_to_expiry:.1f}h")
                    continue
                m["_hours_to_expiry"] = round(hours_to_expiry, 1)
            except Exception:
                m["_hours_to_expiry"] = None
        else:
            m["_hours_to_expiry"] = None

        # Filter 5: Minimum liquidity (volume relative to price — rough proxy)
        # Markets with >$50K volume are generally liquid enough
        m["_volume_24h"] = vol
        m["_yes_price"] = yes_price

        filtered.append(m)

    return filtered[:30]  # Cap at 30 to control API costs


def _track_odds_movement(slug: str, yes_price: float) -> dict:
    """Track odds over time and detect sharp movements."""
    global _odds_history
    now = time.time()

    if slug not in _odds_history:
        _odds_history[slug] = []

    _odds_history[slug].append({"ts": now, "price": yes_price})
    # Keep only last 24h of history
    _odds_history[slug] = [h for h in _odds_history[slug] if now - h["ts"] < 86400]

    history = _odds_history[slug]
    movement = {"change_1h": 0.0, "change_6h": 0.0, "sharp_move": False}

    for h in history:
        age_hours = (now - h["ts"]) / 3600
        if age_hours <= 1.5:
            movement["change_1h"] = yes_price - h["price"]
        if age_hours <= 7:
            movement["change_6h"] = yes_price - h["price"]

    # Sharp move = >10% change in 6 hours
    movement["sharp_move"] = abs(movement["change_6h"]) > 10
    return movement


# ══════════════════════════════════════════════════════════════
# TIER 2: REAL DATA SOURCES
# ══════════════════════════════════════════════════════════════

async def _fetch_category_data(question: str, category: str) -> str:
    """Fetch real data based on market category, with 4-hour cache."""
    now = time.time()
    cache_key = f"{category}:{question[:30]}"
    cached = _category_data_cache.get(cache_key)
    if cached and (now - cached["ts"]) < _CATEGORY_CACHE_TTL:
        return cached["data"]

    if category == "sports":
        data = await _fetch_sports_data(question)
    elif category == "economics":
        data = await _fetch_economics_data(question)
    elif category in ("politics_us", "politics_intl"):
        data = await _fetch_polling_data(question)
    elif category == "tech":
        data = await _fetch_tech_data(question)
    elif category == "commodities":
        data = await _fetch_commodities_data(question)
    elif category == "weather":
        data = await _fetch_weather_data(question)
    else:
        data = ""

    if data:
        _category_data_cache[cache_key] = {"data": data, "ts": now}
    return data


async def _fetch_sports_data(question: str) -> str:
    """Fetch sports stats from free APIs."""
    loop = asyncio.get_event_loop()
    data_parts = []

    # Extract team names from question
    q = question.lower()

    # Try ESPN API for current scores/standings (free, no key needed)
    try:
        # NBA
        if any(w in q for w in ["nba", "lakers", "celtics", "warriors", "bulls", "hawks",
                                  "kings", "raptors", "pacers", "magic", "nets", "knicks",
                                  "bucks", "heat", "sixers", "suns", "nuggets", "thunder",
                                  "cavs", "wolves", "mavs", "grizzlies", "pelicans", "spurs",
                                  "blazers", "jazz", "hornets", "wizards", "pistons", "rockets"]):
            url = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"
            resp = await loop.run_in_executor(None, lambda: requests.get(url, timeout=8))
            if resp.status_code == 200:
                espn = resp.json()
                events = espn.get("events", [])
                for event in events[:5]:
                    name = event.get("name", "")
                    status = event.get("status", {}).get("type", {}).get("description", "")
                    competitors = event.get("competitions", [{}])[0].get("competitors", [])
                    scores = " vs ".join(
                        f"{c.get('team', {}).get('abbreviation', '?')} {c.get('score', '?')} ({c.get('records', [{}])[0].get('summary', '?') if c.get('records') else '?'})"
                        for c in competitors)
                    data_parts.append(f"ESPN: {name} — {status} — {scores}")

        # NFL
        elif any(w in q for w in ["nfl", "chiefs", "eagles", "cowboys", "49ers", "bills",
                                    "ravens", "lions", "bengals", "dolphins", "jets"]):
            url = "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard"
            resp = await loop.run_in_executor(None, lambda: requests.get(url, timeout=8))
            if resp.status_code == 200:
                espn = resp.json()
                for event in espn.get("events", [])[:5]:
                    name = event.get("name", "")
                    data_parts.append(f"ESPN: {name}")
    except Exception as e:
        logger.debug(f"ESPN fetch failed: {e}")

    return "\n".join(data_parts) if data_parts else ""


async def _fetch_economics_data(question: str) -> str:
    """Fetch economic data from FRED API (free, 120 requests/min)."""
    loop = asyncio.get_event_loop()
    data_parts = []
    q = question.lower()

    # FRED series IDs for common economic indicators
    series_map = {
        "fed": "FEDFUNDS",           # Federal Funds Rate
        "rate": "FEDFUNDS",
        "inflation": "CPIAUCSL",      # CPI
        "cpi": "CPIAUCSL",
        "unemployment": "UNRATE",     # Unemployment Rate
        "gdp": "GDP",                # GDP
        "recession": "SAHMREALTIME",  # Sahm Rule Recession Indicator
    }

    for keyword, series_id in series_map.items():
        if keyword in q:
            try:
                url = f"https://api.stlouisfed.org/fred/series/observations"
                params = {
                    "series_id": series_id,
                    "api_key": "DEMO_KEY",  # FRED demo key (limited but works)
                    "file_type": "json",
                    "sort_order": "desc",
                    "limit": 5,
                }
                resp = await loop.run_in_executor(
                    None, lambda: requests.get(url, params=params, timeout=8))
                if resp.status_code == 200:
                    observations = resp.json().get("observations", [])
                    for obs in observations[:3]:
                        data_parts.append(
                            f"FRED {series_id}: {obs.get('date')} = {obs.get('value')}")
            except Exception as e:
                logger.debug(f"FRED fetch failed for {series_id}: {e}")
            break  # Only fetch one series per market

    return "\n".join(data_parts) if data_parts else ""


async def _fetch_polling_data(question: str) -> str:
    """Fetch polling/election data from RSS feeds."""
    loop = asyncio.get_event_loop()
    data_parts = []

    # RealClearPolitics RSS for US politics
    try:
        url = "https://www.realclearpolitics.com/index.xml"
        resp = await loop.run_in_executor(
            None, lambda: requests.get(url, timeout=8, headers={
                "User-Agent": "Mozilla/5.0 (compatible; GKBot/1.0)"}))
        if resp.status_code == 200:
            root = ET.fromstring(resp.content)
            items = root.findall(".//item")[:5]
            for item in items:
                title = item.find("title")
                if title is not None and title.text:
                    data_parts.append(f"RCP: {title.text.strip()}")
    except Exception as e:
        logger.debug(f"RCP fetch failed: {e}")

    return "\n".join(data_parts) if data_parts else ""


async def _fetch_tech_data(question: str) -> str:
    """Fetch tech/AI news from Hacker News top stories (free API)."""
    loop = asyncio.get_event_loop()
    data_parts = []
    try:
        # Hacker News top stories API (free, no key)
        resp = await loop.run_in_executor(
            None, lambda: requests.get("https://hacker-news.firebaseio.com/v0/topstories.json", timeout=8))
        if resp.status_code == 200:
            story_ids = resp.json()[:10]
            for sid in story_ids[:5]:
                sr = await loop.run_in_executor(
                    None, lambda s=sid: requests.get(f"https://hacker-news.firebaseio.com/v0/item/{s}.json", timeout=5))
                if sr.status_code == 200:
                    story = sr.json()
                    title = story.get("title", "")
                    score = story.get("score", 0)
                    if title:
                        data_parts.append(f"HN ({score}pts): {title}")
    except Exception as e:
        logger.debug(f"HN fetch failed: {e}")
    return "\n".join(data_parts) if data_parts else ""


async def _fetch_commodities_data(question: str) -> str:
    """Fetch commodity prices from FRED (oil, gold, etc.)."""
    loop = asyncio.get_event_loop()
    data_parts = []
    q = question.lower()

    series_map = {
        "oil": "DCOILWTICO",      # WTI Crude Oil
        "crude": "DCOILWTICO",
        "wti": "DCOILWTICO",
        "brent": "DCOILBRENTEU",  # Brent Crude
        "gold": "GOLDAMGBD228NLBM",  # Gold Price
        "silver": "SLVPRUSD",     # Silver Price
        "natural gas": "DHHNGSP",  # Henry Hub Natural Gas
        "copper": "PCOPPUSDM",    # Copper Price
    }

    for keyword, series_id in series_map.items():
        if keyword in q:
            try:
                url = "https://api.stlouisfed.org/fred/series/observations"
                params = {
                    "series_id": series_id,
                    "api_key": "DEMO_KEY",
                    "file_type": "json",
                    "sort_order": "desc",
                    "limit": 5,
                }
                resp = await loop.run_in_executor(
                    None, lambda: requests.get(url, params=params, timeout=8))
                if resp.status_code == 200:
                    observations = resp.json().get("observations", [])
                    for obs in observations[:3]:
                        data_parts.append(f"FRED {series_id}: {obs.get('date')} = ${obs.get('value')}")
            except Exception as e:
                logger.debug(f"FRED commodity fetch failed: {e}")
            break
    return "\n".join(data_parts) if data_parts else ""


async def _fetch_weather_data(question: str) -> str:
    """Fetch weather data from NWS API (free, no key needed)."""
    loop = asyncio.get_event_loop()
    data_parts = []
    try:
        # National Weather Service alerts (US) — free API
        resp = await loop.run_in_executor(
            None, lambda: requests.get(
                "https://api.weather.gov/alerts/active?status=actual&severity=Extreme,Severe",
                timeout=8, headers={"User-Agent": "GKBot/1.0"}))
        if resp.status_code == 200:
            alerts = resp.json().get("features", [])[:5]
            for alert in alerts:
                props = alert.get("properties", {})
                headline = props.get("headline", "")
                severity = props.get("severity", "")
                if headline:
                    data_parts.append(f"NWS {severity}: {headline}")
    except Exception as e:
        logger.debug(f"NWS fetch failed: {e}")
    return "\n".join(data_parts) if data_parts else ""


# ══════════════════════════════════════════════════════════════
# NEWS FETCHING (kept from v2)
# ══════════════════════════════════════════════════════════════

async def _fetch_news_deep(question: str) -> dict:
    """Multi-source news with full article extraction."""
    loop = asyncio.get_event_loop()
    result = {"headlines": [], "full_text": "", "sources": []}

    # Google News RSS
    try:
        query = quote_plus(question[:80])
        url = f"https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"
        resp = await loop.run_in_executor(None, lambda: requests.get(url, timeout=10))
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        for item in root.findall(".//item")[:5]:
            title = item.find("title")
            link = item.find("link")
            source = item.find("source")
            if title is not None and title.text:
                result["headlines"].append(title.text.strip())
                if source is not None and source.text:
                    result["sources"].append(source.text.strip())
                if not result["full_text"] and link is not None and link.text:
                    result["full_text"] = await _extract_article_text(link.text)
    except Exception:
        pass

    # Bing News RSS
    try:
        query = quote_plus(question[:80])
        url = f"https://www.bing.com/news/search?q={query}&format=rss&count=5"
        resp = await loop.run_in_executor(
            None, lambda: requests.get(url, timeout=10,
                                        headers={"User-Agent": "Mozilla/5.0 (compatible; GKBot/1.0)"}))
        if resp.status_code == 200:
            root = ET.fromstring(resp.content)
            for item in root.findall(".//item")[:3]:
                title = item.find("title")
                if title is not None and title.text:
                    h = title.text.strip()
                    if h not in result["headlines"]:
                        result["headlines"].append(h)
                        result["sources"].append("Bing News")
    except Exception:
        pass

    if not result["headlines"]:
        result["headlines"] = ["No recent news found"]
    return result


async def _extract_article_text(url: str) -> str:
    """Extract readable text from article URL."""
    try:
        loop = asyncio.get_event_loop()
        resp = await loop.run_in_executor(
            None, lambda: requests.get(url, timeout=8,
                                        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
                                        allow_redirects=True))
        if resp.status_code != 200:
            return ""
        html = resp.text
        html = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
        html = re.sub(r'<style[^>]*>.*?</style>', '', html, flags=re.DOTALL | re.IGNORECASE)
        html = re.sub(r'<[^>]+>', ' ', html)
        text = re.sub(r'\s+', ' ', html).strip()
        sentences = [s.strip() for s in text.split('.') if len(s.strip()) > 40]
        return ('. '.join(sentences[:20]) + '.')[:2000]
    except Exception:
        return ""


# ══════════════════════════════════════════════════════════════
# MARKET CLASSIFICATION & PROMPTS (enhanced from v2)
# ══════════════════════════════════════════════════════════════

def _classify_market(question: str) -> str:
    q = question.lower()
    if any(w in q for w in ["election", "president", "congress", "senate", "governor",
                             "vote", "poll", "democrat", "republican", "trump", "biden",
                             "primary", "nominee", "electoral", "midterm"]):
        return "politics_us"
    if any(w in q for w in ["parliament", "minister", "coalition", "eu ", "brexit",
                             "macron", "orban", "starmer", "modi", "prime minister",
                             "seats", "fidesz", "labour", "tory"]):
        return "politics_intl"
    if any(w in q for w in ["nba", "nfl", "mlb", "nhl", "soccer", "football",
                             "vs.", "vs ", "game", "match", "series", "playoff",
                             "championship", "world cup", "super bowl", "finals",
                             "tennis", "ufc", "boxing", "f1", "grand prix"]):
        return "sports"
    if any(w in q for w in ["bitcoin", "ethereum", "crypto", "btc", "eth", "token",
                             "solana", "defi", "blockchain", "nft", "memecoin"]):
        return "crypto"
    if any(w in q for w in ["war", "military", "invasion", "forces", "attack",
                             "iran", "ukraine", "russia", "china", "taiwan", "nato",
                             "ceasefire", "sanctions", "missile", "troops", "regime"]):
        return "geopolitics"
    if any(w in q for w in ["fed", "rate", "inflation", "gdp", "recession",
                             "unemployment", "tariff", "s&p", "nasdaq", "dow",
                             "treasury", "yield", "cpi", "jobs report", "fomc"]):
        return "economics"
    if any(w in q for w in ["ai ", "artificial intelligence", "openai", "google",
                             "apple", "microsoft", "meta", "nvidia", "chatgpt",
                             "agi", "launch", "release", "product", "tech"]):
        return "tech"
    if any(w in q for w in ["movie", "oscar", "grammy", "emmy", "album", "box office",
                             "streaming", "netflix", "disney", "spotify", "tiktok",
                             "elon", "musk", "tweet", "post", "celebrity"]):
        return "entertainment"
    if any(w in q for w in ["supreme court", "ruling", "trial", "verdict", "lawsuit",
                             "indictment", "guilty", "convicted", "judge", "legal",
                             "extradition", "prosecution"]):
        return "legal"
    if any(w in q for w in ["hurricane", "earthquake", "temperature", "climate",
                             "weather", "wildfire", "flood", "drought", "storm"]):
        return "weather"
    if any(w in q for w in ["oil", "crude", "gold", "silver", "commodity",
                             "wti", "brent", "natural gas", "wheat", "copper"]):
        return "commodities"
    return "general"


def _get_system_prompt(category: str) -> str:
    base = (
        "You are an expert prediction market trader managing real money. "
        "Estimate the TRUE probability independently from market consensus. "
        "Respond with ONLY a JSON object: "
        '{"probability": <number 0-100>, "analysis": "<2-3 sentence explanation>", '
        '"confidence": <number 1-10>, "key_factors": ["<factor1>", "<factor2>"]}. '
        "No markdown, no extra text."
    )
    specs = {
        "politics_us": f"{base}\n\nUS politics specialist. Weight polling data over punditry. Consider historical base rates, incumbency, approval ratings. If given FRED or RCP data, prioritize it over news headlines. Primary vs general elections have very different dynamics. Swing state polls matter more than national polls.",
        "politics_intl": f"{base}\n\nInternational politics specialist. Consider country-specific electoral systems (proportional vs FPTP), coalition math, and local polling. Don't apply US assumptions. Parliamentary systems produce coalition governments — no party wins alone. Weight local language media over English-language coverage.",
        "sports": f"{base}\n\nSports analytics specialist. If given ESPN data (records, scores), use it directly. Home advantage: ~57% NBA, ~57% NFL, ~54% soccer. No game is 100% certain. Markets showing extreme odds on regular-season games are always wrong. Consider rest days, back-to-backs, injuries, and travel schedule. Playoff vs regular season matters enormously.",
        "crypto": f"{base}\n\nCrypto specialist. Consider macro (Fed, DXY), BTC dominance cycle, halving cycles, on-chain metrics if provided. Predictions >1 week are highly uncertain. Crypto is driven by narratives and liquidity more than fundamentals. Weekend and Asian session liquidity differs from US session.",
        "geopolitics": f"{base}\n\nGeopolitical analyst. Most military threats don't materialize (base rate <20%). Weight official statements and logistics over media speculation. Markets overweight dramatic scenarios. Consider: does the actor have the logistical capability? Is there congressional/parliamentary authorization? What are the off-ramps? Escalation ladders have many rungs before war.",
        "economics": f"{base}\n\nEconomic analyst. If given FRED data, use it as primary input. Fed moves slowly and telegraphs via dot plots, minutes, and speeches. Weight CME FedWatch tool probabilities and bond market pricing over pundit predictions. Labor market leads recession by 6-12 months. Consider leading indicators (ISM, PMI, initial claims) over lagging (CPI, GDP).",
        "tech": f"{base}\n\nTechnology industry analyst. Consider company earnings calendars, product launch patterns, regulatory timelines, and competitive dynamics. Tech announcements are often leaked 1-2 weeks early. Apple follows predictable seasonal patterns (WWDC June, iPhone September). AI capabilities are frequently overhyped on short timelines — 'by end of year' predictions for AGI/major breakthroughs usually fail. Weight company official statements and SEC filings over rumors.",
        "entertainment": f"{base}\n\nEntertainment industry analyst. For awards: consider precursor awards (Golden Globes predict Oscars ~70%), critic consensus, box office performance, and campaign spending. For social media predictions (tweet counts, etc.): consider historical posting patterns and recent trends. Celebrity behavior is unpredictable — widen uncertainty bands. For box office: opening weekend is predictable from tracking data, but legs (total gross) depend on word-of-mouth.",
        "legal": f"{base}\n\nLegal analyst. Consider: the specific jurisdiction, judge's track record, legal precedent, strength of evidence as reported. Federal vs state proceedings have different timelines. Grand jury indictments are highly likely once convened (>95%). Trial outcomes are harder to predict — base rate for federal conviction is ~90%. Appeals take years. Distinguish between 'will charges be filed' (high certainty) vs 'will they be convicted' (lower certainty) vs 'will they serve time' (lowest certainty).",
        "weather": f"{base}\n\nClimate and weather analyst. Short-term forecasts (<7 days) are highly reliable. Seasonal predictions are moderately reliable. Specific temperature records depend on measurement station and methodology. Hurricane season predictions have wide error bars. Climate trend predictions (warmest year ever) can be assessed from NOAA/NASA baseline data. Weight official meteorological agencies (NWS, ECMWF) over media headlines.",
        "commodities": f"{base}\n\nCommodities analyst. Oil prices are driven by OPEC+ decisions, geopolitical supply risk, and demand forecasts. Gold correlates inversely with real yields and USD strength. Consider seasonal patterns (heating oil winter, gasoline summer). Supply disruptions cause sharp spikes but usually revert. Weight futures curve (contango vs backwardation) as the market's best forecast. Inventory reports (EIA, API) move prices weekly.",
    }
    return specs.get(category, base)


# ══════════════════════════════════════════════════════════════
# TIER 3: MULTI-PASS ANALYSIS
# ══════════════════════════════════════════════════════════════

async def _analyze_multi_pass(question: str, yes_price: float, news_data: dict,
                               category: str, category_data: str,
                               odds_movement: dict) -> dict | None:
    """Run Claude twice with different perspectives, only recommend when both agree."""
    if not settings.ANTHROPIC_API_KEY:
        return None

    # Build context
    headlines = "\n".join(f"- {h}" for h in news_data["headlines"][:8])
    full_text = news_data.get("full_text", "")
    sources = ", ".join(set(news_data.get("sources", [])))
    calibration = _get_calibration_note()

    context = (
        f"**Question:** {question}\n"
        f"**Market odds:** {yes_price:.0f}% YES\n"
        f"**Category:** {category}\n"
    )
    if odds_movement.get("sharp_move"):
        context += f"**ALERT: Sharp odds movement detected:** {odds_movement['change_6h']:+.1f}% in 6h\n"
    if odds_movement.get("change_1h"):
        context += f"**1h odds change:** {odds_movement['change_1h']:+.1f}%\n"

    context += f"\n## News Headlines\n{headlines}\nSources: {sources or 'Various'}\n"
    if category_data:
        context += f"\n## Real Data ({category})\n{category_data}\n"
    if full_text:
        context += f"\n## Article Extract\n{full_text[:1200]}\n"
    if calibration:
        context += f"\n## Past Accuracy\n{calibration}\n"

    # Pass 1: Standard analysis
    pass1 = await _call_claude(context + "\nEstimate the TRUE probability.", category)
    if not pass1:
        return None

    p1 = float(pass1.get("probability", 50))
    pass1_edge = abs(p1 - yes_price)

    # Pass 2: Contrarian — ONLY if pass 1 found significant edge (>15%)
    # This saves ~60% of contrarian calls on markets with no edge
    pass2 = None
    if pass1_edge > 15:
        contrarian_prompt = (
            f"{context}\n"
            f"A first analyst estimated {p1:.0f}% probability. "
            f"Play devil's advocate: what could make this estimate WRONG? "
            f"Then give your own independent probability estimate."
        )
        pass2 = await _call_claude(contrarian_prompt, category)

    if not pass2:
        # Single-pass result (most markets)
        return {
            **pass1,
            "pass1_prob": p1,
            "pass2_prob": None,
            "disagreement": 0,
        }

    # Combine two passes
    p2 = float(pass2.get("probability", 50))
    disagreement = abs(p1 - p2)

    combined_prob = (p1 + p2) / 2
    combined_conf = min(pass1.get("confidence", 5), pass2.get("confidence", 5))

    # If passes disagree by >15%, reduce confidence
    if disagreement > 15:
        combined_conf = max(1, combined_conf - 2)
        analysis = f"[Split opinion: {p1:.0f}% vs {p2:.0f}%] {pass1.get('analysis', '')}"
    else:
        analysis = pass1.get("analysis", "")

    key_factors = pass1.get("key_factors", []) + pass2.get("key_factors", [])
    # Deduplicate
    seen = set()
    unique_factors = []
    for f in key_factors:
        if f.lower() not in seen:
            seen.add(f.lower())
            unique_factors.append(f)

    return {
        "probability": combined_prob,
        "analysis": analysis,
        "confidence": combined_conf,
        "key_factors": unique_factors[:4],
        "pass1_prob": p1,
        "pass2_prob": p2,
        "disagreement": round(disagreement, 1),
    }


async def _call_claude(prompt: str, category: str) -> dict | None:
    """Single Claude Haiku call with cost tracking."""
    try:
        client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None,
            lambda: client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=400,
                system=_get_system_prompt(category),
                messages=[{"role": "user", "content": prompt}],
            ),
        )

        text = response.content[0].text.strip()
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0]
        elif "```" in text:
            text = text.split("```")[1].split("```")[0]

        # Cost tracking
        inp = response.usage.input_tokens if hasattr(response, 'usage') else 0
        out = response.usage.output_tokens if hasattr(response, 'usage') else 0
        cost = (inp * 0.80 + out * 4.0) / 1_000_000
        _track_cost(cost, inp, out)

        return json.loads(text.strip())
    except Exception as e:
        logger.error(f"Claude call failed: {e}")
        await ws_manager.send_log(f"[POLYMARKET] Claude analysis error: {e}", "error")
        return None


def _track_cost(cost: float, inp: int, out: int):
    global _session_cost, _session_calls
    _session_cost += cost
    _session_calls += 1
    logger.info(f"Polymarket AI: ${cost:.4f} ({inp}+{out} tok) | total: ${_session_cost:.4f} ({_session_calls} calls)")


# ══════════════════════════════════════════════════════════════
# TIER 3: KELLY CRITERION POSITION SIZING
# ══════════════════════════════════════════════════════════════

def _kelly_size(ai_prob: float, market_odds: float, confidence: int) -> dict:
    """Calculate Kelly criterion bet size.
    ai_prob: our estimated probability (0-100)
    market_odds: current market price (0-100)
    Returns {kelly_fraction, bet_size, adjusted_fraction}"""

    p = ai_prob / 100  # Our probability of YES
    q = 1 - p          # Our probability of NO

    if ai_prob > market_odds:
        # Bet YES: payout = 1/market_price - 1
        b = (100 / market_odds) - 1 if market_odds > 0 else 0
        if b <= 0:
            return {"kelly_fraction": 0, "bet_size": 0, "adjusted_fraction": 0, "side": "YES"}
        kelly = (b * p - q) / b
    else:
        # Bet NO: payout = 1/(1-market_price) - 1
        no_price = 100 - market_odds
        b = (100 / no_price) - 1 if no_price > 0 else 0
        if b <= 0:
            return {"kelly_fraction": 0, "bet_size": 0, "adjusted_fraction": 0, "side": "NO"}
        kelly = (b * q - p) / b

    kelly = max(0, kelly)

    # Quarter-Kelly for safety (full Kelly is too aggressive)
    adjusted = kelly * 0.25

    # Further reduce based on confidence (low confidence = smaller bet)
    conf_mult = confidence / 10  # confidence 1-10 → 0.1-1.0
    adjusted *= conf_mult

    # Cap at 10% of bankroll per bet
    adjusted = min(adjusted, 0.10)

    bet_size = round(_bankroll * adjusted, 2)

    return {
        "kelly_fraction": round(kelly, 4),
        "adjusted_fraction": round(adjusted, 4),
        "bet_size": bet_size,
        "side": "YES" if ai_prob > market_odds else "NO",
    }


# ══════════════════════════════════════════════════════════════
# TIER 3: AUTO-RESOLUTION TRACKING
# ══════════════════════════════════════════════════════════════

async def _check_resolutions():
    """Check if any past predictions have resolved on Polymarket."""
    global _bankroll
    try:
        from app.database import get_db
        db = await get_db()
        cursor = await db.execute(
            "SELECT * FROM polymarket_predictions WHERE resolved = 0 ORDER BY timestamp DESC LIMIT 50")
        rows = await cursor.fetchall()

        for row in rows:
            slug = row["market_slug"]
            try:
                loop = asyncio.get_event_loop()
                resp = await loop.run_in_executor(
                    None, lambda s=slug: requests.get(
                        f"https://gamma-api.polymarket.com/markets?slug={s}", timeout=10))
                if resp.status_code != 200:
                    continue
                markets = resp.json()
                if not markets:
                    continue
                market = markets[0]

                # Check if resolved
                resolved = market.get("resolved", False)
                if not resolved:
                    continue

                # Get outcome
                outcome = market.get("outcome", "")
                actual_yes = outcome.lower() in ("yes", "true", "1")

                # Check if our recommendation was correct
                rec = row["recommendation"]
                was_correct = (rec == "BUY YES" and actual_yes) or (rec == "BUY NO" and not actual_yes)

                await db.execute(
                    "UPDATE polymarket_predictions SET resolved=1, actual_outcome=?, was_correct=? WHERE id=?",
                    (1 if actual_yes else 0, 1 if was_correct else 0, row["id"]))

                # Update bankroll
                edge = row["edge_pct"]
                bet_size = _bankroll * min(0.05, edge / 100 * 0.25)  # Rough Kelly
                if was_correct:
                    winnings = bet_size * (100 / max(1, row["market_odds"]) - 1) if rec == "BUY YES" else \
                               bet_size * (100 / max(1, 100 - row["market_odds"]) - 1)
                    _bankroll += winnings
                    _bankroll_history.append({
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "action": "win", "amount": round(winnings, 2),
                        "market": row["question"][:50], "bankroll": round(_bankroll, 2)})
                    logger.info(f"Polymarket WIN: +${winnings:.2f} on '{row['question'][:40]}' (bankroll: ${_bankroll:.2f})")
                    await ws_manager.send_log(
                        f"[POLYMARKET] WIN +${winnings:.2f}: {row['question'][:50]} (bankroll: ${_bankroll:.2f})", "success")
                else:
                    _bankroll -= bet_size
                    _bankroll_history.append({
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "action": "loss", "amount": round(-bet_size, 2),
                        "market": row["question"][:50], "bankroll": round(_bankroll, 2)})
                    logger.info(f"Polymarket LOSS: -${bet_size:.2f} on '{row['question'][:40]}' (bankroll: ${_bankroll:.2f})")
                    await ws_manager.send_log(
                        f"[POLYMARKET] LOSS -${bet_size:.2f}: {row['question'][:50]} (bankroll: ${_bankroll:.2f})", "error")

            except Exception as e:
                logger.debug(f"Resolution check failed for {slug}: {e}")

        await db.commit()
        await db.close()
    except Exception as e:
        logger.debug(f"Resolution check error: {e}")


# ══════════════════════════════════════════════════════════════
# CALIBRATION (enhanced from v2)
# ══════════════════════════════════════════════════════════════

async def _init_prediction_tracking():
    try:
        from app.database import get_db
        db = await get_db()
        await db.execute("""
            CREATE TABLE IF NOT EXISTS polymarket_predictions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                market_slug TEXT NOT NULL,
                question TEXT NOT NULL,
                category TEXT,
                market_odds REAL,
                ai_probability REAL,
                ai_confidence INTEGER,
                edge_pct REAL,
                recommendation TEXT,
                kelly_bet REAL DEFAULT 0,
                resolved INTEGER DEFAULT 0,
                actual_outcome INTEGER,
                was_correct INTEGER,
                created_at TEXT DEFAULT (datetime('now'))
            )
        """)
        await db.commit()
        await db.close()
    except Exception as e:
        logger.debug(f"Prediction tracking init: {e}")


async def _save_prediction(slug: str, question: str, category: str,
                           market_odds: float, ai_prob: float,
                           confidence: int, edge: float, rec: str, kelly_bet: float):
    try:
        from app.database import get_db
        db = await get_db()
        await db.execute(
            """INSERT INTO polymarket_predictions
               (timestamp, market_slug, question, category, market_odds,
                ai_probability, ai_confidence, edge_pct, recommendation, kelly_bet)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (datetime.now(timezone.utc).isoformat(), slug, question,
             category, market_odds, ai_prob, confidence, edge, rec, kelly_bet))
        await db.commit()
        await db.close()
    except Exception:
        pass


def _get_calibration_note() -> str:
    if len(_prediction_history) < 10:
        return ""
    resolved = [p for p in _prediction_history if p.get("resolved")]
    if len(resolved) < 5:
        return ""
    correct = sum(1 for p in resolved if p.get("was_correct"))
    total = len(resolved)
    accuracy = correct / total * 100
    return (
        f"Your accuracy on {total} resolved markets: {accuracy:.0f}% correct. "
        f"Bankroll: ${_bankroll:.2f} (started $1000). "
        f"{'Be more conservative.' if accuracy < 45 else 'Calibration is good.' if accuracy > 55 else 'Moderate accuracy — maintain discipline.'}"
    )


async def _load_prediction_history():
    global _prediction_history, _bankroll
    try:
        from app.database import get_db
        db = await get_db()
        cursor = await db.execute(
            "SELECT * FROM polymarket_predictions ORDER BY timestamp DESC LIMIT 200")
        rows = await cursor.fetchall()
        _prediction_history = [dict(r) for r in rows] if rows else []
        await db.close()
        logger.info(f"Loaded {len(_prediction_history)} predictions for calibration")
    except Exception:
        _prediction_history = []


# ══════════════════════════════════════════════════════════════
# MAIN SCAN LOOP
# ══════════════════════════════════════════════════════════════

async def _scan_once() -> list[dict]:
    global _last_scan_time, _markets_analyzed, _scan_progress

    # Check resolutions first (Tier 3)
    await _check_resolutions()

    markets = await _fetch_markets()
    if not markets:
        _scan_progress = {"scanning": False, "current": 0, "total": 0, "current_market": ""}
        return []

    # ── Pre-filter: prioritize sports + geopolitics, score by potential ──
    for m in markets:
        price = m.get("_yes_price", 50.0)
        vol = m.get("_volume_24h", 0)
        cat = _classify_market(m.get("question", ""))
        # Score: prefer mid-range odds, high volume, sharp movers, priority categories
        mid_score = 10 - abs(price - 50) / 5  # 0-10, best at 50%
        vol_score = min(5, vol / 50_000)       # 0-5 based on volume
        movement = _track_odds_movement(m.get("slug", ""), price)
        move_score = 3 if movement.get("sharp_move") else 0
        # Boost sports and geopolitics (our best categories)
        cat_boost = 5 if cat in ("sports", "geopolitics") else 2 if cat in ("politics_us", "politics_intl") else 0
        m["_prefilter_score"] = mid_score + vol_score + move_score + cat_boost

    markets.sort(key=lambda m: m.get("_prefilter_score", 0), reverse=True)

    # Ensure category diversity: max 3 per category, then fill to 10
    seen_cats: dict = {}
    diverse_markets = []
    remaining = []
    for m in markets:
        cat = _classify_market(m.get("question", ""))
        if seen_cats.get(cat, 0) < 3:
            diverse_markets.append(m)
            seen_cats[cat] = seen_cats.get(cat, 0) + 1
        else:
            remaining.append(m)
    # Fill to 10 with remaining if needed
    markets = (diverse_markets + remaining)[:15]  # Analyze up to 15 for more picks

    cat_summary = {}
    for m in markets:
        c = _classify_market(m.get("question", ""))
        cat_summary[c] = cat_summary.get(c, 0) + 1
    logger.info(f"Polymarket: pre-filtered to {len(markets)} markets: {dict(cat_summary)}")
    await ws_manager.send_log(f"[POLYMARKET] Analyzing top {len(markets)} markets...", "info")

    recommendations = []
    _markets_analyzed = len(markets)
    _scan_progress = {"scanning": True, "current": 0, "total": len(markets), "current_market": ""}

    for idx, market in enumerate(markets):
        question = market.get("question", "")
        slug = market.get("slug", "")
        yes_price = market.get("_yes_price", 50.0)
        _scan_progress["current"] = idx + 1
        _scan_progress["current_market"] = question[:50]

        # Extract outcome names (e.g., ["Chennai Super Kings", "Punjab Kings"] or ["Yes", "No"])
        try:
            outcome_names = json.loads(market.get("outcomes", '["Yes", "No"]'))
        except (json.JSONDecodeError, TypeError):
            outcome_names = ["Yes", "No"]
        yes_label = outcome_names[0] if len(outcome_names) > 0 else "Yes"
        no_label = outcome_names[1] if len(outcome_names) > 1 else "No"

        market_url = f"https://polymarket.com/market/{slug}"
        category = _classify_market(question)

        # Tier 1: Track odds movement (already computed in pre-filter)
        odds_movement = _track_odds_movement(slug, yes_price)

        # Tier 2: Fetch real data for category
        category_data = await _fetch_category_data(question, category)

        # Fetch news (multi-source + full article)
        news_data = await _fetch_news_deep(question)

        # Tier 3: Multi-pass analysis (contrarian only on high edge)
        analysis = await _analyze_multi_pass(
            question, yes_price, news_data, category, category_data, odds_movement)
        if not analysis:
            continue

        ai_prob = float(analysis.get("probability", 50))
        confidence = int(analysis.get("confidence", 5))
        edge = abs(ai_prob - yes_price)

        # Skip if AI detected the event already happened (stale market)
        analysis_text = (analysis.get("analysis") or "").lower()
        already_phrases = ["already been played", "already completed", "already occurred",
                           "already happened", "match has ended", "game is over",
                           "already resolved", "final result", "already won",
                           "already beat", "already lost", "already-played",
                           "retrospective", "match result", "completed match",
                           "pbks won", "csk won", "game has concluded",
                           "has already been", "outcome has already"]
        if any(phrase in analysis_text for phrase in already_phrases):
            logger.info(f"Skipped {question[:40]}: AI detected event already completed")
            await ws_manager.send_log(f"[POLYMARKET] Skipped stale: {question[:40]} (event already over)", "warning")
            continue

        # Lower edge threshold for sports and geopolitics (our best categories)
        min_edge = 8.0 if category in ("sports", "geopolitics") else settings.POLYMARKET_MIN_EDGE_PCT
        if edge < min_edge:
            continue

        # Frame as the actual outcome to bet on (not abstract YES/NO)
        if ai_prob > yes_price:
            bet_on = yes_label  # e.g., "Chennai Super Kings" or "Yes"
            bet_reasoning = f"Market underprices {bet_on} ({yes_price:.0f}% → AI {ai_prob:.0f}%)"
            recommendation = f"BET {bet_on.upper()}" if bet_on != "Yes" else "BET YES"
        else:
            bet_on = no_label  # e.g., "Punjab Kings" or "No"
            bet_reasoning = f"Market underprices {bet_on} ({100-yes_price:.0f}% → AI {100-ai_prob:.0f}%)"
            recommendation = f"BET {bet_on.upper()}" if bet_on != "No" else "BET NO"

        # Tier 3: Kelly sizing
        kelly = _kelly_size(ai_prob, yes_price, confidence)

        # Skip if Kelly says no bet (negative edge after adjustment)
        if kelly["bet_size"] <= 0:
            continue

        # Save prediction
        await _save_prediction(slug, question, category, yes_price, ai_prob,
                               confidence, edge, recommendation, kelly["bet_size"])

        # Log each pick to dashboard
        await ws_manager.send_log(
            f"[POLYMARKET] {recommendation}: {question[:45]} | edge +{edge:.1f}% | Kelly ${kelly['bet_size']:.0f} | conf {confidence}/10",
            "success" if edge >= 20 else "info")

        source_count = len(set(news_data.get("sources", [])))

        recommendations.append({
            "question": question,
            "market_url": market_url,
            "market_odds": round(yes_price, 1),
            "ai_probability": round(ai_prob, 1),
            "edge_pct": round(edge, 1),
            "recommendation": recommendation,
            "bet_on": bet_on,
            "bet_reasoning": bet_reasoning,
            "yes_label": yes_label,
            "no_label": no_label,
            "confidence": "High" if confidence >= 8 else "Medium" if confidence >= 5 else "Low",
            "ai_confidence_score": confidence,
            "analysis_text": analysis.get("analysis", ""),
            "key_factors": analysis.get("key_factors", []),
            "category": category,
            "news_sources": source_count,
            "has_deep_analysis": bool(news_data.get("full_text")),
            "has_real_data": bool(category_data),
            "odds_movement": odds_movement,
            "kelly_bet": kelly["bet_size"],
            "kelly_fraction": kelly["adjusted_fraction"],
            "kelly_side": kelly["side"],
            "pass1_prob": analysis.get("pass1_prob"),
            "pass2_prob": analysis.get("pass2_prob"),
            "disagreement": analysis.get("disagreement", 0),
            "hours_to_expiry": market.get("_hours_to_expiry"),
            "last_updated": datetime.now(timezone.utc).isoformat(),
        })

    # Sort by confidence * edge (best risk-adjusted bets first)
    recommendations.sort(
        key=lambda r: r["ai_confidence_score"] * r["edge_pct"], reverse=True)
    recommendations = recommendations[:settings.POLYMARKET_MAX_RESULTS]

    _last_scan_time = time.time()
    _scan_progress = {"scanning": False, "current": _markets_analyzed,
                      "total": _markets_analyzed, "current_market": ""}
    summary = (f"[POLYMARKET] Scan complete: {len(recommendations)} picks from "
               f"{_markets_analyzed} markets | cost ${_session_cost:.4f} | bankroll ${_bankroll:.2f}")
    logger.info(summary)
    await ws_manager.send_log(summary, "success" if recommendations else "info")
    return recommendations


async def _validate_cached_picks():
    """Check if any cached picks have resolved and remove them.
    Three detection methods:
    1. API resolved/closed flag
    2. Extreme odds (95%+ either side = effectively resolved)
    3. AI analysis text mentions event already happened
    """
    global _cache
    if not _cache:
        return

    loop = asyncio.get_event_loop()
    removed = []

    for rec in list(_cache):
        slug = rec.get("market_url", "").split("/")[-1]
        if not slug:
            continue

        should_remove = False
        reason = ""

        # Method 1: Check API resolved flag
        try:
            resp = await loop.run_in_executor(
                None, lambda s=slug: requests.get(
                    f"https://gamma-api.polymarket.com/markets?slug={s}", timeout=8))
            if resp.status_code == 200:
                markets = resp.json()
                if markets:
                    m = markets[0]
                    if m.get("resolved") or m.get("closed"):
                        should_remove = True
                        reason = "API resolved"

                    # Method 2: Check if odds went extreme (95%+ = effectively over)
                    if not should_remove:
                        try:
                            prices = json.loads(m.get("outcomePrices", "[]"))
                            yes_p = float(prices[0]) * 100 if prices else 50
                            if yes_p >= 95 or yes_p <= 5:
                                should_remove = True
                                reason = f"extreme odds ({yes_p:.0f}%)"
                        except (ValueError, IndexError, TypeError):
                            pass
        except Exception:
            pass

        # Method 3: Check if AI analysis says event already happened
        if not should_remove:
            analysis = (rec.get("analysis_text") or "").lower()
            already_phrases = ["already been played", "already completed", "already occurred",
                               "already happened", "match has ended", "game is over",
                               "already resolved", "final result", "already won",
                               "already beat", "already lost"]
            if any(phrase in analysis for phrase in already_phrases):
                should_remove = True
                reason = "AI detected event completed"

        if should_remove:
            _resolved_slugs.add(slug)
            removed.append((rec["question"][:40], reason))

    if removed:
        _cache = [r for r in _cache
                  if r.get("market_url", "").split("/")[-1] not in _resolved_slugs]
        for q, reason in removed:
            logger.info(f"Polymarket: removed '{q}' ({reason})")
            await ws_manager.send_log(f"[POLYMARKET] Removed: {q} ({reason})", "info")
            await ws_manager.send_log(f"[POLYMARKET] Removed resolved: {q}", "info")


async def run_scanner_loop():
    global _cache, _cache_ts, _next_scan_time

    await _init_prediction_tracking()
    await _load_prediction_history()

    logger.info(f"Polymarket scanner v3 started (interval={settings.POLYMARKET_SCAN_INTERVAL}s)")

    last_validation = 0

    while True:
        try:
            if not _enabled:
                await asyncio.sleep(5)
                continue

            _next_scan_time = time.time() + settings.POLYMARKET_SCAN_INTERVAL
            results = await _scan_once()
            if results:
                _cache = results
                _cache_ts = time.time()
            elif not _cache:
                _cache = []
                _cache_ts = time.time()
        except Exception as e:
            logger.error(f"Polymarket scanner error: {e}")
            await ws_manager.send_log(f"[POLYMARKET] Scanner error: {e}", "error")

        # Between full scans, validate cached picks every 5 minutes
        scan_interval = settings.POLYMARKET_SCAN_INTERVAL
        elapsed = 0
        while elapsed < scan_interval:
            await asyncio.sleep(min(300, scan_interval - elapsed))  # 5 min or remaining
            elapsed += 300
            if not _enabled:
                break
            # Validate cached picks — remove resolved ones
            if time.time() - last_validation > 300:
                await _validate_cached_picks()
                last_validation = time.time()


def start_scanner():
    global _scanner_task
    if _scanner_task is None or _scanner_task.done():
        _scanner_task = asyncio.create_task(run_scanner_loop())
        logger.info("Polymarket scanner v3 task created")
