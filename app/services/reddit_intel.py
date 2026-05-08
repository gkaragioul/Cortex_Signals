"""Reddit intel digest — scans prediction-market / trading subs once per day and
sends a single Telegram message ~1h before the daily digest.

Purpose: META-LEARNING for bot development, NOT trade signals. We do not want
Sonnet suggesting which markets to bet on. We want it surfacing posts that
teach us something about how to build, improve, or harden the bot — pro/whale
workflows, post-mortems, new tools, market-structure insight, transferable
quant techniques, research findings.

Design: surface, don't execute. Mirrors the BeefSlayer / polybot-arena
discovery pattern — needles in haystacks worth a human eye to absorb and
possibly turn into a feature.

Fetches Reddit's public .json endpoints (no auth, ~60 req/min) with a Mozilla
UA to sidestep the default-agent 403 wall. Fails soft — if any sub errors, the
others still ship.
"""
from __future__ import annotations

import asyncio
import html
import json
import logging
import os
import urllib.request
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

SUBREDDITS = [
    # Prediction markets
    "Polymarket",
    "polymarketAnalysis",
    "PredictionMarkets",
    "Kalshi",
    # General trading / quant
    "algotrading",
    "quantitativefinance",
    "wallstreetbets",
    "options",
    # Domain signals (weather / sports / crypto / macro)
    "sportsbook",
    "CryptoCurrency",
    "meteorology",
    "geopolitics",
]

POSTS_PER_SUB = 5
FETCH_TIMEOUT = 8
UA = "Mozilla/5.0 (Cortex Signals; +https://github.com/karagioules/Cortex_Signals)"


def _fetch_sub(sub: str) -> list[dict]:
    url = f"https://www.reddit.com/r/{sub}/top.json?t=day&limit={POSTS_PER_SUB}"
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=FETCH_TIMEOUT) as r:
            body = r.read().decode("utf-8", errors="replace")
    except Exception as e:
        logger.warning("reddit_intel: fetch r/%s failed: %s", sub, e)
        return []

    try:
        data = json.loads(body)
    except Exception:
        return []

    posts = []
    for c in (data.get("data") or {}).get("children", []) or []:
        d = c.get("data") or {}
        if d.get("stickied") or d.get("over_18"):
            continue
        posts.append({
            "sub": sub,
            "title": (d.get("title") or "").strip()[:200],
            "score": int(d.get("score") or 0),
            "comments": int(d.get("num_comments") or 0),
            "url": f"https://reddit.com{d.get('permalink', '')}",
            "selftext": (d.get("selftext") or "")[:500],
        })
    return posts


async def _gather_posts() -> list[dict]:
    loop = asyncio.get_running_loop()
    results = await asyncio.gather(
        *[loop.run_in_executor(None, _fetch_sub, s) for s in SUBREDDITS],
        return_exceptions=True,
    )
    posts: list[dict] = []
    for r in results:
        if isinstance(r, list):
            posts.extend(r)
    return posts


async def _sonnet_digest(posts: list[dict]) -> str:
    """Run Sonnet over the scraped posts and extract what's actionable.

    Returns HTML-safe text for Telegram. Falls back to a raw top-N list if the
    API call fails so the digest still ships.
    """
    from app.config import settings

    if not posts:
        return "No posts fetched — all subs returned empty."

    if not settings.ANTHROPIC_API_KEY:
        return _fallback_summary(posts)

    lines = []
    for p in posts:
        sn = p["selftext"].replace("\n", " ").strip()
        if sn:
            sn = f" — {sn[:200]}"
        lines.append(f"[{p['sub']}] ({p['score']}\u2191 {p['comments']}\U0001F4AC) {p['title']}{sn}\n  {p['url']}")
    feed = "\n".join(lines)

    prompt = (
        "You scan trading + prediction-market subreddits for an operator BUILDING a "
        "Polymarket autotrader. The goal is NOT trade signals — we do not need you to "
        "tell us which markets to bet on or which events to react to. The goal is "
        "knowledge that helps us improve the BOT ITSELF: tools, methodologies, "
        "post-mortems, whale/pro workflows, architectural ideas, data sources, "
        "failure stories, things that could become new features or sharpen existing "
        "signals. Meta-learning, not market-timing.\n\n"
        "What counts as valuable:\n"
        "- A pro or whale explaining HOW they trade (process, sizing, filters, tooling)\n"
        "- Post-mortems: a bet/strategy that won big or blew up, with the reasoning\n"
        "- New tools, scanners, dashboards, APIs, or platforms worth evaluating\n"
        "- Quant/algo threads with techniques transferable to prediction markets\n"
        "- Market-structure insight (fees, liquidity regimes, book dynamics, rebates)\n"
        "- Research posts: academic findings, data releases, novel methodologies\n"
        "- Other bots / operators worth tracking or learning from\n\n"
        "What to EXCLUDE (hard rule):\n"
        "- 'Trade X market now' / 'Buy NO on Y because news broke' — zero value here\n"
        "- Event-specific predictions ('this candidate will win')\n"
        "- Price calls, hype threads, cheerleading posts\n"
        "- Generic news already covered by mainstream outlets\n\n"
        "Output format (HTML-safe for Telegram, keep under 3500 chars total):\n"
        "<b>Worth studying:</b>\n"
        "1. <b>[Sub]</b> Title — one sentence on what we can LEARN from it for the bot. Link.\n"
        "2. ...\n"
        "3. ...\n\n"
        "<b>Bot ideas / features to consider:</b>\n"
        "- 3-5 bullets tying a post back to a concrete feature or tweak we could build\n\n"
        "<b>Tools / platforms to evaluate:</b>\n"
        "- Any new scanners, APIs, dashboards, or operator tools mentioned\n\n"
        "<b>Skip:</b> one-line note on pure trade-chasing or hype we filtered out\n\n"
        "Rules: no emojis. No markdown bold (use <b></b>). Escape &, <, > in titles. "
        "If today's posts are all trade-chasing noise with no meta value, say so "
        "plainly — don't fabricate lessons.\n\n"
        f"POSTS (today's top from {len(SUBREDDITS)} subs):\n{feed}"
    )

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)
        loop = asyncio.get_running_loop()
        resp = await loop.run_in_executor(
            None,
            lambda: client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=1500,
                messages=[{"role": "user", "content": prompt}],
            ),
        )
        text = "".join(b.text for b in resp.content if hasattr(b, "text")).strip()
        return text or _fallback_summary(posts)
    except Exception as e:
        logger.warning("reddit_intel: Sonnet call failed: %s", e)
        return _fallback_summary(posts)


def _fallback_summary(posts: list[dict]) -> str:
    top = sorted(posts, key=lambda p: p["score"], reverse=True)[:8]
    lines = ["<b>Raw top posts (Sonnet unavailable):</b>"]
    for p in top:
        t = html.escape(p["title"])
        lines.append(f"- [{p['sub']}] {t} ({p['score']}\u2191)\n  {p['url']}")
    return "\n".join(lines)


async def run_daily_intel() -> bool:
    """Scrape, digest, Telegram-send, and surface in the Bot Brain feed.
    Returns True if Telegram message went out."""
    posts = await _gather_posts()
    logger.info("reddit_intel: fetched %d posts across %d subs", len(posts), len(SUBREDDITS))
    body = await _sonnet_digest(posts)

    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    text = (
        f"<b>\U0001F4F0 Reddit intel \u00b7 {date}</b>\n"
        f"<i>Scanned {len(SUBREDDITS)} subs \u00b7 {len(posts)} posts</i>\n\n"
        f"{body}"
    )

    # v3.18.0 — also pipe to Bot Brain feed (AI Intel filter pill picks it up
    # via the [SONNET] tag prefix). Strips HTML tags for plain-text rendering.
    try:
        from app.services.websocket_manager import ws_manager
        import re
        plain = re.sub(r"<[^>]+>", "", body)
        intro = f"[SONNET] Reddit intel · {date} · scanned {len(SUBREDDITS)} subs / {len(posts)} posts"
        await ws_manager.send_log(intro, "brain")
        # Send as one expandable cluster — appendBrainMessage groups [SONNET]
        # entries via the existing _opusBriefLines collector.
        for line in plain.split("\n"):
            line = line.strip()
            if line:
                await ws_manager.send_log(f"[SONNET] {line}", "brain")
    except Exception as e:
        logger.warning("reddit_intel: brain-feed pipe failed: %s", e)

    from app.services.telegram_notify import _send
    ok = _send(text)
    if not ok:
        logger.info("reddit_intel: telegram disabled or send failed")
    return ok
