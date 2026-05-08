"""
AI Analyst for Cortex Signals
==================================
Two-tier AI system within $0.50/day budget:

Tier 1 — Claude Haiku ($0.02/day): fast signal checks
- Market Context Evaluation (CONFIRM/SKIP/REDUCE before bets)
- Post-Trade Pattern Review (every 10 resolved bets)

Tier 2 — Claude Sonnet ($0.18/day): deep market research
- Runs 6x/day (every 4 hours)
- Reads news, analyzes geopolitical events, weather patterns
- Produces intelligence brief that adjusts signal scoring
- Flags hidden risks and opportunities across all open bets
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timezone

from app.config import settings
from app.services.websocket_manager import ws_manager

logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════
# AI CALL WRAPPER
# ══════════════════════════════════════════════════════════════

_daily_calls = 0
_daily_opus_calls = 0
_daily_reset = 0
_ai_lock = asyncio.Lock()  # Thread-safe rate limiting
MAX_DAILY_HAIKU_CALLS = 200  # ~$0.04/day
MAX_DAILY_OPUS_CALLS = 6     # ~$0.18/day with Sonnet (every 4 hours)
# Total budget: $0.04 (Haiku) + $0.18 (Sonnet) = $0.22/day max

# Deep research cache — latest intelligence brief
_intelligence_brief: dict = {}
_last_deep_research: float = 0
DEEP_RESEARCH_INTERVAL = 14400  # Every 4 hours (6x/day)


async def _ask_claude(prompt: str, max_tokens: int = 150) -> str | None:
    """Call Claude Haiku with thread-safe daily rate limiting."""
    global _daily_calls, _daily_reset

    if not settings.ANTHROPIC_API_KEY:
        return None

    async with _ai_lock:
        # Reset daily counter
        now = time.time()
        if now - _daily_reset > 86400:
            _daily_calls = 0
            _daily_reset = now

        if _daily_calls >= MAX_DAILY_HAIKU_CALLS:
            return None
        _daily_calls += 1  # Reserve the slot inside the lock

    try:
        import anthropic
        loop = asyncio.get_running_loop()
        client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)

        today = datetime.now(timezone.utc).strftime("%B %d, %Y")
        full_prompt = f"Today is {today}.\n\n{prompt}"

        resp = await loop.run_in_executor(
            None,
            lambda: client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=max_tokens,
                messages=[{"role": "user", "content": full_prompt}],
            ),
        )
        text = resp.content[0].text.strip() if resp.content else ""
        return text

    except Exception as e:
        logger.debug(f"AI analyst call failed: {e}")
        return None


# ══════════════════════════════════════════════════════════════
# 1. NEWS SENTIMENT ANALYZER
# ══════════════════════════════════════════════════════════════

async def analyze_rapid_move(market: str, direction: str, move_pct: float,
                              headlines: list[str] = None) -> dict:
    """Analyze a rapid price move: is it justified or an overreaction?

    Returns {"verdict": "JUSTIFIED"|"OVERREACTION"|"UNKNOWN", "reasoning": str, "confidence": float}
    """
    headline_text = ""
    if headlines:
        headline_text = "\nRecent headlines:\n" + "\n".join(f"- {h}" for h in headlines[:5])

    prompt = (
        f"Prediction market: \"{market}\"\n"
        f"Price just moved {direction} by {move_pct:.1f}% in 20 minutes.\n"
        f"{headline_text}\n\n"
        f"Is this move JUSTIFIED by real events, or an OVERREACTION?\n"
        f"Reply: JUSTIFIED or OVERREACTION, then one sentence why.\n"
        f"JUSTIFIED = real news/event caused this, price should stay.\n"
        f"OVERREACTION = emotional trading, price will likely revert."
    )

    text = await _ask_claude(prompt, 80)
    if not text:
        return {"verdict": "UNKNOWN", "reasoning": "AI unavailable", "confidence": 0}

    verdict = "JUSTIFIED" if text.upper().startswith("JUSTIFIED") else \
              "OVERREACTION" if text.upper().startswith("OVERREACTION") else "UNKNOWN"

    return {
        "verdict": verdict,
        "reasoning": text[:200],
        "confidence": 0.7 if verdict != "UNKNOWN" else 0,
    }


# ══════════════════════════════════════════════════════════════
# 2. MARKET CONTEXT EVALUATOR
# ══════════════════════════════════════════════════════════════

async def evaluate_signal(market: str, source: str, side: str, score: float,
                           edge: float, price: float) -> dict:
    """Evaluate a signal before placing a high-conviction bet.

    Only called for score >= 60 signals. Returns recommendation.
    Returns {"action": "CONFIRM"|"SKIP"|"REDUCE", "reasoning": str}
    """
    if score < 60:
        return {"action": "CONFIRM", "reasoning": "Below AI evaluation threshold"}

    prompt = (
        f"Prediction market: \"{market}\"\n"
        f"Signal: {source} says BET {side} at {price*100:.0f}c (score {score:.0f}/100, edge {edge:.1f}%)\n\n"
        f"Quick assessment — any red flags?\n"
        f"Consider: Is this market likely to resolve as expected? "
        f"Are there obvious risks the algorithm might miss? "
        f"Is the timing right?\n\n"
        f"Reply: CONFIRM (looks good), SKIP (red flag found), or REDUCE (bet smaller).\n"
        f"Then one sentence explanation."
    )

    text = await _ask_claude(prompt, 80)
    if not text:
        return {"action": "CONFIRM", "reasoning": "AI unavailable — proceeding"}

    action = "CONFIRM"
    upper = text.upper()
    if upper.startswith("SKIP"):
        action = "SKIP"
    elif upper.startswith("REDUCE"):
        action = "REDUCE"

    return {
        "action": action,
        "reasoning": text[:200],
    }


# ══════════════════════════════════════════════════════════════
# 3. POST-TRADE REVIEW
# ══════════════════════════════════════════════════════════════

_last_review_count = 0


async def review_recent_performance(resolved_bets: list[dict]) -> dict | None:
    """Analyze recent trading patterns after every 10 new resolved bets.

    Looks for: which sources win, which markets lose, timing patterns,
    sizing mistakes, and actionable improvements.

    Returns {"insights": str, "recommendations": list[str]} or None if not enough data.
    """
    global _last_review_count

    total = len(resolved_bets)
    if total < 10 or total - _last_review_count < 10:
        return None

    _last_review_count = total

    # Build summary for Claude
    recent = resolved_bets[:20]  # Last 20 bets
    summary_lines = []
    for b in recent:
        pnl = b.get("pnl", 0)
        sign = "+" if pnl >= 0 else ""
        summary_lines.append(
            f"{'WIN' if pnl > 0 else 'LOSS'}: {b.get('source', '?')} | "
            f"\"{b.get('market', '?')[:40]}\" | "
            f"{b.get('side', '?')} @ {b.get('entry_price', 0)*100:.0f}c | "
            f"{sign}${abs(pnl):.2f} | score {b.get('score', 0):.0f}"
        )

    wins = sum(1 for b in recent if (b.get("pnl", 0) or 0) > 0)
    losses = len(recent) - wins
    total_pnl = sum(b.get("pnl", 0) or 0 for b in recent)

    prompt = (
        f"Trading bot performance review — last {len(recent)} bets:\n"
        f"Record: {wins}W / {losses}L ({wins/len(recent)*100:.0f}% win rate)\n"
        f"Total P&L: {'+'if total_pnl>=0 else ''}${total_pnl:.2f}\n\n"
        f"Individual bets:\n" + "\n".join(summary_lines) + "\n\n"
        f"Analyze the patterns. In 2-3 bullet points:\n"
        f"1. Which signal sources are winning vs losing?\n"
        f"2. Any market types or patterns to avoid?\n"
        f"3. One specific actionable recommendation to improve."
    )

    text = await _ask_claude(prompt, 250)
    if not text:
        return None

    result = {
        "insights": text,
        "reviewed_count": len(recent),
        "win_rate": round(wins / len(recent) * 100, 1),
        "total_pnl": round(total_pnl, 2),
        "timestamp": time.time(),
    }

    # Send to Bot Brain
    await ws_manager.send_log(f"[AI REVIEW] {text[:150]}", "brain")
    logger.info(f"AI performance review: {wins}W/{losses}L, ${total_pnl:.2f}")

    return result


# ══════════════════════════════════════════════════════════════
# 4. OPUS DEEP RESEARCH (6x/day, $0.45/day)
# ══════════════════════════════════════════════════════════════

async def _ask_sonnet(prompt: str) -> str | None:
    """Call Claude Sonnet for deep analysis. Max 6 calls/day."""
    global _daily_opus_calls, _daily_reset, _daily_calls

    if not settings.ANTHROPIC_API_KEY:
        return None

    async with _ai_lock:
        now = time.time()
        if now - _daily_reset > 86400:
            _daily_opus_calls = 0
            _daily_calls = 0
            _daily_reset = now

        if _daily_opus_calls >= MAX_DAILY_OPUS_CALLS:
            return None
        _daily_opus_calls += 1

    try:
        import anthropic
        loop = asyncio.get_running_loop()
        client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)

        today = datetime.now(timezone.utc).strftime("%B %d, %Y, %H:%M UTC")
        full_prompt = f"Current date/time: {today}\n\n{prompt}"

        resp = await loop.run_in_executor(
            None,
            lambda: client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=2048,
                messages=[{"role": "user", "content": full_prompt}],
            ),
        )
        text = resp.content[0].text.strip() if resp.content else ""
        return text

    except Exception as e:
        logger.error(f"Opus deep research failed: {e}")
        return None


async def run_deep_research(open_bets: list[dict], mispricing_edges: list[dict],
                            weather_signals: list[dict], rapid_moves: list[dict]) -> dict | None:
    """Geopolitical & political market intelligence brief (runs every 4 hours, 6x/day).

    Analyses open bets and mispricing edges using Claude's political/geopolitical
    knowledge to flag resolution risk, upcoming catalysts, and sentiment divergence.
    """
    global _intelligence_brief, _last_deep_research

    if time.time() - _last_deep_research < DEEP_RESEARCH_INTERVAL:
        return _intelligence_brief if _intelligence_brief else None

    # Build context from open bets and mispricing edges
    open_summary = ""
    if open_bets:
        lines = []
        for b in open_bets[:15]:
            market = b.get('market', '?')[:70]
            side = b.get('side', '?')
            entry = b.get('entry_price', 0) * 100
            source = b.get('signal_source', '?')
            pnl = b.get('pnl') or 0
            lines.append(f"  - [{side}] \"{market}\" @ {entry:.0f}c ({source}) pnl={pnl:+.1f}")
        open_summary = f"OPEN POSITIONS ({len(open_bets)} total, showing top 15):\n" + "\n".join(lines)

    edges_summary = ""
    if mispricing_edges:
        lines = []
        for e in mispricing_edges[:8]:
            market = e.get('market', '?')[:70]
            edge = e.get('edge_pct', 0)
            side = e.get('side', '?')
            score = e.get('score', 0)
            lines.append(f"  - [{side}] \"{market}\" edge={edge:.1f}% score={score:.0f}")
        edges_summary = f"\nTOP MISPRICING EDGES:\n" + "\n".join(lines)

    from datetime import datetime, timezone
    today = datetime.now(timezone.utc).strftime("%A, %B %d, %Y | %H:%M UTC")

    prompt = (
        f"You are a geopolitical and political market intelligence analyst for a prediction market trading bot.\n"
        f"Today is {today}. Use your knowledge of current world events up to your training cutoff to analyse "
        f"the bot's open positions and flagged mispricing opportunities.\n\n"
        f"{open_summary}\n{edges_summary}\n\n"
        f"Provide a concise intelligence brief (5-7 bullet points covering the topics below):\n\n"
        f"1. RESOLUTION RISK: For the open positions above, are there any upcoming votes, announcements, "
        f"court rulings, elections, or deadlines in the next 1-7 days that could resolve these markets "
        f"against our position? Name specific events and dates.\n"
        f"2. MISPRICING VALIDITY: For the flagged edges, do the market prices seem reasonable or "
        f"mispriced given known political/geopolitical dynamics? Flag any that look like traps "
        f"(e.g. market is cheap because a known risk event is imminent).\n"
        f"3. UPCOMING CATALYSTS: What scheduled political events this week (summits, votes, earnings, "
        f"Fed decisions, election results) could cause large price moves across these markets?\n"
        f"4. SENTIMENT VS FUNDAMENTALS: Where is market sentiment diverging from the most likely "
        f"outcome based on the known facts? Are there any consensus-wrong situations?\n"
        f"5. RISK FLAGS: Any open bets that should be considered for early exit based on your "
        f"knowledge of how these situations typically resolve?\n\n"
        f"Be specific. Name the market, the event, and your reasoning. No vague advice. "
        f"If you have no knowledge of a specific market topic, skip it rather than speculate."
    )

    text = await _ask_sonnet(prompt)
    if not text:
        return _intelligence_brief if _intelligence_brief else None

    _intelligence_brief = {
        "brief": text,
        "timestamp": time.time(),
        "open_bets_count": len(open_bets),
        "edges_count": len(mispricing_edges),
    }
    _last_deep_research = time.time()

    # Send to Bot Brain
    brief_lines = text.split("\n")
    for line in brief_lines[:15]:
        if line.strip():
            await ws_manager.send_log(f"[SONNET] {line.strip()}", "brain")

    logger.info(f"Forecast intelligence brief completed: {len(text)} chars")
    return _intelligence_brief


def get_intelligence_brief() -> dict | None:
    """Return the latest Opus intelligence brief."""
    if not _intelligence_brief:
        return None
    # Brief is stale after 5 hours (slightly longer than research interval)
    if time.time() - _intelligence_brief.get("timestamp", 0) > 18000:
        return None
    return _intelligence_brief
