# SPDX-License-Identifier: MIT

"""v3.20.4 — Exit-rule engine.

Single source of truth for the "when to close a manual position" advice
shown on Opportunities cards, Telegram entry alerts, and used by the
exit-monitor loop to decide whether to fire a Telegram exit push.

Mirrors the internal exit rules polymarket_simulator.py applies to
paper bets (market-type profit targets + stops, thesis invalidation,
time-decay harvesting) so the manual trader gets the same discipline
the paper engine uses.

Entry prices can arrive in either 0-1 proportion (news-speed scanner)
or 0-100 percentage (consensus scanner). _norm() handles both; public
callers never need to care about scale.
"""

from __future__ import annotations


def _norm(p: float | None) -> float:
    """Normalize any price to 0-1 proportion regardless of input scale."""
    if p is None:
        return 0.0
    p = float(p)
    if p > 1.5:  # must be 0-100 scale
        return p / 100.0
    return p


def compute_exit_plan(
    kind: str,
    entry_price: float | None,
    fair_value: float | None = None,
) -> dict:
    """Return an exit plan for a manual position.

    Args:
        kind: Alert kind emitted by opportunity_alerts — one of
            "MISPRICING", "WHALE", "WEATHER", "HARVEST". Unknown kinds
            fall through to DEFAULT (±25%/-12%).
        entry_price: 0-1 or 0-100 accepted (normalized internally).
        fair_value: 0-1 or 0-100 accepted; only used for MISPRICING.

    Returns:
        dict with keys:
            target_price: 0-1 proportion, None if hold_to_settlement.
            stop_price:   0-1 proportion, None if hold_to_settlement.
            target_pct:   % gain target at target_price (None if hold).
            stop_pct:     % loss stop at stop_price (None if hold).
            hold_to_settlement: bool.
            rule_text:    one-line human-readable for card + Telegram.
    """
    k = (kind or "").upper()
    e = _norm(entry_price)

    # Holds-to-settlement: early exit sacrifices the whole thesis
    if k in ("HARVEST", "WEATHER"):
        return {
            "target_price": None,
            "stop_price": None,
            "target_pct": None,
            "stop_pct": None,
            "hold_to_settlement": True,
            "rule_text": "Hold to resolution — no early exit worth taking.",
        }

    if e <= 0:
        # Degenerate entry — return a neutral rule so UI never crashes
        return {
            "target_price": None,
            "stop_price": None,
            "target_pct": None,
            "stop_pct": None,
            "hold_to_settlement": False,
            "rule_text": "No exit rule (entry price unavailable).",
        }

    if k == "MISPRICING":
        # Thesis is "price converges to fair value." Target = fair value itself.
        # v3.20.11 — only honor fair_value when it's actually above entry; a
        # fv ≤ entry is noise (same-side fair_value scraped from a consensus
        # where we're already above it) and would produce a misleading
        # "Exit at Xc (fair value)" rule_text when target falls back to e*1.05.
        fv = _norm(fair_value) if fair_value else 0.0
        if fv > e * 1.05:
            target = min(0.97, fv)
            rule_text = f"Exit at {round(target*100)}c (fair value) or -15% stop at {round(max(0.02, e*0.85)*100)}c."
        else:
            target = min(0.97, e * 1.25)
            rule_text = f"Exit at +25% ({round(target*100)}c) or -15% stop at {round(max(0.02, e*0.85)*100)}c."
        stop = max(0.02, e * 0.85)
        target_pct = round(((target / e) - 1) * 100, 1)
        stop_pct = -15.0
        return {
            "target_price": target,
            "stop_price": stop,
            "target_pct": target_pct,
            "stop_pct": stop_pct,
            "hold_to_settlement": False,
            "rule_text": rule_text,
        }

    if k == "WHALE":
        target = min(0.97, e * 1.25)
        stop = max(0.02, e * 0.88)
        return {
            "target_price": target,
            "stop_price": stop,
            "target_pct": 25.0,
            "stop_pct": -12.0,
            "hold_to_settlement": False,
            "rule_text": f"Exit at +25% ({round(target*100)}c) or -12% stop ({round(stop*100)}c). Mirror the whale — exit when they exit.",
        }

    # Default: political-style generous band
    target = min(0.97, e * 1.25)
    stop = max(0.02, e * 0.88)
    return {
        "target_price": target,
        "stop_price": stop,
        "target_pct": 25.0,
        "stop_pct": -12.0,
        "hold_to_settlement": False,
        "rule_text": f"Exit at +25% ({round(target*100)}c) or -12% stop ({round(stop*100)}c).",
    }


def evaluate_exit(
    kind: str,
    entry_price: float,
    current_price: float,
    fair_value: float | None = None,
    days_to_resolution: float | None = None,
) -> tuple[bool, str]:
    """Decide whether a tracked position should trigger an exit alert NOW.

    Returns (should_exit, reason). reason is empty string when not exiting.
    """
    plan = compute_exit_plan(kind, entry_price, fair_value)
    if plan["hold_to_settlement"]:
        # HARVEST/WEATHER: only fire exit if a rare catastrophic drawdown hits,
        # otherwise let the resolution handler close it naturally.
        e = _norm(entry_price)
        c = _norm(current_price)
        if e > 0 and c < max(0.02, e * 0.50):
            return True, f"Catastrophic drop: entry {round(e*100)}c → now {round(c*100)}c (-{round((1-c/e)*100)}%). Thesis likely broken."
        return False, ""

    e = _norm(entry_price)
    c = _norm(current_price)
    if e <= 0 or c <= 0:
        return False, ""

    pnl_pct = ((c / e) - 1) * 100

    # Target hit
    if plan["target_price"] and c >= plan["target_price"]:
        return True, f"Target hit: {round(c*100)}c ≥ {round(plan['target_price']*100)}c (+{round(pnl_pct, 1)}%). {_target_reason(kind)}"

    # Stop hit
    if plan["stop_price"] and c <= plan["stop_price"]:
        return True, f"Stop hit: {round(c*100)}c ≤ {round(plan['stop_price']*100)}c ({round(pnl_pct, 1)}%). Cut losses."

    # Time-decay harvest: up >+20% with <2 days to resolution → take it
    if days_to_resolution is not None and days_to_resolution < 2.0 and pnl_pct > 20.0:
        return True, f"Time-decay harvest: +{round(pnl_pct, 1)}% with <2 days to resolution. Remaining gain not worth weekend-news risk."

    return False, ""


def _target_reason(kind: str) -> str:
    k = (kind or "").upper()
    if k == "MISPRICING":
        return "Edge closed — fair value reached."
    if k == "WHALE":
        return "Profit target — whales rarely hold to resolution."
    return "Profit target reached."
