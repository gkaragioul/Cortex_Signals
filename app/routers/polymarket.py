# SPDX-License-Identifier: MIT

"""Cortex public endpoints — Telegram webhook + click tracker.

v3.22.0 — the dashboard API (recommendations, wallets, mispricing, simulator,
backtest, all-signals, settled-bets, calibration, exit-alerts, market-detail,
walls, signal-config, etc.) was removed with the webapp. The background
engines still read the same data and push it via Telegram. Only these two
auth-free endpoints remain:
  /open              — click tracker (redirects to Polymarket, stamps row)
  /telegram-webhook  — inline-keyboard button handler (close / snooze)
"""

import os

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse, PlainTextResponse


public_router = APIRouter(prefix="/api/polymarket", tags=["polymarket-public"])


@public_router.get("/open")
async def track_open_redirect(
    slug: str,
    side: str = "YES",
    kind: str = "DEFAULT",
    redirect: str = "",
    market: str = "",
    event_slug: str = "",
    entry_price: float | None = None,
):
    """v3.20.8 — click tracker for Polymarket links.

    Telegram alert links route through this endpoint. Stamps user_opened_at
    on the tracked_alerts row (or creates one) so exit_monitor only fires
    EXIT alerts for positions the user actually clicked to open — never for
    ignored Telegram alerts. Redirects 302 to the Polymarket URL.

    Open-redirect guard: only redirects to polymarket.com hosts."""
    from app.services.exit_monitor import mark_or_create_opened

    redirect_safe = ""
    if redirect:
        r_lower = redirect.lower()
        if r_lower.startswith("https://polymarket.com/") or r_lower.startswith("http://polymarket.com/"):
            redirect_safe = redirect
        else:
            return PlainTextResponse("invalid redirect target", status_code=400)

    try:
        await mark_or_create_opened(
            slug=slug, side=side, kind=kind,
            market=market or None, event_slug=event_slug or None,
            entry_price=entry_price,
            url=redirect_safe or None,
        )
    except Exception:
        # Tracking failure must never block the redirect.
        pass

    if redirect_safe:
        return RedirectResponse(redirect_safe, status_code=302)
    return PlainTextResponse("ok")


@public_router.post("/telegram-webhook")
async def telegram_webhook(request: Request):
    """v3.21.0 — receive Telegram callback_query updates for inline-keyboard
    button taps on exit alerts. Auth-free by design (Telegram's servers POST
    here cookieless); security via (1) optional secret_token matched against
    X-Telegram-Bot-Api-Secret-Token header, (2) callback_query.from.id
    matched against TELEGRAM_CHAT_ID env.

    Callback data grammar:
      c:{row_id}  → close (status='closed_by_user')
      z:{row_id}  → snooze 24h (bump exit_alerted_at)
    """
    from app.services.telegram_notify import answer_callback_query, edit_message_reply_markup
    from app.services.exit_monitor import acknowledge_exit, snooze_exit, get_row_summary

    expected_secret = os.getenv("TELEGRAM_WEBHOOK_SECRET", "").strip()
    if expected_secret:
        got = request.headers.get("x-telegram-bot-api-secret-token", "")
        if got != expected_secret:
            return PlainTextResponse("forbidden", status_code=403)

    try:
        update = await request.json()
    except Exception:
        return PlainTextResponse("bad json", status_code=400)

    cb = update.get("callback_query") if isinstance(update, dict) else None
    if not cb:
        return {"ok": True}

    expected_chat = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    sender_id = str((cb.get("from") or {}).get("id") or "")
    if expected_chat and sender_id != expected_chat:
        answer_callback_query(cb.get("id", ""), text="Not authorized", show_alert=False)
        return {"ok": True}

    data = str(cb.get("data") or "")
    parts = data.split(":", 1)
    if len(parts) != 2:
        answer_callback_query(cb.get("id", ""), text="Unknown action")
        return {"ok": True}

    action, raw_id = parts[0], parts[1]
    try:
        row_id = int(raw_id)
    except ValueError:
        answer_callback_query(cb.get("id", ""), text="Bad id")
        return {"ok": True}

    row = await get_row_summary(row_id)
    if not row:
        answer_callback_query(cb.get("id", ""), text="Position not found")
        return {"ok": True}

    market_short = (row.get("market") or "")[:40] or row.get("slug", "")[:40]

    if action == "c":
        ok = await acknowledge_exit(row_id)
        toast = f"✓ Closed: {market_short}" if ok else "Close failed"
    elif action == "z":
        ok = await snooze_exit(row_id)
        toast = f"🔕 Snoozed 24h: {market_short}" if ok else "Snooze failed"
    else:
        toast = "Unknown action"

    answer_callback_query(cb.get("id", ""), text=toast)

    msg = cb.get("message") or {}
    chat_id = (msg.get("chat") or {}).get("id")
    message_id = msg.get("message_id")
    if chat_id and message_id:
        edit_message_reply_markup(chat_id, message_id, reply_markup=None)

    return {"ok": True}
