# SPDX-License-Identifier: MIT

"""Telegram push notifications for ManualShots.

Sends HTML-formatted alerts with a clickable Polymarket link so the user can
tap straight through from the notification into the market to place a manual
trade.

Activated by setting TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID env vars.
If either is missing, all send calls are silent no-ops (safe to leave wired in).
"""
from __future__ import annotations

import html
import logging
import os

import requests

logger = logging.getLogger(__name__)

_API = "https://api.telegram.org/bot{token}/sendMessage"


def _send(text: str, reply_markup: dict | None = None) -> bool:
    """Send a Telegram message. v3.21.0 — optional reply_markup enables
    inline keyboards for callback-button UX (exit-alert [✓ Closed]/[🔕 Snooze]).
    """
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        return False
    try:
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": False,
        }
        if reply_markup:
            payload["reply_markup"] = reply_markup
        r = requests.post(
            _API.format(token=token),
            json=payload,
            timeout=5,
        )
        if r.status_code != 200:
            logger.warning("Telegram send failed %s: %s", r.status_code, r.text[:200])
            return False
        return True
    except Exception as e:
        logger.warning("Telegram send exception: %s", e)
        return False


def answer_callback_query(callback_query_id: str, text: str = "", show_alert: bool = False) -> bool:
    """v3.21.0 — acknowledge a button tap so Telegram stops the spinner on
    the user's phone. Optional text shown as a toast (or alert popup if
    show_alert=True). Must be called within ~15s of the callback or the
    button visually 'hangs' for the user."""
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{token}/answerCallbackQuery",
            json={
                "callback_query_id": callback_query_id,
                "text": text,
                "show_alert": show_alert,
            },
            timeout=5,
        )
        return r.status_code == 200
    except Exception as e:
        logger.warning("answerCallbackQuery failed: %s", e)
        return False


def edit_message_reply_markup(chat_id: str | int, message_id: int, reply_markup: dict | None = None) -> bool:
    """v3.21.0 — replace or strip the inline keyboard on an existing message.
    Pass reply_markup=None to remove the keyboard entirely (used after user
    acts on a button so they don't tap it again)."""
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        return False
    try:
        payload = {"chat_id": chat_id, "message_id": message_id}
        if reply_markup is not None:
            payload["reply_markup"] = reply_markup
        else:
            payload["reply_markup"] = {"inline_keyboard": []}
        r = requests.post(
            f"https://api.telegram.org/bot{token}/editMessageReplyMarkup",
            json=payload,
            timeout=5,
        )
        return r.status_code == 200
    except Exception as e:
        logger.warning("editMessageReplyMarkup failed: %s", e)
        return False


def set_webhook(url: str, secret_token: str = "") -> bool:
    """v3.21.0 — register our webhook URL with Telegram. Called once on
    startup. Telegram will POST callback_query + message updates to this
    URL. secret_token is optional — when set, Telegram includes it in the
    X-Telegram-Bot-Api-Secret-Token header on every call, letting us
    verify authenticity beyond just sender chat_id."""
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not token or not url:
        return False
    try:
        payload: dict = {
            "url": url,
            "allowed_updates": ["callback_query"],
            "drop_pending_updates": True,
        }
        if secret_token:
            payload["secret_token"] = secret_token
        r = requests.post(
            f"https://api.telegram.org/bot{token}/setWebhook",
            json=payload,
            timeout=5,
        )
        if r.status_code != 200:
            logger.warning("setWebhook failed %s: %s", r.status_code, r.text[:200])
            return False
        logger.info("Telegram webhook registered: %s", url)
        return True
    except Exception as e:
        logger.warning("setWebhook exception: %s", e)
        return False


def _link(url: str, label: str = "Open on Polymarket \u2192") -> str:
    if not url:
        return ""
    return f'<a href="{html.escape(url, quote=True)}">{label}</a>'


# v3.20.11 — notify_bet_resolved + notify_daily_digest removed. Signals-only
# mode (v3.20.7+) has no user-facing paper P&L notifications by design. The
# paper simulator is invisible training data driving auto-pruner/calibration,
# never a channel that pushes fake-money wins/losses to the user's phone.

