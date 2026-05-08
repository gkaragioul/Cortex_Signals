"""v3.20.4 — Exit monitor for manual Cortex positions.

When opportunity_alerts fires a Telegram entry alert, we auto-track the
(slug, side, kind, entry_price, fair_value) in Postgres. Every scan cycle
this module polls the current CLOB mid-price for every open tracked alert
and asks exit_advisor.evaluate_exit whether to ping the user. When yes,
we fire a Telegram EXIT alert with target/stop/reason and mark the row
`exit_alerted` so it doesn't re-fire for 24h.

When the underlying market closes (Gamma API returns closed=true), we
mark the row `resolved` and stop polling — lets HARVEST/WEATHER positions
close naturally at settlement without manual intervention.

Postgres-only. If pg_store is unavailable the feature degrades silently:
entries don't get tracked, no exits fire, paper path unaffected.
"""

from __future__ import annotations

import json
import logging
import os
import time

import httpx

from app.services import pg_store
from app.services.exit_advisor import compute_exit_plan, evaluate_exit, _norm

logger = logging.getLogger(__name__)

POLYMARKET_GAMMA_SLUG_URL = "https://gamma-api.polymarket.com/markets?slug="

# How long to suppress re-alerts on the same (slug, side) after an exit
# alert fires. User may take a while to actually close the position.
EXIT_COOLDOWN_SECONDS = 24 * 3600

# Max positions to check per scan cycle (stops runaway cost if table bloats)
MAX_POSITIONS_PER_SCAN = 50


async def init_table() -> None:
    """One-time idempotent schema bootstrap. Safe to re-run on every startup."""
    try:
        pool = await pg_store._get_pool()
        if not pool:
            logger.warning("exit_monitor: no Postgres pool — tracking disabled")
            return
        async with pool.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS tracked_alerts (
                    id SERIAL PRIMARY KEY,
                    slug TEXT NOT NULL,
                    side TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    market TEXT,
                    event_slug TEXT,
                    entry_price NUMERIC,
                    fair_value NUMERIC,
                    url TEXT,
                    status TEXT DEFAULT 'open',
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    exit_alerted_at TIMESTAMPTZ,
                    resolved_at TIMESTAMPTZ,
                    last_price NUMERIC,
                    last_checked_at TIMESTAMPTZ,
                    last_exit_reason TEXT
                )
                """
            )
            # v3.20.5 migration: older deploys created the table without
            # last_exit_reason. Add if missing; no-op on fresh deploy.
            await conn.execute(
                "ALTER TABLE tracked_alerts ADD COLUMN IF NOT EXISTS last_exit_reason TEXT"
            )
            # v3.20.8 migration: user_opened_at gates exit monitoring to only
            # positions the user actually clicked to open. NULL = untracked
            # (alert fired but user never engaged); set = user hit the
            # /api/polymarket/open tracking URL from Telegram or dashboard.
            await conn.execute(
                "ALTER TABLE tracked_alerts ADD COLUMN IF NOT EXISTS user_opened_at TIMESTAMPTZ"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS tracked_alerts_status_idx "
                "ON tracked_alerts(status, created_at DESC)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS tracked_alerts_slug_side_idx "
                "ON tracked_alerts(slug, side, status)"
            )
        logger.info("exit_monitor: tracked_alerts table ready")
    except Exception as e:
        logger.warning(f"exit_monitor.init_table failed: {e}")


# v3.20.11 — track_alert removed. Since v3.20.8 tracking happens only at
# click-time via mark_or_create_opened (below); fire-and-forget pre-tracking
# on every Telegram alert send was replaced by explicit user-intent gating.


async def _fetch_market(slug: str) -> dict | None:
    """Fetch a single Gamma market payload. Returns None on failure."""
    try:
        async with httpx.AsyncClient(timeout=8.0) as cli:
            r = await cli.get(POLYMARKET_GAMMA_SLUG_URL + slug)
            if r.status_code != 200:
                return None
            data = r.json()
            if isinstance(data, list) and data:
                return data[0]
            return None
    except Exception:
        return None


def _extract_current_price(market: dict, side: str) -> float | None:
    """Return current mid-price for the given side as 0-1 proportion.

    Polymarket's Gamma API returns outcomePrices as a JSON-encoded list
    like '["0.25","0.75"]' — index 0 is YES, index 1 is NO.
    """
    try:
        raw = market.get("outcomePrices") or "[]"
        prices = json.loads(raw) if isinstance(raw, str) else raw
        if not prices or len(prices) < 2:
            return None
        idx = 0 if (side or "YES").upper() == "YES" else 1
        return float(prices[idx])
    except Exception:
        return None


def _days_to_end(market: dict) -> float | None:
    try:
        from datetime import datetime, timezone
        end = market.get("endDate")
        if not end:
            return None
        dt = datetime.fromisoformat(end.replace("Z", "+00:00"))
        delta = (dt - datetime.now(timezone.utc)).total_seconds() / 86400.0
        return max(0.0, delta)
    except Exception:
        return None


def _telegram_send(msg: str, row_id: int | None = None) -> None:
    """v3.21.0 — exit messages now carry inline-keyboard action buttons.
    Row id is embedded in callback_data so the webhook handler knows which
    tracked_alerts row the user is acting on."""
    try:
        from app.services.telegram_notify import _send
        reply_markup = None
        if row_id is not None:
            reply_markup = {
                "inline_keyboard": [[
                    {"text": "✓ Closed", "callback_data": f"c:{row_id}"},
                    {"text": "🔕 Snooze 24h", "callback_data": f"z:{row_id}"},
                ]]
            }
        _send(msg, reply_markup=reply_markup)
    except Exception as e:
        logger.warning(f"exit_monitor telegram send failed: {e}")


def _format_exit_msg(row: dict, current_price: float, reason: str, market: dict | None) -> str:
    """Build the Telegram EXIT message."""
    entry = _norm(row.get("entry_price"))
    cur = _norm(current_price)
    pnl_pct = ((cur / entry) - 1) * 100 if entry > 0 else 0.0
    pnl_sign = "+" if pnl_pct >= 0 else ""
    market_name = (row.get("market") or "?")[:80]
    kind = row.get("kind") or "?"
    side = row.get("side") or "?"
    url = row.get("url") or ""

    lines = [
        f"🔔 <b>EXIT · {kind}</b>  <i>{pnl_sign}{round(pnl_pct, 1)}%</i>",
        f"<b>{market_name}</b>",
        f"<b>Side:</b> {side}",
        f"<b>Entry:</b> {round(entry * 100)}c  →  <b>Now:</b> {round(cur * 100)}c",
        f"<b>Reason:</b> {reason}",
    ]
    if url:
        lines.append(f'\n<a href="{url}">Close on Polymarket</a>')
    return "\n".join(lines)


async def check_exit_triggers() -> int:
    """Scan all open tracked positions, fire EXIT alerts where triggered.

    Called from the main scan loop in polymarket_wallets.py every cycle.
    Returns count of exits fired this cycle. Never raises.
    """
    try:
        pool = await pg_store._get_pool()
        if not pool:
            return 0

        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, slug, side, kind, market, event_slug, entry_price,
                       fair_value, url, exit_alerted_at
                FROM tracked_alerts
                WHERE status = 'open' AND user_opened_at IS NOT NULL
                ORDER BY created_at DESC
                LIMIT $1
                """,
                MAX_POSITIONS_PER_SCAN,
            )

        fired = 0
        now = time.time()

        for r in rows:
            row = dict(r)
            slug = row["slug"]
            side = row["side"]

            market = await _fetch_market(slug)
            if market is None:
                continue

            # Auto-close if market resolved — stop polling, no alert needed
            if market.get("closed"):
                try:
                    async with pool.acquire() as conn:
                        await conn.execute(
                            "UPDATE tracked_alerts SET status='resolved', resolved_at=NOW() WHERE id=$1",
                            row["id"],
                        )
                except Exception:
                    pass
                continue

            cur_price = _extract_current_price(market, side)
            if cur_price is None:
                continue

            # Always update last_checked_at / last_price so dashboard can
            # show "last seen" in future UI additions.
            try:
                async with pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE tracked_alerts SET last_price=$1, last_checked_at=NOW() WHERE id=$2",
                        cur_price, row["id"],
                    )
            except Exception:
                pass

            # Respect cooldown
            if row["exit_alerted_at"]:
                elapsed = now - row["exit_alerted_at"].timestamp()
                if elapsed < EXIT_COOLDOWN_SECONDS:
                    continue

            days = _days_to_end(market)
            should_exit, reason = evaluate_exit(
                kind=row["kind"],
                entry_price=row["entry_price"],
                current_price=cur_price,
                fair_value=row.get("fair_value"),
                days_to_resolution=days,
            )
            if not should_exit:
                continue

            msg = _format_exit_msg(row, cur_price, reason, market)
            _telegram_send(msg, row_id=row["id"])

            try:
                async with pool.acquire() as conn:
                    # Store the reason so the dashboard EXIT NOW card can
                    # show the same text the Telegram push used.
                    await conn.execute(
                        "UPDATE tracked_alerts SET exit_alerted_at=NOW(), last_exit_reason=$1 WHERE id=$2",
                        reason, row["id"],
                    )
            except Exception:
                pass

            logger.info(f"exit_monitor: fired EXIT for {row['kind']} {side} {slug[:40]}: {reason}")
            fired += 1

        return fired
    except Exception as e:
        logger.warning(f"exit_monitor.check_exit_triggers failed: {e}")
        return 0


async def get_open_tracked() -> list[dict]:
    """Expose open tracked positions for dashboard / API use."""
    try:
        pool = await pg_store._get_pool()
        if not pool:
            return []
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, slug, side, kind, market, entry_price, fair_value,
                       last_price, created_at, exit_alerted_at, url
                FROM tracked_alerts
                WHERE status = 'open'
                ORDER BY created_at DESC
                LIMIT 100
                """
            )
        return [dict(r) for r in rows]
    except Exception:
        return []


async def get_triggered_exits() -> list[dict]:
    """v3.20.5 — positions where an EXIT was fired but user hasn't ack'd yet.

    Drives the "🚨 EXIT NOW" section at the top of the Opportunities tab.
    Row included if exit_alerted_at IS NOT NULL AND status='open' (i.e.
    the scan loop fired a Telegram exit, the market is still open, and
    the user hasn't clicked "Mark as closed" yet).
    """
    try:
        pool = await pg_store._get_pool()
        if not pool:
            return []
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, slug, side, kind, market, event_slug, entry_price,
                       fair_value, last_price, created_at, exit_alerted_at,
                       last_exit_reason, url
                FROM tracked_alerts
                WHERE status = 'open'
                  AND exit_alerted_at IS NOT NULL
                  AND user_opened_at IS NOT NULL
                ORDER BY exit_alerted_at DESC
                LIMIT 50
                """
            )
        out = []
        for r in rows:
            d = dict(r)
            # Serialize timestamps + decimals for JSON safety
            for k in ("created_at", "exit_alerted_at"):
                if d.get(k):
                    d[k] = d[k].isoformat()
            for k in ("entry_price", "fair_value", "last_price"):
                if d.get(k) is not None:
                    d[k] = float(d[k])
            out.append(d)
        return out
    except Exception as e:
        logger.warning(f"exit_monitor.get_triggered_exits failed: {e}")
        return []


async def mark_or_create_opened(
    slug: str,
    side: str = "YES",
    kind: str = "DEFAULT",
    market: str | None = None,
    entry_price: float | None = None,
    event_slug: str | None = None,
    url: str | None = None,
) -> dict:
    """v3.20.8 — user clicked the Polymarket link → record that they
    actually opened this position. Only rows with user_opened_at set are
    monitored for exits; everything else is dormant entry alerts the user
    never acted on.

    Behavior:
      - If a 'closed_by_user' row exists for (slug, side): no-op. User
        already said they're done with this market — don't resurrect it.
      - If an open row exists with user_opened_at IS NULL: stamp NOW().
      - If an open row exists already marked: no-op (already tracked).
      - If no row exists: INSERT a new tracked row with user_opened_at=NOW().

    Never raises. Returns {"status": ..., "id": ...} for debugging.
    """
    try:
        if not slug:
            return {"status": "noop", "reason": "empty slug"}
        side = (side or "YES").upper()
        kind = (kind or "DEFAULT").upper()
        pool = await pg_store._get_pool()
        if not pool:
            return {"status": "noop", "reason": "no pg pool"}

        async with pool.acquire() as conn:
            # closed_by_user is sticky — user acked, don't reopen
            closed = await conn.fetchrow(
                "SELECT id FROM tracked_alerts WHERE slug=$1 AND side=$2 "
                "AND status='closed_by_user' LIMIT 1",
                slug, side,
            )
            if closed:
                return {"status": "closed_by_user", "id": closed["id"]}

            existing = await conn.fetchrow(
                "SELECT id, user_opened_at FROM tracked_alerts "
                "WHERE slug=$1 AND side=$2 AND status='open' LIMIT 1",
                slug, side,
            )
            if existing:
                if existing["user_opened_at"] is None:
                    await conn.execute(
                        "UPDATE tracked_alerts SET user_opened_at=NOW() WHERE id=$1",
                        existing["id"],
                    )
                    return {"status": "marked", "id": existing["id"]}
                return {"status": "already_open", "id": existing["id"]}

            # No row exists yet — user clicked a dashboard card before a
            # Telegram alert fired (or alert was dedup'd out). Create one.
            row = await conn.fetchrow(
                """
                INSERT INTO tracked_alerts
                    (slug, side, kind, market, event_slug, entry_price, url, user_opened_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
                RETURNING id
                """,
                slug, side, kind,
                (market or "")[:200],
                event_slug or "",
                float(entry_price) if entry_price is not None else None,
                url or "",
            )
            return {"status": "created", "id": row["id"] if row else None}
    except Exception as e:
        logger.warning(f"exit_monitor.mark_or_create_opened failed: {e}")
        return {"status": "error", "error": str(e)}


async def snooze_exit(row_id: int) -> bool:
    """v3.21.0 — bump exit_alerted_at so the 24h cooldown restarts.
    User tapped '🔕 Snooze 24h' in Telegram — they've seen the exit signal
    and want to decide later, without the same message re-firing."""
    try:
        pool = await pg_store._get_pool()
        if not pool:
            return False
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE tracked_alerts SET exit_alerted_at=NOW() WHERE id=$1 AND status='open'",
                int(row_id),
            )
        return True
    except Exception as e:
        logger.warning(f"exit_monitor.snooze_exit failed: {e}")
        return False


async def get_row_summary(row_id: int) -> dict | None:
    """v3.21.0 — fetch minimal info for a tracked_alerts row so the webhook
    handler can show a confirmation toast like '✓ Closed: Ukraine ceasefire'."""
    try:
        pool = await pg_store._get_pool()
        if not pool:
            return None
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT id, slug, side, kind, market, status FROM tracked_alerts WHERE id=$1",
                int(row_id),
            )
        return dict(row) if row else None
    except Exception:
        return None


async def acknowledge_exit(row_id: int) -> bool:
    """User clicked 'Mark as closed' on the EXIT NOW card.

    Flips status to 'closed_by_user'. Position won't re-fire or show up
    in get_triggered_exits again.
    """
    try:
        pool = await pg_store._get_pool()
        if not pool:
            return False
        async with pool.acquire() as conn:
            result = await conn.execute(
                "UPDATE tracked_alerts SET status='closed_by_user', resolved_at=NOW() WHERE id=$1 AND status='open'",
                int(row_id),
            )
        # asyncpg returns "UPDATE N" string; simplest to always return True
        # if no exception (UI doesn't differentiate "not found" from success)
        return True
    except Exception as e:
        logger.warning(f"exit_monitor.acknowledge_exit failed: {e}")
        return False
