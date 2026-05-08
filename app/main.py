"""Cortex Signals — Telegram-only service.

v3.22.0 — the webapp was shut down. No HTML, no dashboard, no auth. The
FastAPI process exists for exactly three reasons:
  1. Railway healthcheck (`/health`)
  2. Telegram webhook for inline-keyboard button taps (`/api/polymarket/telegram-webhook`)
  3. Click-tracker for Telegram links (`/api/polymarket/open`)
Everything else runs as background tasks — scan loop, opportunity_alerts,
exit_monitor, paper simulator — and pushes to the user via Telegram.
"""
import asyncio
import logging
import os

from fastapi import FastAPI

from app.database import init_db
from app.routers import polymarket

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

app = FastAPI(
    title="Cortex Signals",
    description="Telegram-only prediction-market signal service",
    version="3.22.1",
)

# Public endpoints only — /open click-tracker + /telegram-webhook.
app.include_router(polymarket.public_router)


@app.on_event("startup")
async def startup():
    await init_db()
    logging.getLogger(__name__).info("Database initialized")

    from app.config import settings as _settings

    if _settings.POLYMARKET_WALLET_TRACKER_ENABLED:
        from app.services.polymarket_wallets import start_wallet_tracker
        start_wallet_tracker()
        logging.getLogger(__name__).info("Polymarket wallet tracker started")

    if _settings.POLYMARKET_WALLET_TRACKER_ENABLED:
        from app.services.pg_store import init_tables as pg_init
        pg_ok = await pg_init()
        if pg_ok:
            logging.getLogger(__name__).info("Simulator: Postgres initialized")
        else:
            logging.getLogger(__name__).warning("Simulator: Postgres unavailable, using SQLite")
        from app.services.polymarket_simulator import init_sim_tables, reset_simulator
        await init_sim_tables()

        # v3.1 one-time cleanup flag
        from app.services import pg_store as _pg
        try:
            pool = await _pg._get_pool()
            if pool:
                async with pool.acquire() as conn:
                    flag = await conn.fetchrow("SELECT 1 FROM sim_state WHERE id = 31")
                    if not flag:
                        await conn.execute(
                            "INSERT INTO sim_state (id, bankroll) VALUES (31, 0) ON CONFLICT (id) DO NOTHING"
                        )
                        await reset_simulator()
                        logging.getLogger(__name__).info("v3.1: Simulator reset")
        except Exception as e:
            logging.getLogger(__name__).warning(f"v3.1 reset check failed: {e}")

        # v3.7 one-time migration: disable overround
        try:
            pool = await _pg._get_pool()
            if pool:
                async with pool.acquire() as conn:
                    flag = await conn.fetchrow("SELECT 1 FROM sim_state WHERE id = 32")
                    if not flag:
                        await conn.execute(
                            "INSERT INTO sim_state (id, bankroll) VALUES (32, 0) ON CONFLICT (id) DO NOTHING"
                        )
                        from app.services.signal_config import set_signal_enabled
                        await set_signal_enabled("overround", False)
                        logging.getLogger(__name__).info("v3.7: disabled Overround/Favorite-Fade")
        except Exception as e:
            logging.getLogger(__name__).warning(f"v3.7 overround disable failed: {e}")

        # v3.9 one-time migration: enable Settlement Harvester
        try:
            pool = await _pg._get_pool()
            if pool:
                async with pool.acquire() as conn:
                    flag = await conn.fetchrow("SELECT 1 FROM sim_state WHERE id = 34")
                    if not flag:
                        await conn.execute(
                            "INSERT INTO sim_state (id, bankroll) VALUES (34, 0) ON CONFLICT (id) DO NOTHING"
                        )
                        from app.services.signal_config import set_signal_enabled
                        await set_signal_enabled("settlement", True)
                        logging.getLogger(__name__).info("v3.9: Settlement Harvester enabled")
        except Exception as e:
            logging.getLogger(__name__).warning(f"v3.9 settlement enable failed: {e}")

        # v3.8 one-time migration: disable momentum + orderbook
        try:
            pool = await _pg._get_pool()
            if pool:
                async with pool.acquire() as conn:
                    flag = await conn.fetchrow("SELECT 1 FROM sim_state WHERE id = 33")
                    if not flag:
                        await conn.execute(
                            "INSERT INTO sim_state (id, bankroll) VALUES (33, 0) ON CONFLICT (id) DO NOTHING"
                        )
                        from app.services.signal_config import set_signal_enabled
                        await set_signal_enabled("momentum", False)
                        await set_signal_enabled("orderbook", False)
                        logging.getLogger(__name__).info("v3.8: disabled Momentum + Orderbook")
        except Exception as e:
            logging.getLogger(__name__).warning(f"v3.8 bleeder disable failed: {e}")

        # v3.20.4 — tracked_alerts table for exit monitor
        try:
            from app.services.exit_monitor import init_table as exit_monitor_init
            await exit_monitor_init()
        except Exception as e:
            logging.getLogger(__name__).warning(f"v3.20.4 exit_monitor init failed: {e}")

        logging.getLogger(__name__).info("Simulator initialized")

    # v3.21.0 — register Telegram webhook for inline-keyboard button taps.
    try:
        public_url = os.getenv("CORTEX_PUBLIC_URL", "").strip().rstrip("/")
        tg_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        if public_url and tg_token:
            from app.services.telegram_notify import set_webhook
            secret = os.getenv("TELEGRAM_WEBHOOK_SECRET", "").strip()
            hook_url = f"{public_url}/api/polymarket/telegram-webhook"
            ok = set_webhook(hook_url, secret_token=secret)
            if ok:
                logging.getLogger(__name__).info(
                    "v3.21.0: Telegram webhook registered at %s", hook_url
                )
            else:
                logging.getLogger(__name__).warning(
                    "v3.21.0: Telegram webhook registration failed (check token/URL)"
                )
    except Exception as e:
        logging.getLogger(__name__).warning(f"v3.21.0 telegram webhook register failed: {e}")

    if _settings.POLYMARKET_WALLET_TRACKER_ENABLED:
        from app.services.polymarket_backtest import start_backtest_loop
        start_backtest_loop()
        logging.getLogger(__name__).info("Backtest auto-loop started")


@app.get("/health")
async def health():
    """Healthcheck endpoint. Used by Railway."""
    return {"status": "ok"}
