<h1 align="center">Cortex Signals</h1>

<p align="center">
  <strong>Experimental Telegram-only prediction-market signals for Polymarket opportunities.</strong><br>
  <em>Background intelligence, entry alerts, exit guidance, and click tracking without a dashboard.</em>
</p>

<p align="center">
  <a href="#what-it-does">What It Does</a> &bull;
  <a href="#public-http-surface">Endpoints</a> &bull;
  <a href="#runtime-behavior">Runtime</a> &bull;
  <a href="#configuration">Config</a> &bull;
  <a href="#safety-notice">Safety</a> &bull;
  <a href="#license">License</a>
</p>

---

> This is an abandoned/archived experimental project shared for learning and reference. It is not financial advice, not a trading recommendation engine, and not production-ready software.

## What it does

- Scans prediction-market opportunities in background tasks
- Sends entry and exit signals to Telegram
- Tracks opened links so exit alerts are only generated for positions the user actually opened
- Handles Telegram inline buttons for close/snooze actions
- Maintains paper/simulation data for calibration and signal pruning

## Public HTTP surface

Only these routes are intentionally exposed:

| Route | Purpose |
|-------|---------|
| `GET /health` | Host health check |
| `GET /api/polymarket/open` | Click tracker that redirects to Polymarket |
| `POST /api/polymarket/telegram-webhook` | Telegram callback handler for inline buttons |

All dashboard pages and legacy trading APIs were removed in v3.22.

## Runtime behavior

Cortex runs background workers when `POLYMARKET_WALLET_TRACKER_ENABLED=true`:

- Wallet and market scanning
- Opportunity Telegram alerts
- Exit monitoring for tracked positions
- Historical backtest loop
- Paper simulator and calibration storage

The current codebase is signals-only. It does not include live-wallet execution, live order submission endpoints, private-key handling, or a dashboard UI.

## Requirements

- Python 3.11+
- An ASGI host such as Railway, Render, Fly.io, or a local Uvicorn process
- Telegram bot token and chat ID for notifications
- PostgreSQL via `DATABASE_URL` for persistent production storage

## Local development

```bash
git clone https://github.com/karagioules/Cortex_Signals.git
cd Cortex_Signals
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
copy .env.example .env
.\.venv\Scripts\python.exe -m uvicorn app.main:app --host 127.0.0.1 --port 8000
```

For safe local smoke tests without starting scanner/backtest background loops:

```powershell
$env:POLYMARKET_WALLET_TRACKER_ENABLED="false"
.\.venv\Scripts\python.exe -m uvicorn app.main:app --host 127.0.0.1 --port 8000
```

## Configuration

| Variable | Required | Description |
|----------|----------|-------------|
| `DATABASE_URL` | Production | PostgreSQL connection string |
| `ANTHROPIC_API_KEY` | Optional | Enables AI analysis where configured |
| `TELEGRAM_BOT_TOKEN` | Yes for Telegram | Telegram bot token |
| `TELEGRAM_CHAT_ID` | Yes for Telegram | Authorized Telegram chat/user ID |
| `CORTEX_PUBLIC_URL` | Yes for Telegram webhook | Public app URL |
| `TELEGRAM_WEBHOOK_SECRET` | Optional | Secret token checked on Telegram webhook calls |
| `POLYMARKET_WALLET_TRACKER_ENABLED` | Optional | Enables background scanner/backtest loops; defaults to `false` |
| `POLYMARKET_SCAN_INTERVAL` | Optional | Scan interval in seconds |
| `POLYMARKET_MIN_EDGE_PCT` | Optional | Minimum edge threshold |
| `POLYMARKET_MAX_RESULTS` | Optional | Maximum returned/scored opportunities |
| `TAVILY_API_KEY` | Optional | Enhances news-speed signal research |
| `NTFY_TOPIC` | Optional | Legacy notification integration |

## Privacy and network behavior

Cortex stores operational data in SQLite locally or PostgreSQL in production. Depending on enabled features, it may call third-party services including Polymarket/Gamma APIs, Telegram, Anthropic, Reddit public JSON endpoints, Tavily, weather APIs, and PostgreSQL. Telegram messages may include market titles, signal scores, entry/exit guidance, and Polymarket links.

No user-facing analytics SDK, browser tracker, or dashboard telemetry is included in the current Telegram-only app.

## Safety notice

This repository is educational and experimental. Prediction markets and trading systems can lose money, behave unexpectedly, or produce incorrect signals. Review the code, use paper/simulation modes, protect secrets, and do not rely on this project as financial advice.

If you previously ran a private copy of this project, rotate any API keys, Telegram tokens, database credentials, or deployment passwords before publishing or sharing forks.

## Third-party notices

Python dependencies are installed from `requirements.txt` during deployment. See [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md) for dependency license notes.

## Security

See [SECURITY.md](SECURITY.md) for reporting and secret-handling guidance.

## License

MIT. See [LICENSE](LICENSE) for the full terms.
