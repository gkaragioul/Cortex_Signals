# Security Policy

## Supported status

Cortex Signals is an archived experimental project. It is shared publicly for reference and learning, not as maintained production infrastructure.

## Reporting a vulnerability

The repository is archived and read-only on GitHub, so issues and pull requests are closed and no security fixes will be released. If you find a problem, fix it in your own fork. Never publish live secrets, private keys, tokens, database URLs, or personal account details.

## Secret handling

Do not commit `.env` files or real credentials. Rotate any key that has ever been committed, pasted into issue trackers, included in logs, or used in a repository that later becomes public.

Before running a fork, create fresh credentials for:

- Telegram bot token
- Anthropic API key
- PostgreSQL or hosting database credentials
- Tavily or other optional API keys
- Deployment passwords or webhook secrets

## Financial risk

This project is not financial advice and does not guarantee accurate signals, profitable outcomes, or correct market data. Run it only with credentials and accounts you are prepared to protect and monitor yourself.
