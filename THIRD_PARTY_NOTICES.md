# Third-Party Notices

Cortex Signals installs Python dependencies from `requirements.txt` during deployment. These third-party packages retain their own license terms. The MIT license in this repository applies only to the original project code.

## Runtime Python dependencies

| Package | Version tested | License metadata |
|---------|----------------|------------------|
| FastAPI | 0.136.1 | MIT |
| Uvicorn | 0.46.0 | BSD-3-Clause |
| websockets | 16.0 | BSD-3-Clause |
| aiosqlite | 0.22.1 | MIT |
| Anthropic SDK | 0.100.0 | MIT |
| requests | 2.33.1 | Apache-2.0 |
| python-dotenv | 1.2.2 | BSD-3-Clause |
| asyncpg | 0.31.0 | Apache-2.0 |
| pytz | 2026.2 | MIT |
| httpx | 0.28.1 | BSD-3-Clause |

## Deployment note

If Cortex Signals is packaged or redistributed, include this file plus the full dependency license texts or package metadata required by the relevant third-party licenses. MIT/BSD/ISC-style dependencies generally require preserving copyright and license notices; Apache-2.0 dependencies require preserving the license text and any upstream NOTICE file.
