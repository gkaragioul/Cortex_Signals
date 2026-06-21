# SPDX-License-Identifier: MIT

import aiosqlite
from app.config import settings

DB_PATH = settings.DATABASE_PATH

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS sim_bets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    market TEXT NOT NULL,
    market_slug TEXT,
    side TEXT NOT NULL,
    entry_price REAL NOT NULL,
    bet_amount REAL NOT NULL,
    shares REAL NOT NULL,
    signal_source TEXT,
    signal_detail TEXT,
    score REAL DEFAULT 0,
    status TEXT DEFAULT 'open',
    exit_price REAL,
    pnl REAL,
    resolved_at TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS sim_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    bankroll REAL DEFAULT 1000.0,
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS sim_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL,
    bankroll REAL NOT NULL,
    created_at TEXT DEFAULT (datetime('now'))
);
"""


async def get_db() -> aiosqlite.Connection:
    db = await aiosqlite.connect(DB_PATH)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA journal_mode=WAL")
    return db


async def init_db():
    db = await get_db()
    try:
        await db.executescript(SCHEMA_SQL)

        # Seed sim_state if empty
        cursor = await db.execute("SELECT COUNT(*) FROM sim_state")
        count = (await cursor.fetchone())[0]
        if count == 0:
            await db.execute("INSERT INTO sim_state (id, bankroll) VALUES (1, 1000.0)")

        await db.commit()
    finally:
        await db.close()
