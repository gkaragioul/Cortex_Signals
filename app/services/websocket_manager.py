import asyncio
import json
import logging
from collections import deque
from datetime import datetime, timezone
from fastapi import WebSocket

logger = logging.getLogger(__name__)


class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []
        # Buffers for history backfill when new clients connect
        self._log_history: deque = deque(maxlen=200)
        self._news_history: deque = deque(maxlen=100)

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"WebSocket connected. Total: {len(self.active_connections)}")
        # Send buffered history to the new client
        try:
            for msg in self._log_history:
                await websocket.send_json(msg)
            for msg in self._news_history:
                await websocket.send_json(msg)
        except Exception:
            pass

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        logger.info(f"WebSocket disconnected. Total: {len(self.active_connections)}")

    async def broadcast(self, message: dict):
        """Send a message to all connected clients and buffer for history."""
        # Buffer logs and news for new client backfill
        msg_type = message.get("type")
        if msg_type == "log":
            self._log_history.append(message)
        elif msg_type == "news":
            self._news_history.append(message)

        dead = []
        for conn in self.active_connections:
            try:
                await conn.send_json(message)
            except Exception:
                dead.append(conn)
        for conn in dead:
            self.disconnect(conn)

    async def send_price_update(self, symbol: str, data: dict):
        await self.broadcast({"type": "price", "symbol": symbol, "data": data})

    async def send_trade_update(self, trade: dict):
        await self.broadcast({"type": "trade", "data": trade})

    async def send_position_update(self, positions: list):
        await self.broadcast({"type": "positions", "data": positions})

    async def send_account_update(self, account: dict):
        await self.broadcast({"type": "account", "data": account})

    async def send_bot_status(self, status: dict):
        await self.broadcast({"type": "bot_status", "data": status})

    async def send_log(self, message: str, level: str = "info"):
        # Server-side timestamp so the dashboard can show true event time.
        # Without this, the client stamps at receive/render time, causing all
        # log lines in a refresh burst to share the same clock second.
        ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
        await self.broadcast({"type": "log", "data": {"message": message, "level": level, "ts": ts}})

    async def send_signal(self, signal: dict):
        await self.broadcast({"type": "signal", "data": signal})


class DebugLogManager:
    """Manages debug WebSocket connections and streams all Python log output."""

    def __init__(self):
        self.connections: list[WebSocket] = []
        self.buffer: deque = deque(maxlen=1000)  # Keep last 1000 log lines
        self._loop: asyncio.AbstractEventLoop | None = None

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.connections.append(websocket)
        # Send buffered history
        for entry in self.buffer:
            try:
                await websocket.send_json(entry)
            except Exception:
                break

    def disconnect(self, websocket: WebSocket):
        if websocket in self.connections:
            self.connections.remove(websocket)

    def push_log(self, record: logging.LogRecord):
        """Called synchronously from the logging handler. Queues async broadcast."""
        entry = {
            "ts": datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3],
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "file": f"{record.filename}:{record.lineno}",
        }
        if record.exc_text:
            entry["exception"] = record.exc_text

        self.buffer.append(entry)

        # Schedule async broadcast if we have an event loop
        if self.connections:
            try:
                loop = self._loop or asyncio.get_event_loop()
                if loop.is_running():
                    loop.create_task(self._broadcast(entry))
            except RuntimeError:
                pass

    async def _broadcast(self, entry: dict):
        dead = []
        for conn in self.connections:
            try:
                await conn.send_json(entry)
            except Exception:
                dead.append(conn)
        for conn in dead:
            self.disconnect(conn)

    def set_loop(self, loop: asyncio.AbstractEventLoop):
        self._loop = loop


class WebSocketLogHandler(logging.Handler):
    """Python logging handler that pushes all log records to debug WebSocket clients."""

    def __init__(self, debug_manager: 'DebugLogManager'):
        super().__init__()
        self.debug_manager = debug_manager

    def emit(self, record: logging.LogRecord):
        try:
            self.debug_manager.push_log(record)
        except Exception:
            pass


ws_manager = ConnectionManager()
debug_log_manager = DebugLogManager()
