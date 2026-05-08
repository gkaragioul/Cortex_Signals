import os
from dotenv import load_dotenv

load_dotenv()


class Settings:
    ANTHROPIC_API_KEY: str = os.getenv("ANTHROPIC_API_KEY", "")

    DATABASE_PATH: str = os.getenv("DATABASE_PATH", "trading_bot.db")
    HOST: str = "0.0.0.0"
    PORT: int = int(os.getenv("PORT", "8000"))

    # Polymarket intelligence
    POLYMARKET_ENABLED: bool = os.getenv("POLYMARKET_ENABLED", "true").lower() == "true"
    POLYMARKET_WALLET_TRACKER_ENABLED: bool = os.getenv("POLYMARKET_WALLET_TRACKER_ENABLED", "false").lower() == "true"
    POLYMARKET_SIM_ENABLED: bool = os.getenv("POLYMARKET_SIM_ENABLED", "true").lower() == "true"
    POLYMARKET_SCAN_INTERVAL: int = int(os.getenv("POLYMARKET_SCAN_INTERVAL", "600"))  # 10 min (was 30 — edges close in 2-5 min)
    POLYMARKET_MIN_EDGE_PCT: float = float(os.getenv("POLYMARKET_MIN_EDGE_PCT", "12.0"))
    POLYMARKET_MAX_RESULTS: int = int(os.getenv("POLYMARKET_MAX_RESULTS", "10"))

    # Push notifications (ntfy.sh)
    NTFY_TOPIC: str = os.getenv("NTFY_TOPIC", "")

    # Tavily web search (optional — enhances news speed signal with full article text)
    # Free tier: 1000 searches/month. Get key at tavily.com
    TAVILY_API_KEY: str = os.getenv("TAVILY_API_KEY", "")


settings = Settings()
