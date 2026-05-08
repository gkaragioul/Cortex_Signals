from pydantic import BaseModel
from typing import Optional


class SimBet(BaseModel):
    id: Optional[int] = None
    timestamp: str
    market: str
    market_slug: Optional[str] = None
    side: str
    entry_price: float
    bet_amount: float
    shares: float
    signal_source: Optional[str] = None
    signal_detail: Optional[str] = None
    score: float = 0
    status: str = "open"
    exit_price: Optional[float] = None
    pnl: Optional[float] = None
    resolved_at: Optional[str] = None
