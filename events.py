"""
Defines shared immutable event data structures used across modules.
"""

from models import OptionType, UnderlyingTradeEvent, TradeEvent, QuoteEvent, DefinitionEvent

from dataclasses import dataclass
from datetime import datetime

@dataclass
class FlowEvent:
    timestamp: datetime
    strike: float
    option_type: str
    side: str
    size: int
    price: float
    notional: float