from dataclasses import dataclass
from typing import Union
from enum import Enum


class OptionType(Enum):
    CALL = "CALL"
    PUT = "PUT"


@dataclass
class UnderlyingTradeEvent:
    """Isolated underlying stock trade event (e.g., SPY)."""
    symbol: str
    price: float
    size: int
    timestamp: Union[float, int]


@dataclass
class TradeEvent:
    """Options contract trade event — resolved ONLY via instrument_id in registry."""
    instrument_id: int
    price: float
    size: int
    timestamp: Union[float, int]


@dataclass
class QuoteEvent:
    """Options contract top-of-book quote event."""
    instrument_id: int
    bid_price: float
    bid_size: int
    ask_price: float
    ask_size: int
    timestamp: Union[float, int]


@dataclass
class DefinitionEvent:
    """Instrument definition event for permanent contract registry."""
    instrument_id: int
    strike: float
    option_type: str
    symbol: str
    expiration: str = ""
