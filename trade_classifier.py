"""
Options Trade Classifier.
Classifies execution side using strict priority: Bid/Ask -> Midpoint -> Tick Rule -> UNKNOWN.
"""

from enum import Enum
from typing import Optional, Dict
from models import TradeEvent, QuoteEvent


class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"
    UNKNOWN = "UNKNOWN"


class TradeClassifier:
    """Determines aggressor side for options trade prints."""

    def __init__(self, config=None):
        self.config = config
        self._last_price_map: Dict[int, float] = {}  # instrument_id -> last_trade_price

    def classify(self, trade: TradeEvent, quote: Optional[QuoteEvent] = None) -> Side:
        """
        Multi-tier aggressor classification:
        1. Quote Bid/Ask boundaries
        2. Quote Midpoint comparison
        3. Tick Rule comparison (vs previous trade price)
        4. UNKNOWN fallback (retains volume accounting)
        """
        price = trade.price
        inst_id = trade.instrument_id

        # Priority 1 & 2: NBBO Quote matching
        if quote is not None and quote.bid_price > 0 and quote.ask_price > 0:
            # Priority 1: Direct Bid/Ask hits
            if price >= quote.ask_price:
                self._last_price_map[inst_id] = price
                return Side.BUY
            if price <= quote.bid_price:
                self._last_price_map[inst_id] = price
                return Side.SELL

            # Priority 2: Midpoint
            midpoint = (quote.bid_price + quote.ask_price) / 2.0
            if price > midpoint:
                self._last_price_map[inst_id] = price
                return Side.BUY
            if price < midpoint:
                self._last_price_map[inst_id] = price
                return Side.SELL

        # Priority 3: Tick Rule
        prev_price = self._last_price_map.get(inst_id)
        self._last_price_map[inst_id] = price

        if prev_price is not None and prev_price > 0:
            if price > prev_price:
                return Side.BUY
            if price < prev_price:
                return Side.SELL

        # Priority 4: Unknown
        return Side.UNKNOWN