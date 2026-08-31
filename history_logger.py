"""
History Logger.
Periodically persists live order flow metrics and sentiment statistics.
"""

import os
import time
import json
import logging
from typing import Dict, Any, List, Optional

from tz_utils import et_time_str


class HistoryLogger:
    """Logs snapshot metrics for model validation and historical audit."""

    def __init__(self, config=None, flow_engine=None, sentiment_engine=None, spot_estimator=None):
        self.config = config
        self.flow_engine = flow_engine
        self.sentiment_engine = sentiment_engine
        self.spot_estimator = spot_estimator
        self.history: List[Dict[str, Any]] = []
        self.log_dir = "logs"
        os.makedirs(self.log_dir, exist_ok=True)

    def save_snapshot(self, timeframe: str = '1m') -> Optional[Dict[str, Any]]:
        if not self.flow_engine:
            return None

        snapshot = self.flow_engine.get_dashboard_snapshot(timeframe=timeframe)
        sentiment = (
            self.sentiment_engine.calculate_sentiment()
            if self.sentiment_engine else {}
        )

        record = {
            "timestamp": et_time_str(snapshot["timestamp"], "%Y-%m-%d %H:%M:%S") + " ET",
            "spy_price": snapshot["spy_price"],
            "estimated_spx": snapshot["estimated_spx"],
            "atm_strike": snapshot["atm_strike"],
            "call_buy": snapshot["call_buy"],
            "call_sell": snapshot["call_sell"],
            "put_buy": snapshot["put_buy"],
            "put_sell": snapshot["put_sell"],
            "unknown_volume": snapshot["unknown_volume"],
            "call_put_ratio": snapshot["call_put_ratio"],
            "bullish_volume": snapshot["bullish_volume"],
            "bearish_volume": snapshot["bearish_volume"],
            "quote_coverage": snapshot["quote_coverage"],
            "trades_received": snapshot["trades_received"],
            "contract_count": snapshot["contract_count"],
            "sentiment": sentiment.get("sentiment", "N/A"),
            "confidence": sentiment.get("confidence", 0.0),
        }

        self.history.append(record)
        logging.info(f"[HistoryLogger] Snapshot saved. Total records: {len(self.history)}")
        return record

    def flush(self) -> None:
        if not self.history:
            return
        logging.info("[HistoryLogger] Flushing logs to persistent storage...")
        out_path = os.path.join(self.log_dir, "history_snapshots.json")
        try:
            with open(out_path, "w") as f:
                json.dump(self.history, f, indent=2)
            logging.info(f"[HistoryLogger] Successfully persisted {len(self.history)} records to {out_path}")
        except Exception as e:
            logging.error(f"[HistoryLogger] Error flushing history logs: {e}")