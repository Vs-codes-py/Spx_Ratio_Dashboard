"""
Application Orchestrator & Entrypoint.
Pipeline: SPY → SpotEstimator | TCBBO → Registry → Classifier → FlowEngine
"""

import sys
import time
import signal
import logging
import threading
from datetime import datetime
from typing import Dict, Any

from config import Config
from spot_estimator import SpotEstimator
from flow_engine import FlowEngine, LIVE_MODES
from sentiment_engine import SentimentEngine
from history_logger import HistoryLogger
from feed_provider import FlowProvider
from trade_classifier import TradeClassifier
from registry_cache import load_registry, save_registry
from models import UnderlyingTradeEvent, TradeEvent, QuoteEvent, DefinitionEvent


class TerminalOrchestrator:
    """Master Orchestrator — all option trades resolve via instrument_id registry."""

    def __init__(self):
        self.running = False
        self.config = Config()
        self.spot_estimator = SpotEstimator(config=self.config)
        self.flow_engine = FlowEngine(config=self.config)
        self.sentiment_engine = SentimentEngine(
            flow_engine=self.flow_engine,
            spot_estimator=self.spot_estimator,
            config=self.config,
        )
        self.history_logger = HistoryLogger(
            config=self.config,
            flow_engine=self.flow_engine,
            sentiment_engine=self.sentiment_engine,
            spot_estimator=self.spot_estimator,
        )
        self.provider = FlowProvider(config=self.config)
        self.classifier = TradeClassifier(config=self.config)
        self._last_snapshot_time: float = 0.0
        self._spot_thread = None

    def _sync_underlying_to_engine(self) -> None:
        session = self.spot_estimator.get_session_stats()
        self.flow_engine.update_underlying(
            spy_price=self.spot_estimator.get_spy(),
            estimated_spx=self.spot_estimator.get_estimated_spx(),
            atm_strike=self.spot_estimator.get_atm(),
            prev_close=session["prev_close"],
            day_high=session["day_high"],
            day_low=session["day_low"],
        )

    def initialize(self) -> None:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
        logging.info("[System] Initializing SPX Order Flow Orchestrator (STRICT LIVE MODE)...")
        # One baseline fetch only. The live SPY feed keeps SPX updated continuously;
        # a lightweight 60-second refresh thread also refreshes the SPX baseline.
        self.spot_estimator.fetch_live_prices()
        self._sync_underlying_to_engine()
        self.register_callbacks()

        # Try disk cache first (fast startup)
        cached = load_registry(self.flow_engine.register_contract)
        if cached > 0:
            logging.info(f"[System] Loaded {cached:,} contracts from cache")
        else:
            # Historical bootstrap happens in provider.start()
            pass

    def register_callbacks(self) -> None:
        def on_spy_trade(spy_event: UnderlyingTradeEvent):
            self.spot_estimator.update_spy(spy_event.price)
            self._sync_underlying_to_engine()

        def on_quote(quote_event: QuoteEvent):
            self.flow_engine.update_quote(quote_event)

        def on_spx_trade(trade_event: TradeEvent):
            # TCBBO quote already cached by on_quote; classify using NBBO
            quote = self.flow_engine.get_latest_quote(trade_event.instrument_id)
            quote_matched = quote is not None and quote.bid_price > 0 and quote.ask_price > 0
            side = self.classifier.classify(trade_event, quote)
            self.flow_engine.add_trade(trade_event, side, quote_matched=quote_matched)

        def on_definition(def_event: DefinitionEvent):
            self.flow_engine.register_contract(
                instrument_id=def_event.instrument_id,
                strike=def_event.strike,
                option_type=def_event.option_type,
                symbol=def_event.symbol,
                expiration=def_event.expiration,
            )

        self.provider.subscribe_spy_trades(on_spy_trade)
        self.provider.subscribe_trades(on_spx_trade)
        self.provider.subscribe_quotes(on_quote)
        self.provider.subscribe_definitions(on_definition)

    def get_feed_status(self) -> Dict[str, Any]:
        health = self.provider.get_feed_health()
        stats = self.flow_engine.statistics(feed_health=health)
        active_mode = self.provider.active_mode
        is_live = active_mode in LIVE_MODES

        spy_status = health.get("spy_status", "OFFLINE")
        opt_status = health.get("options_status", "OFFLINE")

        if active_mode == "ERROR_NO_LIVE_DATA":
            status_label, status_class = "LIVE FEED OFFLINE", "offline"
        elif spy_status == "STALE" or opt_status == "STALE":
            status_label, status_class = "LIVE FEED STALE", "waiting"
        elif is_live and spy_status == "LIVE" and opt_status == "LIVE":
            status_label, status_class = "LIVE (DATABENTO)", "live"
        elif is_live:
            status_label, status_class = "LIVE — AWAITING DATA", "waiting"
        else:
            status_label, status_class = "LIVE FEED OFFLINE", "offline"

        def fmt_ts(dt):
            if dt is None:
                return "—"
            if isinstance(dt, datetime):
                return dt.strftime("%H:%M:%S.%f")[:-3] + " ET"
            return str(dt)

        def status_icon(s):
            return {"LIVE": "🟢", "STALE": "🟡", "OFFLINE": "🔴"}.get(s, "🔴")

        return {
            **stats,
            "status_label": status_label,
            "status_class": status_class,
            "is_live": is_live,
            "has_spot": self.spot_estimator.get_estimated_spx() > 0,
            "has_trades": self.flow_engine.trades_received > 0,
            "active_mode": active_mode,
            "last_spy_trade_fmt": fmt_ts(health.get("last_spy_trade")),
            "last_options_trade_fmt": fmt_ts(health.get("last_options_trade")),
            "spy_status": spy_status,
            "options_status": opt_status,
            "spy_status_icon": status_icon(spy_status),
            "options_status_icon": status_icon(opt_status),
            "spy_dataset": self.config.databento.SPY_DATASET,
            "options_dataset": self.config.databento.OPTIONS_DATASET,
            "options_symbol": self.config.databento.OPTIONS_SYMBOL,
            "options_schema": self.config.databento.OPTIONS_SCHEMA,
            "spy_trades_count": health.get("spy_trades_count", 0),
            "options_trades_count": health.get("options_trades_count", 0),
            "definitions_count": health.get("definitions_count", 0),
            "bootstrapped_count": health.get("bootstrapped_count", 0),
            "registry_count": len(self.flow_engine.contracts),
        }

    def maybe_save_snapshot(self, interval_sec: int = 60) -> None:
        now = time.time()
        if now - self._last_snapshot_time >= interval_sec:
            self.history_logger.save_snapshot()
            self._last_snapshot_time = now

    def _spot_refresh_loop(self) -> None:
        # Initial baseline is fetched during initialize(); wait one full interval
        # before the first periodic network refresh.
        time.sleep(self.config.spx_estimation.UPDATE_INTERVAL)
        while self.running:
            started = time.time()
            try:
                self.spot_estimator.fetch_live_prices()
                self._sync_underlying_to_engine()
            except Exception as e:
                logging.warning(f"[System] Periodic SPX refresh failed: {e}")
            elapsed = time.time() - started
            time.sleep(max(1.0, self.config.spx_estimation.UPDATE_INTERVAL - elapsed))

    def start(self) -> None:
        self.running = True
        self._spot_thread = threading.Thread(
            target=self._spot_refresh_loop, daemon=True, name="spx-periodic-refresh"
        )
        self._spot_thread.start()
        skip = len(self.flow_engine.contracts) >= 100
        if skip:
            logging.info(f"[System] Skipping bootstrap — {len(self.flow_engine.contracts):,} contracts from cache")
        self.provider.start(skip_bootstrap=skip)
        count = len(self.flow_engine.contracts)
        if count > 0:
            save_registry(self.flow_engine.contracts)
        logging.info(f"[System] Live engine online. Registry: {count:,} contracts.")

    def reconnect(self) -> None:
        """Stop feed, re-bootstrap registry, restart live connection."""
        logging.info("[System] Reconnecting live feed...")
        self.provider.stop()
        time.sleep(3)
        self.provider.start()
        count = len(self.flow_engine.contracts)
        if count > 0:
            save_registry(self.flow_engine.contracts)
        logging.info(f"[System] Reconnected. Registry: {count:,} contracts.")

    def stop(self) -> None:
        self.running = False
        self.provider.stop()
        self.history_logger.flush()
        logging.info("[System] Shutdown complete.")


def create_application() -> TerminalOrchestrator:
    app = TerminalOrchestrator()
    app.initialize()
    return app


if __name__ == "__main__":
    app = create_application()
    app.start()

    def signal_handler(sig, frame):
        app.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    while app.running:
        app.maybe_save_snapshot()
        time.sleep(1)
