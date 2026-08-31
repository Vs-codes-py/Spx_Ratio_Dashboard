"""
Databento Feed Provider.
Two parallel Live connections (Databento requires separate connections per dataset):
  EQUS.MINI   → SPY trades
  OPRA.PILLAR → SPXW SymbolMapping + TCBBO
Bootstraps registry from historical definitions + live symbology mappings.
"""

import time
import logging
import threading
from datetime import datetime, timezone
from typing import Callable, Optional, Any, Dict
from dotenv import load_dotenv
from tz_utils import now_et
from config import Config
from models import UnderlyingTradeEvent, TradeEvent, QuoteEvent, DefinitionEvent
from contract_parser import parse_occ_symbol
from flow_engine import LIVE_MODES

load_dotenv()

try:
    import databento as db
except ImportError:
    db = None

CONNECTION_LIMIT_BACKOFF = 30  # seconds when Databento connection limit is hit


def _ns_to_date(expiration_ns: int) -> str:
    if not expiration_ns:
        return ""
    try:
        return datetime.fromtimestamp(expiration_ns / 1e9, tz=timezone.utc).strftime("%Y-%m-%d")
    except (OSError, ValueError, OverflowError):
        return ""


class FlowProvider:
    """Parallel Databento live provider — one connection per dataset (Databento requirement)."""

    def __init__(self, config: Optional[Config] = None):
        self.config = config or Config()
        self._running = False
        self._spy_thread: Optional[threading.Thread] = None
        self._spx_thread: Optional[threading.Thread] = None
        self._spy_client: Optional[Any] = None
        self._spx_client: Optional[Any] = None
        self._health_lock = threading.RLock()
        self.active_mode = "INITIALIZING"

        self._spy_trade_cb: Optional[Callable[[UnderlyingTradeEvent], None]] = None
        self._spx_trade_cb: Optional[Callable[[TradeEvent], None]] = None
        self._quote_cb: Optional[Callable[[QuoteEvent], None]] = None
        self._def_cb: Optional[Callable[[DefinitionEvent], None]] = None

        self._health: Dict[str, Any] = {
            "connected": False,
            "spy_connected": False,
            "options_connected": False,
            "last_spy_trade": None,
            "last_options_trade": None,
            "last_quote": None,
            "last_definition": None,
            "spy_trades_count": 0,
            "options_trades_count": 0,
            "quotes_count": 0,
            "definitions_count": 0,
            "bootstrapped_count": 0,
            "reconnect_count": 0,
            "last_error": None,
            "connection_started": None,
        }

    def subscribe_spy_trades(self, cb: Callable): self._spy_trade_cb = cb
    def subscribe_trades(self, cb: Callable): self._spx_trade_cb = cb
    def subscribe_quotes(self, cb: Callable): self._quote_cb = cb
    def subscribe_definitions(self, cb: Callable): self._def_cb = cb

    def _safe_historical_end(self, dataset: str) -> str:
        """Get latest available historical end time from Databento metadata."""
        api_key = self.config.databento.API_KEY
        if not db or not api_key:
            return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        try:
            hist = db.Historical(key=api_key)
            info = hist.metadata.get_dataset_range(dataset)
            end = info.get("schema", {}).get("definition", {}).get("end") or info.get("end")
            if end:
                return end.replace("Z", "").split(".")[0]
        except Exception as e:
            logging.warning(f"[FlowProvider] Could not fetch dataset range: {e}")
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    def bootstrap_definitions(self) -> int:
        """Pre-load SPXW contract registry from historical definitions."""
        api_key = self.config.databento.API_KEY
        if not db or not api_key or not self._def_cb:
            return 0

        dataset = self.config.databento.OPTIONS_DATASET
        try:
            hist = db.Historical(key=api_key)
            end = self._safe_historical_end(dataset)
            start = end[:10]

            logging.info(f"[FlowProvider] Bootstrapping registry (start={start}, end={end})...")
            data = hist.timeseries.get_range(
                dataset=dataset,
                schema="definition",
                symbols=self.config.databento.OPTIONS_SYMBOL,
                stype_in="parent",
                start=start,
                end=end,
            )

            count = 0
            for record in data:
                evt = self._record_to_definition(record)
                if evt:
                    self._def_cb(evt)
                    count += 1
                    if count % 5000 == 0:
                        logging.info(f"[FlowProvider] Bootstrapped {count:,} contracts...")

            with self._health_lock:
                self._health["bootstrapped_count"] = count
                self._health["definitions_count"] = max(self._health["definitions_count"], count)

            logging.info(f"[FlowProvider] Bootstrapped {count:,} SPXW contracts")
            return count

        except Exception as e:
            logging.error(f"[FlowProvider] Bootstrap failed: {e} — relying on cache + live symbology")
            with self._health_lock:
                self._health["last_error"] = f"Bootstrap: {e}"
            return 0

    def get_feed_health(self) -> Dict[str, Any]:
        with self._health_lock:
            health = dict(self._health)
            health["active_mode"] = self.active_mode
            health["is_live"] = self.active_mode in LIVE_MODES
            health["latency_ms"] = self._estimate_latency()

            stale_sec = self.config.provider.STALE_FEED_SECONDS
            now = now_et()

            def _age(dt):
                return (now - dt).total_seconds() if dt else None

            spy_age, opt_age = _age(health.get("last_spy_trade")), _age(health.get("last_options_trade"))
            health["spy_age_sec"], health["options_age_sec"] = spy_age, opt_age
            health["spy_stale"] = health["spy_connected"] and (spy_age is None or spy_age > stale_sec)
            health["options_stale"] = health["options_connected"] and (opt_age is None or opt_age > stale_sec)

            health["spy_status"] = (
                "LIVE" if health["spy_connected"] and not health["spy_stale"] and health.get("last_spy_trade")
                else "STALE" if health["spy_connected"] else "OFFLINE"
            )
            health["options_status"] = (
                "LIVE" if health["options_connected"] and not health["options_stale"] and health.get("last_options_trade")
                else "STALE" if health["options_connected"] else "OFFLINE"
            )
            health["connected"] = health["spy_connected"] or health["options_connected"]
            return health

    def _estimate_latency(self) -> Optional[float]:
        with self._health_lock:
            now = time.time()
            candidates = []
            for key in ("last_spy_trade", "last_options_trade", "last_quote"):
                ts = self._health.get(key)
                if ts and isinstance(ts, datetime):
                    candidates.append((now - ts.timestamp()) * 1000.0)
            return round(min(candidates), 1) if candidates else None

    def _update_health(self, **kwargs) -> None:
        with self._health_lock:
            self._health.update(kwargs)

    def _set_active_mode(self) -> None:
        with self._health_lock:
            spy = self._health.get("spy_connected", False)
            opt = self._health.get("options_connected", False)
            if spy and opt:
                self.active_mode = "DATABENTO_LIVE"
            elif spy:
                self.active_mode = "DATABENTO_LIVE_SPY"
            elif opt:
                self.active_mode = "DATABENTO_LIVE_OPTIONS"
            elif self.active_mode not in ("ERROR_NO_LIVE_DATA", "INITIALIZING"):
                self.active_mode = "DISCONNECTED"

    def _backoff_for_error(self, error: Exception) -> float:
        msg = str(error).lower()
        if "connection limit" in msg:
            logging.warning(f"[FlowProvider] Connection limit hit — waiting {CONNECTION_LIMIT_BACKOFF}s")
            return CONNECTION_LIMIT_BACKOFF
        return self.config.provider.RECONNECT_DELAY

    def _close_client(self, client: Any, label: str) -> None:
        if client is None:
            return
        try:
            client.stop()
            logging.info(f"[FlowProvider] Closed {label} connection")
        except Exception as e:
            logging.debug(f"[FlowProvider] Close {label}: {e}")

    def start(self, skip_bootstrap: bool = False) -> None:
        if self._running:
            return
        self._running = True
        self._update_health(connection_started=now_et())
        if not skip_bootstrap:
            self.bootstrap_definitions()

        logging.info("[FlowProvider] Starting parallel feeds: EQUS.MINI (SPY) + OPRA.PILLAR (SPXW TCBBO)")
        self._spx_thread = threading.Thread(target=self._run_spx_feed, daemon=True, name="databento-spxw")
        self._spx_thread.start()
        time.sleep(3)  # SPXW first (critical path), then SPY to reduce connection burst
        self._spy_thread = threading.Thread(target=self._run_spy_feed, daemon=True, name="databento-spy")
        self._spy_thread.start()

    def stop(self) -> None:
        self._running = False
        self._close_client(self._spy_client, "SPY")
        self._close_client(self._spx_client, "SPXW")
        self._spy_client = self._spx_client = None
        for t in (self._spy_thread, self._spx_thread):
            if t and t.is_alive():
                t.join(timeout=3)
        self._update_health(spy_connected=False, options_connected=False, connected=False)
        self.active_mode = "DISCONNECTED"

    def is_alive(self) -> bool:
        spy = self._spy_thread and self._spy_thread.is_alive()
        spx = self._spx_thread and self._spx_thread.is_alive()
        return self._running and (spy or spx)

    def _run_spy_feed(self) -> None:
        api_key = self.config.databento.API_KEY
        if not db or not api_key:
            self.active_mode = "ERROR_NO_LIVE_DATA"
            return

        while self._running:
            client = None
            try:
                logging.info("[FlowProvider] Connecting SPY (EQUS.MINI)...")
                client = db.Live(key=api_key)
                self._spy_client = client
                client.subscribe(
                    dataset=self.config.databento.SPY_DATASET,
                    schema=self.config.databento.SPY_SCHEMA,
                    symbols=self.config.databento.SPY_SYMBOL,
                )
                self._update_health(spy_connected=True, connected=True)
                self._set_active_mode()
                logging.info("[FlowProvider] SPY feed LIVE")

                for record in client:
                    if not self._running:
                        break
                    self._process_spy_trade(record)

            except Exception as e:
                logging.error(f"[FlowProvider] SPY error: {e}")
                self._update_health(spy_connected=False, last_error=str(e),
                                    reconnect_count=self._health.get("reconnect_count", 0) + 1)
                self._set_active_mode()
                if self._running:
                    time.sleep(self._backoff_for_error(e))
            finally:
                self._close_client(client, "SPY")
                if self._spy_client is client:
                    self._spy_client = None
                self._update_health(spy_connected=False)
                self._set_active_mode()

    def _run_spx_feed(self) -> None:
        api_key = self.config.databento.API_KEY
        if not db or not api_key:
            self.active_mode = "ERROR_NO_LIVE_DATA"
            return

        symbol = self.config.databento.OPTIONS_SYMBOL
        dataset = self.config.databento.OPTIONS_DATASET

        while self._running:
            client = None
            try:
                logging.info(f"[FlowProvider] Connecting SPXW ({dataset}, TCBBO)...")
                client = db.Live(key=api_key)
                self._spx_client = client

                client.subscribe(
                    dataset=dataset,
                    schema="definition",
                    symbols=symbol,
                    stype_in="parent",
                )
                client.subscribe(
                    dataset=dataset,
                    schema=self.config.databento.OPTIONS_SCHEMA,
                    symbols=symbol,
                    stype_in="parent",
                )

                self._update_health(options_connected=True, connected=True)
                self._set_active_mode()
                logging.info("[FlowProvider] SPXW feed LIVE")

                for record in client:
                    if not self._running:
                        break
                    self._dispatch_options_record(record)

            except Exception as e:
                logging.error(f"[FlowProvider] SPXW error: {e}")
                self._update_health(options_connected=False, last_error=str(e),
                                    reconnect_count=self._health.get("reconnect_count", 0) + 1)
                self._set_active_mode()
                if self._running:
                    time.sleep(self._backoff_for_error(e))
            finally:
                self._close_client(client, "SPXW")
                if self._spx_client is client:
                    self._spx_client = None
                self._update_health(options_connected=False)
                self._set_active_mode()

    def _record_to_definition(self, record: Any) -> Optional[DefinitionEvent]:
        if not hasattr(record, "instrument_id"):
            return None

        strike = 0.0
        if hasattr(record, "strike_price") and record.strike_price:
            strike = record.strike_price / 1e9

        opt_type = str(getattr(record, "instrument_class", "") or "")
        symbol = getattr(record, "raw_symbol", "") or getattr(record, "stype_out_symbol", "")
        expiration = _ns_to_date(getattr(record, "expiration", 0))

        if strike <= 0 and symbol:
            parsed = parse_occ_symbol(symbol)
            if parsed:
                strike = parsed["strike"]
                opt_type = opt_type or parsed["option_type"]
                expiration = expiration or parsed["expiration"]

        if strike <= 0 or opt_type not in ("C", "P", "CALL", "PUT"):
            return None

        return DefinitionEvent(
            instrument_id=record.instrument_id,
            strike=strike,
            option_type=opt_type,
            symbol=str(symbol).strip(),
            expiration=expiration,
        )

    def _register_definition(self, evt: DefinitionEvent) -> None:
        if self._def_cb:
            self._def_cb(evt)
        with self._health_lock:
            self._health["definitions_count"] += 1
            self._health["last_definition"] = now_et()

    def _dispatch_options_record(self, record: Any) -> None:
        rtype = type(record).__name__

        if rtype == "SystemMsg":
            return

        if rtype == "SymbolMappingMsg":
            symbol = getattr(record, "stype_out_symbol", "") or getattr(record, "stype_in_symbol", "")
            parsed = parse_occ_symbol(symbol)
            if parsed:
                self._register_definition(DefinitionEvent(
                    instrument_id=record.instrument_id,
                    strike=parsed["strike"],
                    option_type=parsed["option_type"],
                    symbol=parsed.get("symbol", symbol),
                    expiration=parsed["expiration"],
                ))
            return

        if rtype == "InstrumentDefMsg" or (hasattr(record, "strike_price") and getattr(record, "strike_price", 0)):
            evt = self._record_to_definition(record)
            if evt:
                self._register_definition(evt)
            return

        if rtype == "TCBBOMsg" or (
            hasattr(record, "price") and hasattr(record, "size") and hasattr(record, "instrument_id")
            and (hasattr(record, "bid_px_00") or (hasattr(record, "levels") and record.levels))
        ):
            self._process_tcbbo(record)

    def _process_spy_trade(self, record: Any) -> None:
        if not (hasattr(record, "price") and hasattr(record, "size")):
            return
        symbol = getattr(record, "symbol", "") or getattr(record, "raw_symbol", "")
        if self.config.databento.SPY_SYMBOL not in str(symbol):
            return

        ts = record.ts_event / 1e9 if hasattr(record, "ts_event") else time.time()
        if self._spy_trade_cb:
            self._spy_trade_cb(UnderlyingTradeEvent(
                symbol=str(symbol), price=record.price / 1e9,
                size=record.size, timestamp=ts,
            ))
        with self._health_lock:
            self._health["spy_trades_count"] += 1
            self._health["last_spy_trade"] = now_et()

    def _process_tcbbo(self, record: Any) -> None:
        ts = record.ts_event / 1e9 if hasattr(record, "ts_event") else time.time()
        inst_id = record.instrument_id

        bid_px = ask_px = bid_sz = ask_sz = 0
        if hasattr(record, "levels") and record.levels:
            top = record.levels[0]
            bid_px, ask_px = top.bid_px / 1e9, top.ask_px / 1e9
            bid_sz, ask_sz = top.bid_sz, top.ask_sz
        elif hasattr(record, "bid_px_00"):
            bid_px = record.bid_px_00 / 1e9
            ask_px = record.ask_px_00 / 1e9
            bid_sz = getattr(record, "bid_sz_00", 0)
            ask_sz = getattr(record, "ask_sz_00", 0)

        now = now_et()
        if bid_px > 0 or ask_px > 0:
            if self._quote_cb:
                self._quote_cb(QuoteEvent(
                    instrument_id=inst_id, bid_price=bid_px, ask_price=ask_px,
                    bid_size=bid_sz, ask_size=ask_sz, timestamp=ts,
                ))
            with self._health_lock:
                self._health["quotes_count"] += 1
                self._health["last_quote"] = now

        if self._spx_trade_cb:
            self._spx_trade_cb(TradeEvent(
                instrument_id=inst_id, price=record.price / 1e9,
                size=record.size, timestamp=ts,
            ))
        with self._health_lock:
            self._health["options_trades_count"] += 1
            self._health["last_options_trade"] = now
