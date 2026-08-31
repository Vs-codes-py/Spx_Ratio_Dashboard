"""
Flow Engine & Central Market State.
Permanent contract registry (instrument_id → strike/C-P/expiry).
Full-chain strike matrix aggregated from ALL live SPXW contracts.
STRICT LIVE MODE: No synthetic fallback values.
"""

import time
import threading
from copy import deepcopy
from collections import deque, defaultdict
from datetime import datetime
from typing import Dict, Any, Optional, List
from models import TradeEvent, QuoteEvent
from tz_utils import et_time_str


LIVE_MODES = frozenset({
    "DATABENTO_LIVE",
    "DATABENTO_LIVE_SPY",
    "DATABENTO_LIVE_OPTIONS",
})


class FlowEngine:
    """Central state store and volume aggregator for all SPXW contracts."""

    def __init__(self, config=None):
        self.config = config
        self._lock = threading.RLock()

        self.spy_price: float = 0.0
        self.estimated_spx: float = 0.0
        self.atm_strike: float = 0.0
        self.prev_close: float = 0.0
        self.day_high: float = 0.0
        self.day_low: float = 0.0

        # Permanent contract registry: instrument_id → {strike, option_type, expiration, symbol}
        self.contracts: Dict[int, Dict[str, Any]] = {}
        self.strike_instruments = defaultdict(lambda: {"C": [], "P": []})
        self._best_quotes_by_strike_type = {}
        self._last_prices_by_strike_type = {}

        self.quote_cache: Dict[int, Dict[str, Any]] = {}
        self._last_trade_price: Dict[int, float] = {}
        self.strike_metrics: Dict[float, Dict[str, Dict[str, float]]] = {}
        self.recent_trades: deque = deque(maxlen=500)

        self.trades_received: int = 0
        self.trades_unregistered: int = 0
        self.quote_matched: int = 0
        self.quote_missing: int = 0
        self.unknown_count: int = 0

        self._trade_timestamps: deque = deque(maxlen=1000)
        self._quote_timestamps: deque = deque(maxlen=1000)
        self._stats_start_time: float = time.time()

        # Rolling, NON-CUMULATIVE flow.  Trades are aggregated into 1-minute
        # buckets so dashboard reads are O(number of buckets), not O(number
        # of individual option trades).
        self._minute_buckets = defaultdict(self._empty_flow_bucket)
        self._minute_strike_buckets = defaultdict(lambda: defaultdict(self._empty_flow_bucket))
        self._bucket_retention_minutes = 24 * 60
        self._ratio_history = defaultdict(lambda: deque(maxlen=500))
        self._ratio_last_boundary = {}
        self._ratio_snapshots = {}
        self._ratio_last_trade_count = {}
        self._matrix_snapshots = {}
        self._active_timeframe = "1m"

        # Session cumulative flow. These values only increase as real trades
        # arrive and are reset automatically when the New York trading date changes.
        self._session_date = None
        self._cumulative_flow = self._empty_flow_bucket()

    @staticmethod
    def _empty_flow_bucket():
        return {
            "C": {"buy": 0.0, "sell": 0.0, "unknown": 0.0},
            "P": {"buy": 0.0, "sell": 0.0, "unknown": 0.0},
        }

    @staticmethod
    def _timeframe_seconds(timeframe: str) -> int:
        return {"1m": 60, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600}.get(
            str(timeframe).lower(), 60
        )

    def set_active_timeframe(self, timeframe: str) -> None:
        timeframe = str(timeframe).lower()
        if timeframe not in {"1m", "5m", "15m", "30m", "1h"}:
            timeframe = "1m"
        with self._lock:
            self._active_timeframe = timeframe
            self._record_ratio_snapshot_locked(timeframe, force=True)

    def _prune_buckets_locked(self, now: float) -> None:
        cutoff = int(now // 60) - self._bucket_retention_minutes
        old = [k for k in self._minute_buckets if k < cutoff]
        for k in old:
            del self._minute_buckets[k]
            self._minute_strike_buckets.pop(k, None)

    def _session_date_for_timestamp(self, ts: float):
        try:
            from zoneinfo import ZoneInfo
            return datetime.fromtimestamp(ts, tz=ZoneInfo("America/New_York")).date()
        except Exception:
            return datetime.fromtimestamp(ts).date()

    def _ensure_session_locked(self, ts: float) -> None:
        session_date = self._session_date_for_timestamp(ts)
        if self._session_date == session_date:
            return
        self._session_date = session_date
        self._cumulative_flow = self._empty_flow_bucket()
        self._ratio_history.clear()
        self._ratio_last_boundary.clear()
        self._ratio_snapshots.clear()
        self._ratio_last_trade_count.clear()
        self._matrix_snapshots.clear()

    @staticmethod
    def _ratios_from_flow(flow: Dict[str, float]) -> Dict[str, float]:
        cb, cs = flow["call_buy"], flow["call_sell"]
        pb, ps = flow["put_buy"], flow["put_sell"]
        call_total = cb + cs + flow.get("call_unknown", 0.0)
        put_total = pb + ps + flow.get("put_unknown", 0.0)
        return {
            "call_buy_sell_ratio": cb / cs if cs > 0 else 0.0,
            "put_buy_sell_ratio": pb / ps if ps > 0 else 0.0,
            "call_put_ratio": call_total / put_total if put_total > 0 else 0.0,
        }

    def _flow_snapshot_record_locked(self, timeframe: str, now: float, boundary: int) -> Dict[str, Any]:
        flow = self._window_flow_locked(timeframe, now)
        ratios = self._ratios_from_flow(flow)
        cumulative = self._cumulative_flow
        cumulative_flat = {
            "call_buy": cumulative["C"]["buy"],
            "call_sell": cumulative["C"]["sell"],
            "call_unknown": cumulative["C"]["unknown"],
            "put_buy": cumulative["P"]["buy"],
            "put_sell": cumulative["P"]["sell"],
            "put_unknown": cumulative["P"]["unknown"],
        }
        cumulative_ratios = self._ratios_from_flow(cumulative_flat)
        previous = self._ratio_history[timeframe][-1] if self._ratio_history[timeframe] else None
        record = {
            "timestamp": now,
            "boundary": boundary,
            "timeframe": timeframe,
            "call_buy": flow["call_buy"],
            "call_sell": flow["call_sell"],
            "put_buy": flow["put_buy"],
            "put_sell": flow["put_sell"],
            "call_unknown": flow["call_unknown"],
            "put_unknown": flow["put_unknown"],
            "call_buy_sell_ratio": round(ratios["call_buy_sell_ratio"], 4),
            "put_buy_sell_ratio": round(ratios["put_buy_sell_ratio"], 4),
            "call_put_ratio": round(ratios["call_put_ratio"], 4),
            "cumulative_call_buy": cumulative_flat["call_buy"],
            "cumulative_call_sell": cumulative_flat["call_sell"],
            "cumulative_put_buy": cumulative_flat["put_buy"],
            "cumulative_put_sell": cumulative_flat["put_sell"],
            "cumulative_call_buy_sell_ratio": round(cumulative_ratios["call_buy_sell_ratio"], 4),
            "cumulative_put_buy_sell_ratio": round(cumulative_ratios["put_buy_sell_ratio"], 4),
            "cumulative_call_put_ratio": round(cumulative_ratios["call_put_ratio"], 4),
            "call_ratio_change": round(ratios["call_buy_sell_ratio"] - (previous["call_buy_sell_ratio"] if previous else ratios["call_buy_sell_ratio"]), 4),
            "put_ratio_change": round(ratios["put_buy_sell_ratio"] - (previous["put_buy_sell_ratio"] if previous else ratios["put_buy_sell_ratio"]), 4),
            "call_put_ratio_change": round(ratios["call_put_ratio"] - (previous["call_put_ratio"] if previous else ratios["call_put_ratio"]), 4),
        }
        return record

    def _record_ratio_snapshot_locked(self, timeframe: str, force: bool = False) -> None:
        timeframe = str(timeframe).lower()
        sec = self._timeframe_seconds(timeframe)
        now = time.time()
        self._ensure_session_locked(now)
        boundary = int(now // sec) * sec

        # The selected timeframe is the model clock. A rolling model snapshot is
        # recalculated once per timeframe boundary, but only when at least one
        # new real option trade has arrived. This prevents the model from
        # changing after the market closes merely because the wall clock moves.
        current_trade_count = self.trades_received
        if not force:
            if self._ratio_last_boundary.get(timeframe) == boundary:
                return
            if self._ratio_last_trade_count.get(timeframe) == current_trade_count:
                return

        record = self._flow_snapshot_record_locked(timeframe, now, boundary)
        self._ratio_history[timeframe].append(record)
        self._ratio_snapshots[timeframe] = record
        # Freeze the per-strike matrix at the exact same model snapshot.
        self._matrix_snapshots[timeframe] = deepcopy(self._window_strike_metrics_locked(timeframe, now=now))
        self._ratio_last_boundary[timeframe] = boundary
        self._ratio_last_trade_count[timeframe] = current_trade_count

    def _cached_or_current_flow_locked(self, timeframe: str) -> Dict[str, float]:
        self._record_ratio_snapshot_locked(timeframe)
        snap = self._ratio_snapshots.get(timeframe)
        if snap is None:
            return self._window_flow_locked(timeframe)
        return {
            "call_buy": snap["call_buy"], "call_sell": snap["call_sell"],
            "call_unknown": snap["call_unknown"], "put_buy": snap["put_buy"],
            "put_sell": snap["put_sell"], "put_unknown": snap["put_unknown"],
        }

    def _window_flow_locked(self, timeframe: str, now: Optional[float] = None) -> Dict[str, float]:
        now = now or time.time()
        seconds = self._timeframe_seconds(timeframe)
        cutoff_minute = int(now // 60) - (seconds // 60) + 1
        # Include current minute and all minutes touched by the rolling window.
        keys = range(cutoff_minute, int(now // 60) + 1)
        totals = {"call_buy": 0.0, "call_sell": 0.0, "call_unknown": 0.0,
                  "put_buy": 0.0, "put_sell": 0.0, "put_unknown": 0.0}
        for k in keys:
            b = self._minute_buckets.get(k)
            if not b:
                continue
            totals["call_buy"] += b["C"]["buy"]
            totals["call_sell"] += b["C"]["sell"]
            totals["call_unknown"] += b["C"]["unknown"]
            totals["put_buy"] += b["P"]["buy"]
            totals["put_sell"] += b["P"]["sell"]
            totals["put_unknown"] += b["P"]["unknown"]
        return totals

    def get_timeframe_flow(self, timeframe: str = "1m") -> Dict[str, float]:
        with self._lock:
            return self._cached_or_current_flow_locked(timeframe)

    def get_ratio_history(self, timeframe: str = "1m", limit: int = 120) -> List[Dict[str, Any]]:
        with self._lock:
            self._record_ratio_snapshot_locked(timeframe)
            return list(self._ratio_history[timeframe])[-limit:]

    def get_cumulative_summary(self) -> Dict[str, float]:
        with self._lock:
            self._ensure_session_locked(time.time())
            c = self._cumulative_flow["C"]
            p = self._cumulative_flow["P"]
            call_total = c["buy"] + c["sell"] + c["unknown"]
            put_total = p["buy"] + p["sell"] + p["unknown"]
            return {
                "call_buy": c["buy"], "call_sell": c["sell"], "put_buy": p["buy"], "put_sell": p["sell"],
                "call_buy_sell_ratio": c["buy"] / c["sell"] if c["sell"] > 0 else 0.0,
                "put_buy_sell_ratio": p["buy"] / p["sell"] if p["sell"] > 0 else 0.0,
                "call_put_ratio": call_total / put_total if put_total > 0 else 0.0,
            }

    def register_contract(
        self,
        instrument_id: int,
        strike: float,
        option_type: str,
        symbol: str = "",
        expiration: str = "",
    ) -> None:
        """Register contract in permanent registry keyed by instrument_id."""
        with self._lock:
            if instrument_id in self.contracts:
                return  # already registered
            opt_type_str = self._normalize_option_type(option_type)

            self.contracts[instrument_id] = {
                "strike": float(strike),
                "option_type": opt_type_str,
                "symbol": symbol,
                "expiration": expiration,
            }
            self.strike_instruments[float(strike)][opt_type_str].append(instrument_id)

            if strike not in self.strike_metrics:
                self.strike_metrics[strike] = self._empty_strike_metrics()

    @staticmethod
    def _normalize_option_type(option_type: str) -> str:
        if hasattr(option_type, 'value'):
            raw = option_type.value
        else:
            raw = str(option_type).upper()
        if raw in ("CALL", "C"):
            return "C"
        if raw in ("PUT", "P"):
            return "P"
        return raw

    @staticmethod
    def _empty_strike_metrics() -> Dict[str, Dict[str, float]]:
        empty = {"buy_volume": 0.0, "sell_volume": 0.0, "unknown_volume": 0.0, "total_volume": 0.0}
        return {"C": dict(empty), "P": dict(empty)}

    def update_quote(self, quote: QuoteEvent) -> None:
        with self._lock:
            ts = quote.timestamp if isinstance(quote.timestamp, (int, float)) else time.time()
            q = {
                "bid": quote.bid_price,
                "ask": quote.ask_price,
                "bid_size": quote.bid_size,
                "ask_size": quote.ask_size,
                "timestamp": ts,
            }
            self.quote_cache[quote.instrument_id] = q
            contract = self.contracts.get(quote.instrument_id)
            if contract:
                key = (contract["strike"], contract["option_type"])
                current = self._best_quotes_by_strike_type.get(key)
                if current is None or ts >= current.get("timestamp", 0):
                    self._best_quotes_by_strike_type[key] = q
            self._quote_timestamps.append(time.time())

    def get_latest_quote(self, instrument_id: int, max_age_ms: Optional[int] = None) -> Optional[QuoteEvent]:
        lookback_ms = max_age_ms or getattr(
            getattr(self.config, 'provider', None), 'QUOTE_LOOKBACK_MS', 500
        )
        with self._lock:
            q = self.quote_cache.get(instrument_id)
            if not q:
                return None
            now = time.time()
            age_ms = (now - q["timestamp"]) * 1000.0
            if age_ms <= lookback_ms or lookback_ms <= 0:
                return QuoteEvent(
                    instrument_id=instrument_id,
                    bid_price=q["bid"],
                    ask_price=q["ask"],
                    bid_size=q["bid_size"],
                    ask_size=q["ask_size"],
                    timestamp=q["timestamp"],
                )
            return None

    def update_underlying(
        self, spy_price: float, estimated_spx: float, atm_strike: float,
        prev_close: float = 0.0, day_high: float = 0.0, day_low: float = 0.0,
    ) -> None:
        with self._lock:
            if spy_price > 0:
                self.spy_price = spy_price
            if estimated_spx > 0:
                self.estimated_spx = estimated_spx
            if atm_strike > 0:
                self.atm_strike = atm_strike
            if prev_close > 0:
                self.prev_close = prev_close
            if day_high > 0:
                self.day_high = day_high
            if day_low > 0:
                self.day_low = day_low

    def add_trade(self, trade: TradeEvent, side: str, quote_matched: bool = True) -> None:
        """
        Aggregate trade volume. Contract MUST exist in registry.
        Never reads strike/C-P from the trade record itself.
        """
        with self._lock:
            contract = self.contracts.get(trade.instrument_id)
            if not contract:
                self.trades_unregistered += 1
                return

            self.trades_received += 1
            self._trade_timestamps.append(time.time())
            self._last_trade_price[trade.instrument_id] = trade.price

            if quote_matched:
                self.quote_matched += 1
            else:
                self.quote_missing += 1

            strike = contract["strike"]
            opt_type = contract["option_type"]
            self._last_prices_by_strike_type[(strike, opt_type)] = trade.price

            if strike not in self.strike_metrics:
                self.strike_metrics[strike] = self._empty_strike_metrics()

            metrics = self.strike_metrics[strike][opt_type]
            size = float(trade.size)
            metrics["total_volume"] += size

            # Session cumulative totals are updated only by actual classified
            # live trades. They never decay when the rolling window moves.
            self._ensure_session_locked(trade.timestamp if isinstance(trade.timestamp, (int, float)) else time.time())
            cumulative_field = "buy" if side == "BUY" else "sell" if side == "SELL" else "unknown"
            self._cumulative_flow[opt_type][cumulative_field] += size

            if side == "BUY":
                metrics["buy_volume"] += size
            elif side == "SELL":
                metrics["sell_volume"] += size
            else:
                metrics["unknown_volume"] += size
                self.unknown_count += 1

            # O(1) update of the rolling minute bucket.
            event_ts = trade.timestamp if isinstance(trade.timestamp, (int, float)) else time.time()
            bucket_key = int(event_ts // 60)
            bucket = self._minute_buckets[bucket_key]
            field = "buy" if side == "BUY" else "sell" if side == "SELL" else "unknown"
            bucket[opt_type][field] += size
            strike_bucket = self._minute_strike_buckets[bucket_key][strike]
            strike_bucket[opt_type][field] += size
            self._prune_buckets_locked(event_ts)

            ts = event_ts
            t_str = et_time_str(ts, "%H:%M:%S")
            type_label = "CALL" if opt_type == "C" else "PUT"
            self.recent_trades.appendleft({
                "Time": t_str,
                "Strike": int(strike),
                "Type": type_label,
                "Side": side,
                "Size": int(size),
                "Price": f"${trade.price:.2f}",
                "Notional": f"${size * trade.price * 100:,.0f}",
                "Symbol": contract.get("symbol", ""),
                "Expiry": contract.get("expiration", ""),
            })

    def _instruments_for_strike_type(self, strike: float, opt_type: str) -> List[int]:
        return self.strike_instruments.get(float(strike), {}).get(opt_type, [])

    def _best_quote_for_strike_type(self, strike: float, opt_type: str) -> Dict[str, Any]:
        return self._best_quotes_by_strike_type.get((float(strike), opt_type), {})

    def _latest_price_for_strike_type(self, strike: float, opt_type: str) -> Optional[float]:
        return self._last_prices_by_strike_type.get((float(strike), opt_type))

    def _all_strikes(self) -> List[float]:
        """All strikes from registry + any with accumulated volume."""
        strikes = set(c["strike"] for c in self.contracts.values())
        strikes.update(self.strike_metrics.keys())
        return sorted(strikes)

    def get_dashboard_snapshot(self, timeframe: str = "1m") -> Dict[str, Any]:
        with self._lock:
            flow = self._cached_or_current_flow_locked(timeframe)

            cb = flow["call_buy"]; cs = flow["call_sell"]
            pb = flow["put_buy"]; ps = flow["put_sell"]
            cu = flow["call_unknown"]; pu = flow["put_unknown"]
            call_total = cb + cs + cu
            put_total = pb + ps + pu
            total = call_total + put_total
            quote_coverage = (
                self.quote_matched / self.trades_received * 100.0
                if self.trades_received > 0 else 0.0
            )
            return {
                "timestamp": time.time(),
                "timeframe": timeframe,
                "spy_price": self.spy_price,
                "estimated_spx": self.estimated_spx,
                "atm_strike": self.atm_strike,
                "prev_close": self.prev_close,
                "day_high": self.day_high,
                "day_low": self.day_low,
                "call_buy": cb, "call_sell": cs, "call_unknown": cu, "call_volume": call_total,
                "put_buy": pb, "put_sell": ps, "put_unknown": pu, "put_volume": put_total,
                "bullish_volume": cb + ps,
                "bearish_volume": cs + pb,
                "unknown_volume": cu + pu,
                "total_volume": total,
                "call_put_ratio": call_total / put_total if put_total > 0 else 0.0,
                "cumulative_call_buy": self._cumulative_flow["C"]["buy"],
                "cumulative_call_sell": self._cumulative_flow["C"]["sell"],
                "cumulative_put_buy": self._cumulative_flow["P"]["buy"],
                "cumulative_put_sell": self._cumulative_flow["P"]["sell"],
                "cumulative_call_buy_sell_ratio": (self._cumulative_flow["C"]["buy"] / self._cumulative_flow["C"]["sell"] if self._cumulative_flow["C"]["sell"] > 0 else 0.0),
                "cumulative_put_buy_sell_ratio": (self._cumulative_flow["P"]["buy"] / self._cumulative_flow["P"]["sell"] if self._cumulative_flow["P"]["sell"] > 0 else 0.0),
                "cumulative_call_put_ratio": ((self._cumulative_flow["C"]["buy"] + self._cumulative_flow["C"]["sell"] + self._cumulative_flow["C"]["unknown"]) / (self._cumulative_flow["P"]["buy"] + self._cumulative_flow["P"]["sell"] + self._cumulative_flow["P"]["unknown"]) if (self._cumulative_flow["P"]["buy"] + self._cumulative_flow["P"]["sell"] + self._cumulative_flow["P"]["unknown"]) > 0 else 0.0),
                "quote_coverage": quote_coverage,
                "trades_received": self.trades_received,
                "trades_unregistered": self.trades_unregistered,
                "quote_matched": self.quote_matched,
                "quote_missing": self.quote_missing,
                "contract_count": len(self.contracts),
                "strike_count": len(self.strike_metrics),
                "strike_metrics": self.strike_metrics,
            }

    def _window_strike_metrics_locked(self, timeframe: str, now: Optional[float] = None) -> Dict[float, Dict[str, Dict[str, float]]]:
        seconds = self._timeframe_seconds(timeframe)
        now = now or time.time()
        cutoff = int(now // 60) - (seconds // 60) + 1
        out = {}
        for bucket_key, strike_map in self._minute_strike_buckets.items():
            if bucket_key < cutoff or bucket_key > int(now // 60):
                continue
            for strike, bucket in strike_map.items():
                if strike not in out:
                    out[strike] = self._empty_strike_metrics()
                for opt_type in ("C", "P"):
                    for side in ("buy", "sell", "unknown"):
                        out[strike][opt_type][side + "_volume"] += bucket[opt_type][side]
                for opt_type in ("C", "P"):
                    out[strike][opt_type]["total_volume"] = (
                        out[strike][opt_type]["buy_volume"] +
                        out[strike][opt_type]["sell_volume"] +
                        out[strike][opt_type]["unknown_volume"]
                    )
        return out

    def get_matrix_df(self, timeframe: str = '1m'):
        """Full-chain matrix: ALL active SPXW strikes from registry, aggregated by strike."""
        import pandas as pd

        columns = [
            "Strike", "Call Buy", "Call Sell", "Call Unknown", "Call Net",
            "Call Buy %", "Call Sell %", "Call Bid", "Call Ask", "Call Last",
            "Put Last", "Put Bid", "Put Ask", "Put Buy %", "Put Sell %",
            "Put Net", "Put Buy", "Put Sell", "Put Unknown",
        ]

        with self._lock:
            self._record_ratio_snapshot_locked(timeframe)
            window_metrics = self._matrix_snapshots.get(timeframe) or {}
            strikes = sorted(set(self._all_strikes()) | set(window_metrics.keys()))
            if not strikes:
                return pd.DataFrame(columns=columns)

            records = []
            for strike in strikes:
                m = window_metrics.get(strike, self._empty_strike_metrics())
                c_buy, c_sell, c_unknown = m["C"]["buy_volume"], m["C"]["sell_volume"], m["C"]["unknown_volume"]
                p_buy, p_sell, p_unknown = m["P"]["buy_volume"], m["P"]["sell_volume"], m["P"]["unknown_volume"]
                c_tot = c_buy + c_sell + c_unknown
                p_tot = p_buy + p_sell + p_unknown

                c_quote = self._best_quote_for_strike_type(strike, "C")
                p_quote = self._best_quote_for_strike_type(strike, "P")
                c_last = self._latest_price_for_strike_type(strike, "C")
                p_last = self._latest_price_for_strike_type(strike, "P")

                records.append({
                    "Strike": strike,
                    "Call Buy": int(c_buy),
                    "Call Sell": int(c_sell),
                    "Call Unknown": int(c_unknown),
                    "Call Net": int(c_buy - c_sell),
                    "Call Buy %": round(c_buy / c_tot * 100, 1) if c_tot > 0 else 0.0,
                    "Call Sell %": round(c_sell / c_tot * 100, 1) if c_tot > 0 else 0.0,
                    "Call Bid": round(c_quote["bid"], 2) if c_quote.get("bid") else None,
                    "Call Ask": round(c_quote["ask"], 2) if c_quote.get("ask") else None,
                    "Call Last": round(c_last, 2) if c_last else None,
                    "Put Last": round(p_last, 2) if p_last else None,
                    "Put Bid": round(p_quote["bid"], 2) if p_quote.get("bid") else None,
                    "Put Ask": round(p_quote["ask"], 2) if p_quote.get("ask") else None,
                    "Put Buy %": round(p_buy / p_tot * 100, 1) if p_tot > 0 else 0.0,
                    "Put Sell %": round(p_sell / p_tot * 100, 1) if p_tot > 0 else 0.0,
                    "Put Net": int(p_buy - p_sell),
                    "Put Buy": int(p_buy),
                    "Put Sell": int(p_sell),
                    "Put Unknown": int(p_unknown),
                })

            return pd.DataFrame(records)

    def get_market_summary(self, timeframe: str = '1m'):
        with self._lock:
            # All model-facing summary values are frozen between selected
            # timeframe boundaries. Dashboard reads therefore cannot change
            # the model simply because the wall clock advanced.
            flow = self._cached_or_current_flow_locked(timeframe)
            call_buy, call_sell = flow["call_buy"], flow["call_sell"]
            put_buy, put_sell = flow["put_buy"], flow["put_sell"]
            call_vol = call_buy + call_sell + flow["call_unknown"]
            put_vol = put_buy + put_sell + flow["put_unknown"]

            cb, cs, pb, ps = int(call_buy), int(call_sell), int(put_buy), int(put_sell)
            call_buy_sell_ratio = round(cb / cs, 2) if cs > 0 else 0.0
            put_buy_sell_ratio = round(pb / ps, 2) if ps > 0 else 0.0
            cp_ratio = round(call_vol / put_vol, 2) if put_vol > 0 else 0.0
            bullish_flow, bearish_flow = cb + ps, cs + pb
            dir_ratio = round(bullish_flow / bearish_flow, 2) if bearish_flow > 0 else 0.0

            if call_buy_sell_ratio >= 1.5 and put_buy_sell_ratio <= 0.8 and cp_ratio >= 1.2:
                direction, direction_desc = "STRONG BULLISH 🚀", f"Call Buy/Sell ({call_buy_sell_ratio}) & Put Selling ({put_buy_sell_ratio})."
            elif dir_ratio >= 1.15:
                direction, direction_desc = "BULLISH 📈", f"Net bullish flow {dir_ratio}x."
            elif call_buy_sell_ratio <= 0.7 and put_buy_sell_ratio >= 1.4 and cp_ratio <= 0.8:
                direction, direction_desc = "STRONG BEARISH 🔻", f"Heavy Put Buying ({put_buy_sell_ratio})."
            elif 0 < dir_ratio <= 0.85:
                direction, direction_desc = "BEARISH 📉", f"Net bearish flow {round(1/dir_ratio,2)}x."
            else:
                direction, direction_desc = "NEUTRAL / RANGEBOUND ↔️", "Order flow balanced."

            return {
                "call_buy": cb, "call_sell": cs, "put_buy": pb, "put_sell": ps,
                "call_volume": call_vol, "put_volume": put_vol,
                "call_put_ratio": cp_ratio,
                "call_buy_sell_ratio": call_buy_sell_ratio,
                "put_buy_sell_ratio": put_buy_sell_ratio,
                "directional_ratio": dir_ratio,
                "market_direction": direction,
                "market_direction_desc": direction_desc,
                "net_call_flow": cb - cs, "net_put_flow": pb - ps,
                "total_volume": call_vol + put_vol,
                "total_trades": self.trades_received,
                "contracts_loaded": len(self.contracts),
                "call_buy_pct": round(cb / call_vol * 100, 1) if call_vol > 0 else 0.0,
                "put_buy_pct": round(pb / put_vol * 100, 1) if put_vol > 0 else 0.0,
                "cumulative_call_buy": self._cumulative_flow["C"]["buy"],
                "cumulative_call_sell": self._cumulative_flow["C"]["sell"],
                "cumulative_put_buy": self._cumulative_flow["P"]["buy"],
                "cumulative_put_sell": self._cumulative_flow["P"]["sell"],
                "cumulative_call_buy_sell_ratio": (self._cumulative_flow["C"]["buy"] / self._cumulative_flow["C"]["sell"] if self._cumulative_flow["C"]["sell"] > 0 else 0.0),
                "cumulative_put_buy_sell_ratio": (self._cumulative_flow["P"]["buy"] / self._cumulative_flow["P"]["sell"] if self._cumulative_flow["P"]["sell"] > 0 else 0.0),
                "cumulative_call_put_ratio": ((self._cumulative_flow["C"]["buy"] + self._cumulative_flow["C"]["sell"] + self._cumulative_flow["C"]["unknown"]) / (self._cumulative_flow["P"]["buy"] + self._cumulative_flow["P"]["sell"] + self._cumulative_flow["P"]["unknown"]) if (self._cumulative_flow["P"]["buy"] + self._cumulative_flow["P"]["sell"] + self._cumulative_flow["P"]["unknown"]) > 0 else 0.0),
                "spot_price": self.estimated_spx,
                "spy_price": self.spy_price,
                "prev_close": self.prev_close,
                "day_high": self.day_high,
                "day_low": self.day_low,
                "has_live_spot": self.estimated_spx > 0,
            }

    def get_heatmap(self):
        df = self.get_matrix_df()
        if df.empty:
            import pandas as pd
            return pd.DataFrame(columns=["Strike", "Call Buy", "Call Sell", "Net", "Put Buy", "Put Sell"])
        df = df.copy()
        df["Net"] = df["Call Net"] + df["Put Net"]
        return df[["Strike", "Call Buy", "Call Sell", "Net", "Put Buy", "Put Sell"]]

    def get_top_buy_calls(self):
        df = self.get_matrix_df()
        return df.nlargest(10, "Call Buy")[["Strike", "Call Buy", "Call Buy %", "Call Last"]] if not df.empty else df

    def get_top_sell_calls(self):
        df = self.get_matrix_df()
        return df.nlargest(10, "Call Sell")[["Strike", "Call Sell", "Call Sell %", "Call Last"]] if not df.empty else df

    def get_top_buy_puts(self):
        df = self.get_matrix_df()
        return df.nlargest(10, "Put Buy")[["Strike", "Put Buy", "Put Buy %", "Put Last"]] if not df.empty else df

    def get_top_sell_puts(self):
        df = self.get_matrix_df()
        return df.nlargest(10, "Put Sell")[["Strike", "Put Sell", "Put Sell %", "Put Last"]] if not df.empty else df

    def get_volume_leaders(self):
        df = self.get_matrix_df()
        if df.empty:
            return df
        df = df.copy()
        df["Total Vol"] = df["Call Buy"] + df["Call Sell"] + df["Call Unknown"] + df["Put Buy"] + df["Put Sell"] + df["Put Unknown"]
        return df.nlargest(10, "Total Vol")[["Strike", "Total Vol", "Call Net", "Put Net"]]

    def get_most_active_strikes(self):
        return self.get_volume_leaders()

    def get_recent_trades(self, limit=30):
        import pandas as pd
        with self._lock:
            trades = list(self.recent_trades)[:limit]
        if not trades:
            return pd.DataFrame(columns=["Time", "Strike", "Type", "Side", "Size", "Price", "Notional"])
        return pd.DataFrame(trades)

    def statistics(self, feed_health: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        with self._lock:
            now = time.time()
            recent_trades = sum(1 for t in self._trade_timestamps if now - t <= 60)
            recent_quotes = sum(1 for t in self._quote_timestamps if now - t <= 60)
            snapshot = self.get_dashboard_snapshot()

            return {
                "connected": feed_health.get("connected", False) if feed_health else False,
                "latency_ms": feed_health.get("latency_ms") if feed_health else None,
                "quotes_per_sec": round(recent_quotes / 60, 1),
                "trades_per_sec": round(recent_trades / 60, 1),
                "contracts": len(self.contracts),
                "last_update": (
                    et_time_str(max(self._trade_timestamps), "%Y-%m-%d %H:%M:%S") + " ET"
                    if self._trade_timestamps else "—"
                ),
                "unknown_trades": self.unknown_count,
                "unregistered_trades": self.trades_unregistered,
                "quote_cache_size": len(self.quote_cache),
                "reconnect_count": feed_health.get("reconnect_count", 0) if feed_health else 0,
                "trades_received": self.trades_received,
                "quote_coverage": snapshot["quote_coverage"],
                "active_mode": feed_health.get("active_mode", "UNKNOWN") if feed_health else "UNKNOWN",
                "spy_connected": feed_health.get("spy_connected", False) if feed_health else False,
                "options_connected": feed_health.get("options_connected", False) if feed_health else False,
                "spy_status": feed_health.get("spy_status", "OFFLINE") if feed_health else "OFFLINE",
                "options_status": feed_health.get("options_status", "OFFLINE") if feed_health else "OFFLINE",
                "last_spy_trade": feed_health.get("last_spy_trade") if feed_health else None,
                "last_options_trade": feed_health.get("last_options_trade") if feed_health else None,
                "last_error": feed_health.get("last_error") if feed_health else None,
            }

    def export_dataframe(self, timeframe: str = '1m'):
        return self.get_matrix_df(timeframe)

    def get_call_summary(self, timeframe: str = '1m'):
        s = self.get_market_summary(timeframe)
        return {"buy": s["call_buy"], "sell": s["call_sell"], "net": s["net_call_flow"]}

    def get_put_summary(self, timeframe: str = '1m'):
        s = self.get_market_summary(timeframe)
        return {"buy": s["put_buy"], "sell": s["put_sell"], "net": s["net_put_flow"]}
