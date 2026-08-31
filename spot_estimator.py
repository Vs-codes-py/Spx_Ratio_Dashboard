import logging
import threading
from typing import Optional, Dict, Any
import requests
from concurrent.futures import ThreadPoolExecutor
from config import Config


class SpotEstimator:
    """Live SPX estimator.

    SPY ticks update the estimate immediately. A periodic Yahoo refresh is used
    as a 60-second baseline correction/fallback. Network I/O never holds the
    state lock, so the live SPY callback is not blocked by HTTP latency.
    """

    def __init__(self, config: Optional[Config] = None):
        self.config = config or Config()
        self._lock = threading.RLock()
        self.spy_price: float = 0.0
        self.estimated_spx: float = 0.0
        self.atm_strike: float = 0.0
        self.prev_close: float = 0.0
        self.day_high: float = 0.0
        self.day_low: float = 0.0
        self.ratio: float = getattr(self.config.spx_estimation, 'SPY_TO_SPX_RATIO', 10.0308)

    def _parse_yahoo_meta(self, data: dict) -> Dict[str, float]:
        result = {"price": 0.0, "prev_close": 0.0, "day_high": 0.0, "day_low": 0.0}
        try:
            meta = data["chart"]["result"][0]["meta"]
            result["price"] = float(meta.get("regularMarketPrice") or 0)
            result["prev_close"] = float(meta.get("previousClose") or meta.get("chartPreviousClose") or 0)
            result["day_high"] = float(meta.get("regularMarketDayHigh") or 0)
            result["day_low"] = float(meta.get("regularMarketDayLow") or 0)
        except (KeyError, IndexError, TypeError, ValueError):
            pass
        return result

    def _fetch_yahoo(self, symbol: str) -> Dict[str, float]:
        headers = {"User-Agent": "Mozilla/5.0"}
        try:
            r = requests.get(
                f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}",
                headers=headers, timeout=2
            )
            r.raise_for_status()
            return self._parse_yahoo_meta(r.json())
        except Exception as e:
            logging.debug(f"[SpotEstimator] Yahoo {symbol} fetch: {e}")
            return {"price": 0.0, "prev_close": 0.0, "day_high": 0.0, "day_low": 0.0}

    def fetch_live_prices(self) -> None:
        """Refresh SPY/SPX baseline without blocking live feed callbacks."""
        with ThreadPoolExecutor(max_workers=2) as executor:
            f_spy = executor.submit(self._fetch_yahoo, "SPY")
            f_spx = executor.submit(self._fetch_yahoo, "%5EGSPC")
            spy_meta = f_spy.result()
            spx_meta = f_spx.result()

        fetched_spy = spy_meta["price"]
        fetched_spx = spx_meta["price"]

        # TwelveData remains fallback only if Yahoo has no SPY price.
        if fetched_spy <= 0 and self.config.spx_estimation.TWELVE_DATA_API_KEY:
            try:
                key = self.config.spx_estimation.TWELVE_DATA_API_KEY
                r = requests.get(
                    f"https://api.twelvedata.com/price?symbol=SPY&apikey={key}", timeout=2
                )
                data = r.json()
                fetched_spy = float(data.get("price", 0) or 0)
            except Exception as e:
                logging.debug(f"[SpotEstimator] TwelveData fallback: {e}")

        if fetched_spy <= 0:
            return

        with self._lock:
            self.spy_price = fetched_spy

            if fetched_spx > 0:
                self.estimated_spx = fetched_spx
                self.ratio = fetched_spx / fetched_spy
            else:
                self.estimated_spx = fetched_spy * self.ratio

            self.prev_close = spx_meta["prev_close"] or (
                spy_meta["prev_close"] * self.ratio if spy_meta["prev_close"] > 0 else self.prev_close
            )
            self.day_high = spx_meta["day_high"] or (
                spy_meta["day_high"] * self.ratio if spy_meta["day_high"] > 0 else self.day_high
            )
            self.day_low = spx_meta["day_low"] or (
                spy_meta["day_low"] * self.ratio if spy_meta["day_low"] > 0 else self.day_low
            )
            self._recalculate_atm()

        logging.info(
            f"[SpotEstimator] Baseline -> SPY: ${fetched_spy:.2f}, "
            f"SPX: ${self.estimated_spx:.2f}, PrevClose: ${self.prev_close:.2f}"
        )

    def update_spy(self, price: float) -> None:
        """Update SPY and estimated SPX immediately from the live Databento tick."""
        if price <= 0:
            return
        with self._lock:
            self.spy_price = float(price)
            self.estimated_spx = self.spy_price * self.ratio
            self._recalculate_atm()

    def _recalculate_atm(self) -> None:
        interval = self.config.spx_estimation.STRIKE_INTERVAL
        self.atm_strike = round(self.estimated_spx / interval) * interval if interval > 0 else round(self.estimated_spx)

    def get_spy(self) -> float:
        with self._lock:
            return self.spy_price

    def get_estimated_spx(self) -> float:
        with self._lock:
            return self.estimated_spx

    def get_atm(self) -> float:
        with self._lock:
            return self.atm_strike

    def get_session_stats(self) -> Dict[str, float]:
        with self._lock:
            return {
                "prev_close": self.prev_close,
                "day_high": self.day_high,
                "day_low": self.day_low,
            }
