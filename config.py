"""
Unified Application Configuration.
Normalizes Databento datasets, SPY-to-SPX estimation, provider schemas, and caching TTLs.

Secrets (API keys) are never hard-coded and never read from a file that is
committed to git. Resolution order for every key:
  1. Real environment variable (CI / shell / systemd).
  2. `.env` file loaded via python-dotenv (local dev only, git-ignored).
  3. Streamlit secrets — `st.secrets` / the Streamlit Community Cloud "Secrets"
     manager, which stores values encrypted at rest and never exposes them in
     the repo or the app UI.
"""

import os
from dataclasses import dataclass, field
from typing import Optional

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:  # pragma: no cover - dotenv is optional at runtime
    pass


def get_secret(name: str, default: str = "") -> str:
    """Resolve a secret from the environment, then from Streamlit secrets.

    Never logs or prints the value.
    """
    val = os.getenv(name)
    if val:
        return val
    try:
        import streamlit as st  # imported lazily so non-Streamlit entrypoints work
        # `in` avoids raising when the key is simply absent.
        if name in st.secrets:
            return str(st.secrets[name])
    except Exception:
        # No secrets.toml / not running under Streamlit — fall through to default.
        pass
    return default


@dataclass
class DatabentoConfig:
    API_KEY: str = field(default_factory=lambda: get_secret("DATABENTO_API_KEY", ""))

    # SPXW options — parent symbology
    OPTIONS_DATASET: str = "OPRA.PILLAR"
    OPTIONS_SCHEMA: str = "tcbbo"          # Trade + consolidated NBBO for BUY/SELL classification
    OPTIONS_SYMBOL: str = "SPXW.OPT"       # Parent symbol with stype_in=parent

    # SPY underlying
    SPY_DATASET: str = "EQUS.MINI"
    SPY_SCHEMA: str = "trades"
    SPY_SYMBOL: str = "SPY"

    START_TIME: Optional[str] = None
    END_TIME: Optional[str] = None


@dataclass
class SPXEstimationConfig:
    SPY_TO_SPX_RATIO: float = 10.0308
    STRIKE_INTERVAL: float = 5.0
    UPDATE_INTERVAL: int = 60
    TWELVE_DATA_API_KEY: str = field(default_factory=lambda: get_secret("TWELVE_DATA_API_KEY", ""))


@dataclass
class ProviderConfig:
    QUOTE_SCHEMA: str = "tcbbo"
    TRADE_SCHEMA: str = "tcbbo"

    CONTRACT_REFRESH_INTERVAL: int = 60
    MAX_CONTRACTS: int = 10000

    RECONNECT_ENABLED: bool = True
    RECONNECT_DELAY: int = 5

    QUOTE_CACHE_TTL: int = 10
    QUOTE_LOOKBACK_MS: int = 500

    # Feed staleness threshold — no data for this many seconds → STALE/OFFLINE
    STALE_FEED_SECONDS: int = 30

    # COMPLETELY DISABLE SIMULATION - PRODUCTION MODE ONLY
    SIMULATION_MODE: bool = False
    ENABLE_FALLBACK_SIMULATION: bool = False
    STRICT_LIVE_MODE: bool = True


class Config:
    """Master configuration container."""

    def __init__(self):
        self.databento = DatabentoConfig()
        self.spx_estimation = SPXEstimationConfig()
        self.provider = ProviderConfig()

    def to_dict(self) -> dict:
        """Serialize config WITHOUT leaking secrets."""
        def _redact(d: dict) -> dict:
            out = {}
            for k, v in d.items():
                if "KEY" in k.upper() or "SECRET" in k.upper() or "TOKEN" in k.upper():
                    out[k] = "***set***" if v else "***missing***"
                else:
                    out[k] = v
            return out

        return {
            "databento": _redact(self.databento.__dict__),
            "spx_estimation": _redact(self.spx_estimation.__dict__),
            "provider": self.provider.__dict__,
        }
