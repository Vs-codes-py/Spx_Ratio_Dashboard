"""
Shared helper utilities for formatting, calculations, conversions, and styling.
Contains no business logic or API interaction.
"""

from datetime import datetime, time
import math
import os
import json
from typing import Union, List, Dict, Any, Optional
import pandas as pd


# ==============================================================================
# FORMATTING
# ==============================================================================

def format_volume(val: Union[int, float]) -> str:
    """Format large numbers into human-readable shorthand (K, M, B)."""
    if val is None or math.isnan(val):
        return "0"
    abs_val = abs(val)
    sign = "-" if val < 0 else ""
    if abs_val >= 1_000_000_000:
        return f"{sign}{abs_val / 1_000_000_000:.2f}B"
    if abs_val >= 1_000_000:
        return f"{sign}{abs_val / 1_000_000:.2f}M"
    if abs_val >= 1_000:
        return f"{sign}{abs_val / 1_000:.1f}K"
    return f"{sign}{int(abs_val):,}"


def format_price(val: Union[int, float]) -> str:
    """Format floating point values to standardized currency output."""
    if val is None or math.isnan(val):
        return "$0.00"
    return f"${val:,.2f}"


def format_percentage(val: Union[int, float], decimals: int = 1) -> str:
    """Format decimals or percentages into string format."""
    if val is None or math.isnan(val):
        return "0.0%"
    return f"{val:.{decimals}f}%"


def format_time(dt: Optional[datetime]) -> str:
    """Format datetime into standard time format string."""
    if not dt:
        return "N/A"
    return dt.strftime("%H:%M:%S")


def format_latency(ms: float) -> str:
    """Format millisecond numerical latency to string."""
    return f"{ms:.1f} ms"


# ==============================================================================
# COLORING & STYLING
# ==============================================================================

def green(text: str) -> str:
    return f"\033[92m{text}\033[0m"


def red(text: str) -> str:
    return f"\033[91m{text}\033[0m"


def yellow(text: str) -> str:
    return f"\033[93m{text}\033[0m"


def gradient(val: float, min_val: float, max_val: float) -> str:
    """Generate dynamic hex color gradient between Red (-1) and Green (+1)."""
    norm = normalize(val, min_val, max_val)
    r = int(255 * (1 - norm))
    g = int(255 * norm)
    return f"#{r:02x}{g:02x}00"


def heatmap_color(val: float) -> str:
    if val > 0:
        return "#00E676"
    if val < 0:
        return "#FF5252"
    return "#161B22"


def volume_color(volume: int, max_volume: int) -> str:
    intensity = min(1.0, volume / max(1, max_volume))
    alpha = hex(int(intensity * 255))[2:].zfill(2)
    return f"#00E676{alpha}"


def ratio_color(ratio: float) -> str:
    if ratio > 1.2:
        return "#00E676"
    if ratio < 0.8:
        return "#FF5252"
    return "#FFD700"


# ==============================================================================
# TIME HELPERS
# ==============================================================================

def current_time() -> datetime:
    return datetime.now()


def market_open(dt: Optional[datetime] = None) -> datetime:
    d = dt or datetime.now()
    return datetime.combine(d.date(), time(9, 30))


def market_close(dt: Optional[datetime] = None) -> datetime:
    d = dt or datetime.now()
    return datetime.combine(d.date(), time(16, 0))


def seconds_since(dt: datetime) -> float:
    return (datetime.now() - dt).total_seconds()


def minutes_since(dt: datetime) -> float:
    return seconds_since(dt) / 60.0


def session_elapsed() -> float:
    now = datetime.now()
    m_open = market_open(now)
    if now < m_open:
        return 0.0
    return min(23400.0, (now - m_open).total_seconds())  # Max 390 mins = 23400s


# ==============================================================================
# VALIDATION HELPERS
# ==============================================================================

def is_valid_trade(price: float, size: int) -> bool:
    return price > 0.0 and size > 0


def is_valid_quote(bid: float, ask: float) -> bool:
    return bid >= 0.0 and ask >= bid


def is_market_open(dt: Optional[datetime] = None) -> bool:
    now = dt or datetime.now()
    if now.weekday() >= 5:  # Weekend check
        return False
    return market_open(now) <= now <= market_close(now)


def is_spx_symbol(symbol: str) -> bool:
    return "SPX" in symbol.upper() or "SPXW" in symbol.upper()


def is_call(option_type: str) -> bool:
    return option_type.upper() in ["C", "CALL"]


def is_put(option_type: str) -> bool:
    return option_type.upper() in ["P", "PUT"]


# ==============================================================================
# CONVERSIONS
# ==============================================================================

def price_to_str(val: float) -> str:
    return format_price(val)


def volume_to_str(val: int) -> str:
    return format_volume(val)


def datetime_to_str(dt: datetime) -> str:
    return dt.isoformat()


def str_to_datetime(s: str) -> datetime:
    return datetime.fromisoformat(s)


# ==============================================================================
# MATHEMATICAL HELPERS
# ==============================================================================

def clamp(val: float, min_val: float, max_val: float) -> float:
    return max(min_val, min(val, max_val))


def normalize(val: float, min_val: float, max_val: float) -> float:
    if max_val == min_val:
        return 0.5
    return clamp((val - min_val) / (max_val - min_val), 0.0, 1.0)


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    if denominator == 0.0 or math.isnan(denominator):
        return default
    return numerator / denominator


def rolling_average(values: List[float], window: int) -> float:
    if not values:
        return 0.0
    subset = values[-window:]
    return sum(subset) / len(subset)


def rolling_sum(values: List[float], window: int) -> float:
    if not values:
        return 0.0
    return sum(values[-window:])


def moving_average(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window=window, min_periods=1).mean()


def z_score(val: float, mean: float, std: float) -> float:
    if std == 0.0:
        return 0.0
    return (val - mean) / std


# ==============================================================================
# DASHBOARD HELPERS
# ==============================================================================

def create_metric_card(title: str, value: Any, subtitle: str = "", css_class: str = "") -> str:
    return f"""
    <div class="metric-card">
        <div class="metric-card-title">{title}</div>
        <div class="metric-card-value {css_class}">{value}</div>
        <div class="metric-card-sub">{subtitle}</div>
    </div>
    """


def build_dataframe(records: List[Dict[str, Any]]) -> pd.DataFrame:
    return pd.DataFrame(records)


def sort_option_chain(df: pd.DataFrame, ascending: bool = True) -> pd.DataFrame:
    if "Strike" in df.columns:
        return df.sort_values(by="Strike", ascending=ascending)
    return df


def highlight_atm(strike: float, spot: float, threshold: float = 2.5) -> bool:
    return abs(strike - spot) <= threshold


def highlight_volume(val: int, threshold: int = 1000) -> str:
    return "font-weight: bold; color: #00E676;" if val >= threshold else ""


def highlight_net_flow(val: int) -> str:
    if val > 0:
        return "color: #00E676;"
    if val < 0:
        return "color: #FF5252;"
    return "color: #FFFFFF;"


# ==============================================================================
# EXPORT HELPERS
# ==============================================================================

def save_csv(df: pd.DataFrame, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)


def save_json(data: Dict[str, Any], path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w') as f:
        json.dump(data, f, indent=2)


def save_parquet(df: pd.DataFrame, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_parquet(path, index=False)


def export_dataframe(df: pd.DataFrame, export_format: str = "csv") -> Union[str, bytes]:
    if export_format == "json":
        return df.to_json(orient="records")
    return df.to_csv(index=False)


# ==============================================================================
# LOGGING HELPERS
# ==============================================================================

def log_trade(trade_id: int, price: float, size: int, side: str) -> str:
    return f"[TRADE] ID:{trade_id} | Price:{price} | Size:{size} | Side:{side}"


def log_quote(symbol: str, bid: float, ask: float) -> str:
    return f"[QUOTE] {symbol} | Bid:{bid} | Ask:{ask}"


def log_error(err: Exception, ctx: str = "") -> str:
    return f"[ERROR] {ctx}: {str(err)}"


def log_warning(msg: str) -> str:
    return f"[WARN] {msg}"


def log_info(msg: str) -> str:
    return f"[INFO] {msg}"