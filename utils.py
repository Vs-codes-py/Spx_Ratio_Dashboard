"""
Utils module alias re-exporting utilis helper functions.
"""

from utilis import (
    format_volume,
    format_price,
    format_percentage,
    format_time,
    format_latency,
    green,
    red,
    yellow,
    ratio_color,
    heatmap_color,
    is_market_open,
)
from datetime import datetime

def colorize(val: float) -> str:
    if val > 0:
        return "#00E676"
    if val < 0:
        return "#FF5252"
    return "#FFFFFF"

def time_since_update(dt: datetime) -> str:
    if not dt:
        return "N/A"
    diff = (datetime.now() - dt).total_seconds()
    return f"{int(diff)}s ago"
