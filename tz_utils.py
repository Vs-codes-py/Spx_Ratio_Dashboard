"""
Timezone helpers.

Every user-facing timestamp in the dashboard is rendered in US Eastern Time
(America/New_York), which automatically handles EST/EDT daylight-saving.
Streamlit Community Cloud containers run in UTC, so naive `datetime.now()` /
`time.localtime()` calls would otherwise display UTC. Route them through here.
"""

from datetime import datetime, timezone

try:
    from zoneinfo import ZoneInfo
    ET = ZoneInfo("America/New_York")
except Exception:  # pragma: no cover - missing tzdata
    ET = None

# Human label for the active market timezone.
ET_LABEL = "ET"


def now_et() -> datetime:
    """Current time as a timezone-aware datetime in US Eastern Time."""
    return datetime.now(ET) if ET is not None else datetime.now()


def to_et(dt: datetime) -> datetime:
    """Convert any datetime to US Eastern Time.

    Naive datetimes are assumed to be UTC (the Streamlit Cloud default).
    """
    if dt is None:
        return None
    if ET is None:
        return dt
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(ET)


def et_from_timestamp(ts: float) -> datetime:
    """Convert a POSIX timestamp (seconds) to a US Eastern Time datetime."""
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    return dt.astimezone(ET) if ET is not None else datetime.fromtimestamp(ts)


def et_time_str(ts: float, fmt: str = "%H:%M:%S") -> str:
    """Format a POSIX timestamp as an ET wall-clock string."""
    return et_from_timestamp(ts).strftime(fmt)
