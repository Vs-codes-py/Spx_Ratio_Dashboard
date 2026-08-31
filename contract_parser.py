"""
OCC / OSI option symbol parser for Databento SymbolMappingMsg and raw_symbol fields.
"""

import re
from typing import Optional, Dict, Any


def parse_occ_symbol(symbol: str) -> Optional[Dict[str, Any]]:
    """
    Parse OCC option symbol into strike, call/put, expiration.
    Examples: 'SPXW  260811C08030000', 'SPXW260811C08030000'
    """
    if not symbol or not str(symbol).strip():
        return None

    s = str(symbol).strip()
    compact = re.sub(r"\s+", "", s.upper())

    m = re.match(r"^([A-Z0-9]{1,6})(\d{6})([CP])(\d{8})$", compact)
    if m:
        root, date_str, cp, strike_raw = m.groups()
    else:
        padded = s.ljust(21)[:21]
        date_str = padded[6:12]
        cp = padded[12].upper()
        strike_raw = padded[13:21]
        root = padded[0:6].strip()
        if cp not in ("C", "P") or not date_str.isdigit() or not strike_raw.isdigit():
            return None

    try:
        strike = int(strike_raw) / 1000.0
        yy, mm, dd = int(date_str[0:2]), int(date_str[2:4]), int(date_str[4:6])
        year = 2000 + yy if yy < 80 else 1900 + yy
        expiration = f"{year:04d}-{mm:02d}-{dd:02d}"
    except (ValueError, IndexError):
        return None

    return {
        "strike": strike,
        "option_type": cp,
        "expiration": expiration,
        "root": root,
        "symbol": s,
    }
