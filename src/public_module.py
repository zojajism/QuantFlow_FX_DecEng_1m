
from __future__ import annotations

from decimal import Decimal
from pathlib import Path
import yaml
from typing import Dict, Tuple, Optional
from datetime import datetime, time, timezone

from zoneinfo import ZoneInfo

margin_available = 0.0
balance = 0.0
margin_dict = {}

CORRELATION_SCORE=60.0

MAX_HIT_DISTANCE_DEVIATION=5

MIN_SL_PIPS= 8

K_ATR= 1.2

CORRELATION_CHECK= False

FILTER_MARKET_TIME= False

APPLY_SL= False


# Type:
#   correlation_cache[timeframe][(sym_a, sym_b)] = corr_value
# where (sym_a, sym_b) is an unordered / canonical pair (sorted tuple).
correlation_cache: Dict[str, Dict[Tuple[str, str], float]] = {
    "5m": {},
    "15m": {},
}

# Last as_of_time loaded into the cache (for monitoring / debugging)
correlation_as_of_time: Optional[datetime] = None


CONFIG_PATH = Path("/data/config.yaml")
if not CONFIG_PATH.exists():
    CONFIG_PATH = Path(__file__).resolve().parent / "data" / "config.yaml"

if not CONFIG_PATH.exists():
    raise FileNotFoundError(f"Config file not found: {CONFIG_PATH}")

with CONFIG_PATH.open("r", encoding="utf-8") as f:
    config_data = yaml.safe_load(f) or {}


CORRELATION_SCORE = config_data.get("CORRELATION_SCORE", 60.0)
MAX_HIT_DISTANCE_DEVIATION = config_data.get("MAX_HIT_DISTANCE_DEVIATION", 5)

MIN_SL_PIPS = config_data.get("MIN_SL_PIPS", 8)
K_ATR = config_data.get("K_ATR", 1.2)

CORRELATION_CHECK = config_data.get("CORRELATION_CHECK", False)

FILTER_MARKET_TIME = config_data.get("FILTER_MARKET_TIME", False)

APPLY_SL = config_data.get("APPLY_SL", False)


TP_FIX_DISTANCE = Decimal(str(config_data.get("TP_FIX_DISTANCE", "2.0")))
TP_TARGET_FOR_FIX_DISTANCE = Decimal(str(config_data.get("TP_TARGET_FOR_FIX_DISTANCE", "20.0")))

# preparing the margine list
# This section is a list of 1-key dictionaries → convert to normal dict
raw_list = config_data.get("MARGINE_REQUIREMENT", [])

for item in raw_list:
    # Case 1: normal YAML dict, e.g. {"EUR/USD": 28}
    if isinstance(item, dict):
        key, value = next(iter(item.items()))
    
    # Case 2: string like "EUR/USD:28"
    elif isinstance(item, str):
        if ":" not in item:
            raise ValueError(f"Invalid margin entry (no colon): {item!r}")
        key, value = item.split(":", 1)
        key = key.strip()
        value = value.strip()
    
    # Unexpected type
    else:
        raise TypeError(f"Unexpected margin entry type: {type(item)} -> {item!r}")

    margin_dict[key] = float(value)
#=======================================================================

def check_available_required_margine(symbol: str, trade_unit: int) -> tuple[bool, float]:
    margine = margin_dict.get(symbol)
    if margine is None:
        raise KeyError(f"Symbol not found in margin_dict: {symbol}")
    
    required_margine = round(trade_unit / float(margine),3)

    return round(margin_available,3) > required_margine, required_margine


'''
def allow_trade_session_utc(dt_utc: datetime) -> bool:
    """
    Allow trades only during:
      - London: 08:00–17:00 Europe/London
      - New York: 08:00–17:00 America/New_York

    Friday rule:
      - Only London session is allowed (NY is NOT allowed on Fridays)

    Weekends:
      - Always False

    Input:
      - dt_utc can be naive (assumed UTC) or tz-aware.
    """
    # Normalize to aware UTC
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=timezone.utc)
    else:
        dt_utc = dt_utc.astimezone(timezone.utc)

    # Weekend: no trading
    if dt_utc.weekday() >= 5:  # Sat(5), Sun(6)
        return False

    london = dt_utc.astimezone(ZoneInfo("Europe/London"))
    ny = dt_utc.astimezone(ZoneInfo("America/New_York"))

    def in_window(local_dt: datetime, start: time, end: time) -> bool:
        t = local_dt.time()
        return start <= t < end

    in_london = in_window(london, time(8, 0), time(17, 0))
    in_ny = in_window(ny, time(8, 0), time(17, 0))

    # Friday: stop after London close (ignore NY entirely)
    if london.weekday() == 4:  # Friday
        return in_london

    return in_london or in_ny
'''
'''
def allow_trade_session_utc(dt_utc: datetime) -> bool:
    """
    Allow trades only during:
      - London: 08:00–17:00 Europe/London

    Weekends:
      - Always False

    Input:
      - dt_utc can be naive (assumed UTC) or tz-aware.
    """
    # Normalize to aware UTC
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=timezone.utc)
    else:
        dt_utc = dt_utc.astimezone(timezone.utc)

    # Weekend: no trading
    if dt_utc.weekday() >= 5:  # Sat(5), Sun(6)
        return False

    london = dt_utc.astimezone(ZoneInfo("Europe/London"))

    # London trading window: 08:00–17:00 local London time
    t = london.time()
    return time(8, 0) <= t < time(17, 0)
'''

from datetime import datetime, time, timezone
from zoneinfo import ZoneInfo


def allow_trade_session_utc(dt_utc: datetime) -> bool:
    """
    Return False ONLY when:
      - It is Friday
      - AND London session is finished (>= 17:00 London time)

    Otherwise:
      - Always True

    Input:
      - dt_utc can be naive (assumed UTC) or tz-aware.
    """

    # Normalize to aware UTC
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=timezone.utc)
    else:
        dt_utc = dt_utc.astimezone(timezone.utc)

    # Convert to London time
    london = dt_utc.astimezone(ZoneInfo("Europe/London"))

    # Friday = 4
    if london.weekday() == 4:
        if london.time() >= time(17, 0):
            return False

    return True
