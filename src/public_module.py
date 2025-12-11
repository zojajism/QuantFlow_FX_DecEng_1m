from pathlib import Path
import yaml
from typing import Dict, Tuple, Optional
from datetime import datetime

margin_available = 0.0
balance = 0.0
margin_dict = {}

CORRELATION_SCORE=60.0

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
