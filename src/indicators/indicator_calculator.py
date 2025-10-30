# English-only comments

from typing import Dict, Callable, List
from datetime import datetime
import logging
import buffers.buffer_initializer as buffers

# Buffers
from buffers.candle_buffer import CandleBuffer, Keys
from buffers.indicator_buffer import IndicatorBuffer

# --- Indicator placeholders (lightweight) ---
# NOTE: Ensure these functions exist in src.indicators.indicators
from indicators.indicators import (
    # Trend
    ema8_placeholder, ema21_placeholder,
    sma50_placeholder, sma200_placeholder,
    macd_line_placeholder, macd_signal_placeholder,
    # Momentum
    rsi14_placeholder, stoch_k_placeholder, stoch_d_placeholder, cci_placeholder,
    # Volatility
    bollinger_upper_placeholder, bollinger_lower_placeholder,
    atr14_placeholder, atr_ma_placeholder,
    # Volume / Confirmation
    obv_placeholder, mfi_placeholder,
    volume_placeholder, volume_ma_placeholder,
)

logger = logging.getLogger(__name__)

# Map canonical indicator name -> (required_window, function)
# Windows are pragmatic for placeholders (can be tuned later).
INDICATOR_REGISTRY: Dict[str, tuple[int, Callable[[List[dict]], float]]] = {
    # --- Volatility ---
    "atr":      (15, atr14_placeholder),
    "atr_ma":   (45, atr_ma_placeholder),  # atr(14) smoothed over ~30
}

# Which indicators to compute on every candle close:
INDICATORS_TO_COMPUTE: List[str] = list(INDICATOR_REGISTRY.keys())


def compute_and_append_on_close(
    key: Keys,
    close_time: datetime,
    close_price: float,
) -> Dict[str, float]:
    """
    Compute canonical indicators (as required by DecisionEngine) from CandleBuffer
    and append values into IndicatorBuffer under the same canonical names.

    Returns: dict of {indicator_name: value}
    """

    results: Dict[str, float] = {}

    for name in INDICATORS_TO_COMPUTE:
        spec = INDICATOR_REGISTRY.get(name)
        if not spec:
            continue
        window, func = spec

        # Fetch last N candles from CandleBuffer
        candles = buffers.CANDLE_BUFFER.last_n(key, window)

        # Compute indicator
        try:
            value = float(func(candles))
        except Exception as e:
            logger.error(f"Error computing {name}: {e}")
            value = float("nan")

        # Append to IndicatorBuffer with canonical name
        point = {
            "indicator": name,
            "value": value,
            "close_time": close_time,
            "close_price": close_price,
        }
        buffers.INDICATOR_BUFFER.append(key, name, point)
        results[name] = value

    return results
