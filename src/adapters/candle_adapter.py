# English-only comments
# Adapter from your CandleBuffer -> pivot-friendly candles

from typing import List, Dict, Any
from buffers.candle_buffer import Keys, CandleBuffer

# Try to be robust to different key styles in your candles
def _normalize_candle(c: Dict[str, Any]) -> Dict[str, Any]:
    # Accept multiple common variants; default to KeyError if missing
    High = c.get("High", c.get("high", c.get("H")))
    Low = c.get("Low", c.get("low", c.get("L")))
    # times: prefer explicit names; fallbacks for typical internal fields
    CloseTime = c.get("CloseTime", c.get("close_time", c.get("time", c.get("t_close"))))
    OpenTime  = c.get("OpenTime",  c.get("open_time",  c.get("t_open")))

    if High is None or Low is None or CloseTime is None:
        raise KeyError("Candle is missing required fields (High/Low/CloseTime). "
                       "Please ensure buffer stores these or add new fallbacks here.")

    return {
        "High": High,
        "Low": Low,
        "CloseTime": CloseTime,
        "OpenTime": OpenTime,  # can be None; pivots_simple tolerates missing open_time
    }

def candles_for_pivots(
    candle_buffer: CandleBuffer,
    exchange: str,
    symbol: str,
    timeframe: str,
    n: int = 400
) -> List[Dict[str, Any]]:
    """
    Read last n closed candles from CandleBuffer and normalize
    them for the pivot detector.
    Returns ASC order list with keys: High, Low, CloseTime, OpenTime
    """
    key = Keys(exchange=exchange, symbol=symbol, timeframe=timeframe)
    raw = candle_buffer.last_n(key, n)  # ASC order from your buffer
    return [_normalize_candle(c) for c in raw]
