# English-only comments

from typing import Optional, Dict, Any
from buffers.indicator_buffer import IndicatorBuffer, Keys

class IndicatorBufferProvider:
    """
    Concrete DataProvider that serves indicator 'points' out of IndicatorBuffer.

    Assumptions:
      - Indicators are appended to IndicatorBuffer with canonical lowercase names.
      - Each point has:
          {
            "indicator": <name>,
            "value": <float>,
            "close_time": <datetime or int>,
            "close_price": <float>
          }
      - Offsets:
          offset=0 -> last closed candle's point
          offset=1 -> previous closed candle's point
          ...
    """

    def __init__(self, exchange: str, indicator_buffer: IndicatorBuffer):
        self.exchange = exchange
        self.ib = indicator_buffer

    def get_point(self, symbol: str, tf: str, name: str, offset: int = 0) -> Optional[Dict[str, Any]]:
        """
        Return the indicator point dict for (symbol, tf, name) at the given offset,
        or None if not enough data exists.
        """
        if offset < 0:
            offset = 0

        key = Keys(exchange=self.exchange, symbol=symbol, timeframe=tf)

        # We need the last (offset+1) points to retrieve element at that offset
        needed = offset + 1
        points = self.ib.last_n(key, name, needed)  # ASC order list
        if len(points) < needed:
            return None

        # points[-1] is offset=0 (last element), points[-2] is offset=1, etc.
        return points[-1 - offset]
