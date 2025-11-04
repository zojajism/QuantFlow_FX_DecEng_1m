# src/pivots/pivot_buffer.py
from collections import deque
from dataclasses import dataclass
from typing import Deque, Dict, Tuple, Optional, Iterable, Any

@dataclass
class Pivot:
    time: Any          # candle close time (epoch/datetime/ISO) — hashable/eq-friendly
    open_time: Any     # candle open time (optional)
    price: float       # high for peaks, low for lows
    is_hit: bool = False

class PivotBuffer:
    """
    Holds peaks and lows for a (exchange, symbol, timeframe) key,
    with simple, readable query helpers.
    """
    def __init__(self, maxlen: int = 300):
        self.highs: Deque[Pivot] = deque(maxlen=maxlen)
        self.lows:  Deque[Pivot] = deque(maxlen=maxlen)

    # ---- appenders ----
    def add_peak(self, p: Pivot) -> None:
        self.highs.append(p)

    def add_low(self, p: Pivot) -> None:
        self.lows.append(p)

    # ---- queries: latest / first ----
    def latest_peak(self) -> Optional[Pivot]:
        return self.highs[-1] if self.highs else None

    def latest_low(self) -> Optional[Pivot]:
        return self.lows[-1] if self.lows else None

    def first_peak(self) -> Optional[Pivot]:
        return self.highs[0] if self.highs else None

    def first_low(self) -> Optional[Pivot]:
        return self.lows[0] if self.lows else None

    # ---- query by exact close-time ----
    def get_peak_by_time(self, t: Any) -> Optional[Pivot]:
        for p in reversed(self.highs):  # newest -> oldest
            if p.time == t:
                return p
        return None

    def get_low_by_time(self, t: Any) -> Optional[Pivot]:
        for p in reversed(self.lows):
            if p.time == t:
                return p
        return None

    # ---- iteration (newest -> oldest) ----
    def iter_peaks_newest_first(self) -> Iterable[Pivot]:
        return reversed(self.highs)

    def iter_lows_newest_first(self) -> Iterable[Pivot]:
        return reversed(self.lows)

    # ---- counts ----
    def count_peaks(self) -> int:
        return len(self.highs)

    def count_lows(self) -> int:
        return len(self.lows)


class PivotBufferRegistry:
    """
    Registry keyed by (exchange, symbol, timeframe).
    """
    def __init__(self, maxlen: int = 300):
        self._buffers: Dict[Tuple[str, str, str], PivotBuffer] = {}
        self._maxlen = maxlen

    def _key(self, exchange: str, symbol: str, timeframe: str):
        return (exchange, symbol, timeframe)

    def get(self, exchange: str, symbol: str, timeframe: str) -> PivotBuffer:
        k = self._key(exchange, symbol, timeframe)
        if k not in self._buffers:
            self._buffers[k] = PivotBuffer(maxlen=self._maxlen)
        return self._buffers[k]

    # Convenience wrappers
    def add_peak(self, exchange: str, symbol: str, timeframe: str, pivot: Pivot):
        self.get(exchange, symbol, timeframe).add_peak(pivot)

    def add_low(self, exchange: str, symbol: str, timeframe: str, pivot: Pivot):
        self.get(exchange, symbol, timeframe).add_low(pivot)

    def latest_peak(self, exchange: str, symbol: str, timeframe: str):
        return self.get(exchange, symbol, timeframe).latest_peak()

    def latest_low(self, exchange: str, symbol: str, timeframe: str):
        return self.get(exchange, symbol, timeframe).latest_low()

    def get_peak_by_time(self, exchange: str, symbol: str, timeframe: str, t: Any):
        return self.get(exchange, symbol, timeframe).get_peak_by_time(t)

    def get_low_by_time(self, exchange: str, symbol: str, timeframe: str, t: Any):
        return self.get(exchange, symbol, timeframe).get_low_by_time(t)
