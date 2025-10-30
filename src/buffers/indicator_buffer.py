# English-only comments

from dataclasses import dataclass
from collections import deque
from typing import Deque, Dict, Any, List

@dataclass(frozen=True)
class Keys:
    exchange: str
    symbol: str
    timeframe: str

class IndicatorBuffer:
    """
    Similar shape to CandleBuffer, but scoped per (Keys, indicator_name).
    API: get_or_create, ensure_capacity, append, last_n, get_len
    """
    def __init__(self, capacity_fn):
        # map key: (exchange, symbol, timeframe, indicator) -> deque[point]
        self.map: Dict[tuple, Deque[Dict[str, Any]]] = {}
        self.capacity_fn = capacity_fn  # function: (Keys, indicator_name) -> int

    def _k(self, key: Keys, indicator: str):
        return (key.exchange, key.symbol, key.timeframe, indicator)

    def get_or_create(self, key: Keys, indicator: str) -> Deque[Dict[str, Any]]:
        k = self._k(key, indicator)
        dq = self.map.get(k)
        if dq is None:
            dq = deque(maxlen=self.capacity_fn(key, indicator))
            self.map[k] = dq
        return dq

    def ensure_capacity(self, key: Keys, indicator: str, need: int) -> Deque[Dict[str, Any]]:
        dq = self.get_or_create(key, indicator)
        if dq.maxlen < need:
            newdq = deque(dq, maxlen=need)  # keep existing items, grow capacity
            self.map[self._k(key, indicator)] = newdq
            dq = newdq
        return dq

    def append(self, key: Keys, indicator: str, point: Dict[str, Any]) -> None:
        dq = self.get_or_create(key, indicator)
        dq.append(point)

    def last_n(self, key: Keys, indicator: str, n: int) -> List[Dict[str, Any]]:
        dq = self.get_or_create(key, indicator)
        if n <= 0:
            return []
        if n > len(dq):
            return list(dq)  # caller decides WARMUP if not enough
        return list(dq)[-n:]  # ASC order

    def get_len(self, key: Keys, indicator: str) -> int:
        dq = self.get_or_create(key, indicator)
        return len(dq)

    # English-only comments
    def last_value(self, key: Keys, indicator: str, field: str = "value", default=None):
        """
        Return the last single value for an indicator.
        Assumes each point is a dict like {"ts": ..., "value": ...}.
        Change `field` if you store a different key (e.g., "atr").
        """
        dq = self.get_or_create(key, indicator)
        if not dq:
            return default
        last_point = dq[-1]
        return last_point.get(field, default)

    def last_values(self, key: Keys, indicator: str, n: int, field: str = "value"):
        """
        Return the last `n` values for an indicator, in ascending time order.
        If fewer than `n` points exist, returns all available.
        """
        if n <= 0:
            return []
        dq = self.get_or_create(key, indicator)
        # Slice last n points, preserve ASC order
        items = list(dq)[-n:] if n <= len(dq) else list(dq)
        return [p.get(field) for p in items]