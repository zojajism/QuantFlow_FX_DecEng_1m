# English-only comments

from dataclasses import dataclass
from collections import deque
from typing import Deque, Dict, Any, List

@dataclass(frozen=True)
class Keys:
    exchange: str
    symbol: str
    timeframe: str

class CandleBuffer:
    def __init__(self, capacity_fn):
        self.map: Dict[Keys, Deque[Dict[str, Any]]] = {}
        self.capacity_fn = capacity_fn  # function: Keys -> int

    def get_or_create(self, key: Keys) -> Deque[Dict[str, Any]]:
        dq = self.map.get(key)
        if dq is None:
            dq = deque(maxlen=self.capacity_fn(key))
            self.map[key] = dq
        return dq

    def ensure_capacity(self, key: Keys, need: int) -> Deque[Dict[str, Any]]:
        dq = self.get_or_create(key)
        if dq.maxlen < need:
            newdq = deque(dq, maxlen=need)  # keep existing items, grow capacity
            self.map[key] = newdq
            dq = newdq
        return dq

    def append(self, key: Keys, candle: Dict[str, Any]) -> None:
        dq = self.get_or_create(key)
        dq.append(candle)

    def last_n(self, key: Keys, n: int) -> List[Dict[str, Any]]:
        dq = self.get_or_create(key)
        if n <= 0:
            return []
        if n > len(dq):
            return list(dq)  # caller decides WARMUP if not enough
        return list(dq)[-n:]  # ASC order

    def get_len(self, key: Keys) -> int:
        dq = self.get_or_create(key)
        return len(dq)
    
    