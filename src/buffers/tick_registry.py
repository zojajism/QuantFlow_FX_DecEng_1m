# file: buffers/tick_registry.py
# English-only comments

from __future__ import annotations
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from threading import Lock
from typing import Deque, Dict, NamedTuple, Optional


class TickKey(NamedTuple):
    exchange: str
    symbol: str


@dataclass
class Tick:
    time: datetime
    bid: float
    ask: float


class TickRegistry:
    """
    Very small in-memory registry for last ticks per (exchange, symbol).

    - We keep only a small history (maxlen=capacity, default 5)
      mainly for debugging / monitoring.
    - For logic (e.g. position_price), we usually only need the last tick.
    """
    def __init__(self, capacity: int = 5) -> None:
        self._capacity = capacity
        self._data: Dict[TickKey, Deque[Tick]] = {}
        self._lock = Lock()

    def update_tick(
        self,
        exchange: str,
        symbol: str,
        bid: float,
        ask: float,
        tick_time: datetime,
    ) -> None:
        """
        Append a new tick for (exchange, symbol).
        If the deque does not exist, it will be created.
        """
        key = TickKey(exchange, symbol)
        with self._lock:
            dq = self._data.get(key)
            if dq is None:
                dq = deque(maxlen=self._capacity)
                self._data[key] = dq
            dq.append(Tick(time=tick_time, bid=bid, ask=ask))

    def get_last_tick(self, exchange: str, symbol: str) -> Optional[Tick]:
        """
        Return the most recent tick for (exchange, symbol), or None
        if we have never seen a tick for this key.
        """
        key = TickKey(exchange, symbol)
        with self._lock:
            dq = self._data.get(key)
            if not dq:
                return None
            return dq[-1]

    def get_price_for_side(
        self,
        exchange: str,
        symbol: str,
        side: str,  # "BUY" or "SELL"
    ) -> Optional[float]:
        """
        Convenience helper:
        - For BUY -> we pay ASK
        - For SELL -> we hit BID
        """
        last = self.get_last_tick(exchange, symbol)
        if last is None:
            return None

        side_upper = side.upper()
        if side_upper == "BUY":
            return last.ask
        elif side_upper == "SELL":
            return last.bid
        else:
            return None
