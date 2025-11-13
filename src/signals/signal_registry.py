# file: src/signals/signal_registry.py
# English-only comments

from __future__ import annotations
from datetime import datetime
import threading
from typing import Set, Tuple


def _minute_trunc(dt: datetime) -> datetime:
    """Truncate to minute precision (zero sec/microsec)."""
    return dt.replace(second=0, microsecond=0)


class SignalRegistry:
    """
    Process-lifetime, thread-safe registry to deduplicate signals ACROSS triggers.

    Uniqueness key (anchor of the real market scenario):
      (symbol_upper, side_lower, found_at_minute)

    - symbol_upper: e.g. "EUR/USD"
    - side_lower: "buy" | "sell"
    - found_at_minute: datetime truncated to minute (the pivot candle time for the TARGET symbol)
    """

    def __init__(self) -> None:
        self._seen: Set[Tuple[str, str, datetime]] = set()
        self._lock = threading.Lock()

    def remember(self, symbol: str, side: str, found_at: datetime) -> bool:
        """
        Returns True if this is a NEW scenario (not seen before), and stores it.
        Returns False if already seen.
        """
        if not isinstance(found_at, datetime):
            # Defensive: refuse to store invalid timestamps
            return False

        key = (symbol.upper(), side.lower(), _minute_trunc(found_at))
        with self._lock:
            if key in self._seen:
                return False
            self._seen.add(key)
            return True


# Singleton access ----------------------------------------------------

_REGISTRY_SINGLETON: SignalRegistry | None = None
_SINGLETON_LOCK = threading.Lock()


def get_signal_registry() -> SignalRegistry:
    """
    Returns the process-wide singleton SignalRegistry.
    """
    global _REGISTRY_SINGLETON
    if _REGISTRY_SINGLETON is None:
        with _SINGLETON_LOCK:
            if _REGISTRY_SINGLETON is None:
                _REGISTRY_SINGLETON = SignalRegistry()
    return _REGISTRY_SINGLETON
