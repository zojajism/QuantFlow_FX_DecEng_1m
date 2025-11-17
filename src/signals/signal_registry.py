# file: src/signals/signal_registry.py
# English-only comments

from __future__ import annotations

from datetime import datetime, timezone
import threading
from typing import Set, Tuple

# We only need a lightweight DB helper that returns recent signals.
# If import fails, registry will just start empty.
try:
    from database.db_signals import fetch_recent_signals
except Exception as e:
    fetch_recent_signals = None
    print(f"[SignalRegistry] WARNING: could not import fetch_recent_signals: {e}")


def _minute_trunc(dt: datetime) -> datetime:
    """Truncate datetime to minute precision (zero seconds/microseconds)."""
    return dt.replace(second=0, microsecond=0)


Key = Tuple[str, str, datetime]  # (symbol_upper, side_lower, found_at_minute)


class SignalRegistry:
    """
    Process-lifetime, thread-safe registry to deduplicate signals ACROSS triggers.

    Uniqueness key:
      (symbol_upper, side_lower, found_at_minute)

    IMPORTANT:
      - Runtime signals usually come as timezone-aware UTC (tzinfo=Etc/UTC).
      - DB signals (PostgreSQL timestamp without time zone) are naive.
      - To make them comparable, we convert DB timestamps to UTC-aware
        (tzinfo=timezone.utc) before building the key.
    """

    def __init__(self) -> None:
        self._seen: Set[Key] = set()
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Key helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _normalize_from_db(found_at: datetime) -> datetime:
        """
        Normalize timestamps coming from DB so they match runtime format.

        DB returns naive datetimes (timestamp without time zone).
        We treat them as UTC and attach tzinfo=UTC, so they are
        comparable with runtime UTC-aware datetimes.
        """
        if found_at.tzinfo is None:
            # Assume DB timestamps are in UTC; attach tzinfo.
            return found_at.replace(tzinfo=timezone.utc)
        return found_at

    @staticmethod
    def make_key(symbol: str, side: str, found_at: datetime) -> Key:
        """
        Build the canonical dedup key for a signal.

        NOTE:
          - This function assumes `found_at` is already in the desired
            timezone format (UTC-aware) if needed.
          - For DB bootstrap, we call `_normalize_from_db` before this.
        """
        if not isinstance(found_at, datetime):
            raise TypeError("found_at must be a datetime")

        return (symbol.upper(), side.lower(), _minute_trunc(found_at))

    # ------------------------------------------------------------------
    # Core API
    # ------------------------------------------------------------------
    def remember(self, symbol: str, side: str, found_at: datetime) -> bool:
        """
        Returns True if this is a NEW scenario (not seen before), and stores it.
        Returns False if already seen.

        `found_at` here is supplied by runtime (strategy). It should already
        be a timezone-aware UTC datetime (Etc/UTC).
        """
        key = self.make_key(symbol, side, found_at)

        with self._lock:
            if key in self._seen:
                # Duplicate signal (already processed sometime in this process).
                return False

            self._seen.add(key)
            return True

    def has(self, symbol: str, side: str, found_at: datetime) -> bool:
        """
        Check if a given (symbol, side, found_at_minute) is already registered.
        """
        key = self.make_key(symbol, side, found_at)
        with self._lock:
            return key in self._seen

    def __len__(self) -> int:
        with self._lock:
            return len(self._seen)

    # ------------------------------------------------------------------
    # Bootstrap from DB (last N hours by found_at)
    # ------------------------------------------------------------------
    @classmethod
    def from_recent_db(cls, hours_back: int = 24) -> "SignalRegistry":
        """
        Create a registry and preload it from the DB.

        We use `found_at` as the anchor (pivot time for target symbol).
        All DB timestamps are converted to UTC-aware format so they match
        runtime datetimes used by the strategy.
        """
        reg = cls()

        if fetch_recent_signals is None:
            print(
                "[SignalRegistry] WARNING: fetch_recent_signals not available; "
                "starting registry empty."
            )
            return reg

        try:
            rows = fetch_recent_signals(hours_back=hours_back)
        except Exception as e:
            print(
                f"[SignalRegistry] WARNING: failed to bootstrap from DB "
                f"(hours_back={hours_back}): {e}"
            )
            return reg

        loaded = 0
        for row in rows:
            symbol = row.get("signal_symbol")
            side = row.get("position_type")
            found_at = row.get("found_at")

            if not symbol or not side or not isinstance(found_at, datetime):
                continue

            # Normalize DB timestamp to UTC-aware format, matching runtime.
            found_at_norm = cls._normalize_from_db(found_at)

            try:
                key = cls.make_key(symbol, side, found_at_norm)
            except Exception:
                continue

            with reg._lock:
                if key in reg._seen:
                    continue
                reg._seen.add(key)
                loaded += 1

        print(
            f"[SignalRegistry] Bootstrapped {loaded} unique keys from DB "
            f"(found_at last {hours_back}h)."
        )
        return reg


# ----------------------------------------------------------------------
# Singleton access
# ----------------------------------------------------------------------

_REGISTRY_SINGLETON: SignalRegistry | None = None
_SINGLETON_LOCK = threading.Lock()


def get_signal_registry() -> SignalRegistry:
    """
    Returns the process-wide singleton SignalRegistry.

    On first creation, it bootstraps from DB (last 24 hours by found_at).
    """
    global _REGISTRY_SINGLETON
    if _REGISTRY_SINGLETON is None:
        with _SINGLETON_LOCK:
            if _REGISTRY_SINGLETON is None:
                _REGISTRY_SINGLETON = SignalRegistry.from_recent_db(hours_back=24)
    return _REGISTRY_SINGLETON
