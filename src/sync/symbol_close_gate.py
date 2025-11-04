# src/sync/symbol_close_gate.py
from collections import defaultdict
from typing import Set, Dict, Tuple, Any

class SymbolCloseGate:
    """
    Tracks which symbols have delivered a CLOSED candle for a given close_time.
    When all expected symbols are present for a close_time, it returns True once.
    """

    def __init__(self, expected_symbols: Set[Tuple[str, str]]):
        """
        expected_symbols: set of (exchange, symbol)
        """
        self.expected_symbols = set(expected_symbols)
        self._arrivals: Dict[Any, Set[Tuple[str, str]]] = defaultdict(set)
        self._released: Set[Any] = set()

    def mark_arrival(self, close_ts: Any, exchange: str, symbol: str) -> bool:
        """
        Register that (exchange, symbol) delivered a CLOSED candle for close_ts.
        Return True exactly once when all expected symbols have arrived for close_ts.
        """
        key = (exchange, symbol)
        if key not in self.expected_symbols:
            return False

        self._arrivals[close_ts].add(key)
        if close_ts in self._released:
            return False

        if self._arrivals[close_ts] == self.expected_symbols:
            self._released.add(close_ts)
            return True

        return False
