# English-only comments

from typing import List, Tuple, Set, Any
from sync.symbol_close_gate import SymbolCloseGate
from pivots.pivot_buffer import PivotBufferRegistry
from strategy.execute import execute_strategy

# ---- Configuration ----
TIMEFRAME = "1m"
EXPECTED_SYMBOLS: List[Tuple[str, str]] = [
    ("OANDA", "EUR/USD"),
    ("OANDA", "GBP/USD"),
    ("OANDA", "USD/CHF"),
    ("OANDA", "AUD/USD"),
    ("OANDA", "DXY/DXY"),
]

# ---- Singletons ----
_pivot_registry = PivotBufferRegistry(maxlen=300)
_close_gate = SymbolCloseGate(expected_symbols=set(EXPECTED_SYMBOLS))
_candle_buffer = None  # will point to your buffers.CANDLE_BUFFER

def init_entry(candle_buffer):
    """
    Pass your global CandleBuffer instance here, e.g. buffers.CANDLE_BUFFER
    """
    global _candle_buffer
    _candle_buffer = candle_buffer

def on_candle_closed(exchange: str, symbol: str, timeframe: str, close_time: Any):
    """
    Call this once when you receive a CLOSED candle for (exchange,symbol,timeframe).
    If all expected symbols have arrived for this close_time, execute strategy.
    """
    if timeframe != TIMEFRAME:
        return

    if _candle_buffer is None:
        print("[on_candle_closed] candle_buffer not initialized.")
        return

    ready = _close_gate.mark_arrival(close_ts=close_time, exchange=exchange, symbol=symbol)
    if not ready:
        return

    execute_strategy(
        close_time=close_time,
        candle_registry=_candle_buffer,   # <- pass CandleBuffer here
        pivot_registry=_pivot_registry,
        timeframe=TIMEFRAME,
        symbols=EXPECTED_SYMBOLS,
        n=5,
        eps=1e-9,
        strict=False,
        hit_strict=True
    )

def get_pivot_registry() -> PivotBufferRegistry:
    return _pivot_registry
