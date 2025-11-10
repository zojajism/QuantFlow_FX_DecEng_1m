# English-only comments

from datetime import datetime
from typing import List, Tuple, Any, Dict
from sync.symbol_close_gate import SymbolCloseGate
from pivots.pivot_buffer import PivotBufferRegistry
from pivots.pivot_registry_provider import get_pivot_registry  # <-- use shared singleton
from strategy.execute import execute_strategy
from strategy.pivot_corr_engine import run_decision_event, SignalMemory
from database.db_general import get_pg_conn

# ---- Configuration ----
TIMEFRAME = "1m"
EXPECTED_SYMBOLS: List[Tuple[str, str]] = [
    ("OANDA", "EUR/USD"),
    ("OANDA", "GBP/USD"),
    ("OANDA", "AUD/USD"),
    ("OANDA", "USD/CHF"),
    ("OANDA", "DXY/DXY"),
]

def minute_trunc(dt: datetime) -> datetime:
    """Truncate to minute precision (zero sec/microsec)."""
    return dt.replace(second=0, microsecond=0)

# ---- GROUPING (names don't matter) ----
SYMBOL_GROUPS: Dict[str, List[str]] = {
    "USD_Majors": ["EUR/USD", "GBP/USD", "AUD/USD"],
    "DXY_Mirror": ["USD/CHF", "DXY/DXY"],
}
# Or single group:
# SYMBOL_GROUPS = {"All": [s for _, s in EXPECTED_SYMBOLS]}

# ---- Singletons ----
# IMPORTANT: use the shared provider so both writer and reader see the SAME registry
_pivot_registry: PivotBufferRegistry = get_pivot_registry()
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

    # --- Compute/update pivots into the SAME registry singleton ---
    execute_strategy(
        close_time=close_time,
        candle_registry=_candle_buffer,   # <- pass CandleBuffer here
        pivot_registry=_pivot_registry,   # <- SAME instance as the decision engine uses
        timeframe=TIMEFRAME,
        symbols=EXPECTED_SYMBOLS,
        n=5,
        eps=1e-9,
        strict=False,
        hit_strict=True
    )

    # --- Decision engine call with grouping + DB connection ---
    sigmem = SignalMemory()
    conn = get_pg_conn()

    run_decision_event(
        exchange=exchange,
        symbols=[s for (_, s) in EXPECTED_SYMBOLS],  # flat list of symbol names
        timeframe=TIMEFRAME,
        event_time=minute_trunc(close_time),
        signal_memory=sigmem,
        groups=SYMBOL_GROUPS,
        conn=conn,   # <-- give DB connection so rows get inserted
    )

def get_pivot_registry() -> PivotBufferRegistry:
    return _pivot_registry
