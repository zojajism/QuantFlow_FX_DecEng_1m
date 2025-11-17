# file: src/entry.py
# English-only comments

from datetime import datetime
from typing import List, Tuple, Any, Dict
import threading

from sync.symbol_close_gate import SymbolCloseGate
from pivots.pivot_buffer import PivotBufferRegistry
from pivots.pivot_registry_provider import get_pivot_registry  # shared singleton
from strategy.execute import execute_strategy
from strategy.pivot_corr_engine import run_decision_event, SignalMemory
from database.db_general import get_pg_conn

# NEW: broker sync (order management)
from orders.order_executor import sync_broker_orders


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


# ---------- PivotList snapshot helpers ----------

def _gather_pivot_list_rows(
    *,
    event_time: datetime,
    timeframe: str,
) -> List[tuple]:
    """
    Build the full rows for pivot_list at this event_time across all EXPECTED_SYMBOLS.
    Returns list of tuples:
      (event_time, symbol, pivot_type, pivot_time, pivot_open_time, price, hit)
    """
    rows: List[tuple] = []

    pairs: List[Tuple[str, str]] = [(ex, sym) for (ex, sym) in EXPECTED_SYMBOLS]

    for exchange, symbol in pairs:
        pb = _pivot_registry.get(exchange, symbol, timeframe)
        if pb is None:
            continue

        # Iterate highs
        try:
            for p in pb.iter_peaks_newest_first():
                pivot_time = getattr(p, "close_time", None) or getattr(p, "time", None)
                pivot_open_time = getattr(p, "open_time", None)
                price = getattr(p, "level", None)
                if price is None:
                    price = getattr(p, "price", None)
                hit = bool(getattr(p, "hit", getattr(p, "is_hit", False)))

                rows.append((
                    event_time,            # event_time
                    symbol,                # symbol
                    "HIGH",                # pivot_type
                    pivot_time,            # pivot_time (close_time of that candle)
                    pivot_open_time,       # pivot_open_time (if available)
                    price,                 # price
                    hit,                   # hit
                ))
        except Exception:
            pass

        # Iterate lows
        try:
            for p in pb.iter_lows_newest_first():
                pivot_time = getattr(p, "close_time", None) or getattr(p, "time", None)
                pivot_open_time = getattr(p, "open_time", None)
                price = getattr(p, "level", None)
                if price is None:
                    price = getattr(p, "price", None)
                hit = bool(getattr(p, "hit", getattr(p, "is_hit", False)))

                rows.append((
                    event_time,
                    symbol,
                    "LOW",
                    pivot_time,
                    pivot_open_time,
                    price,
                    hit,
                ))
        except Exception:
            pass

    return rows


def _async_insert_pivot_list(rows: List[tuple]) -> None:
    """
    Insert rows into pivot_list in a background thread to avoid blocking.
    Schema: (event_time, symbol, pivot_type, pivot_time, pivot_open_time, price, hit)
    """
    if not rows:
        return

    def _worker(batch: List[tuple]) -> None:
        try:
            conn = get_pg_conn()
            sql = """
                INSERT INTO pivot_list (
                    event_time, symbol, pivot_type, pivot_time, pivot_open_time, price, hit
                ) VALUES (%s,%s,%s,%s,%s,%s,%s)
            """
            with conn.cursor() as cur:
                cur.executemany(sql, batch)
            conn.commit()
            conn.close()
        except Exception as e:
            # Soft-fail: keep processing without raising
            print(f"[pivot_list insert] error: {e}")

    t = threading.Thread(target=_worker, args=(rows,), daemon=True)
    t.start()


# ---------- Public API ----------

def init_entry(candle_buffer):
    """
    Pass your global CandleBuffer instance here, e.g. buffers.CANDLE_BUFFER
    """
    global _candle_buffer
    _candle_buffer = candle_buffer


def on_candle_closed(exchange: str, symbol: str, timeframe: str, close_time: Any):
    """
    Call this once when you receive a CLOSED candle for (exchange,symbol,timeframe).

    Flow for each 1m event_time:
      - Wait until all EXPECTED_SYMBOLS for that minute are received (SymbolCloseGate)
      - Compute/update pivots (execute_strategy)
      - Snapshot pivot_list (async)
      - Run decision engine (run_decision_event) -> signals + orders + DB writes
      - Sync with broker (sync_broker_orders) -> slippage, profit, exit info, telegram
    """
    if timeframe != TIMEFRAME:
        return

    if _candle_buffer is None:
        print("[on_candle_closed] candle_buffer not initialized.")
        return

    # Wait until all symbols for this minute have arrived
    ready = _close_gate.mark_arrival(close_ts=close_time, exchange=exchange, symbol=symbol)
    if not ready:
        return

    # event_time is the 1-minute trigger
    event_time = minute_trunc(close_time)

    # --- Compute/update pivots into the shared registry ---
    execute_strategy(
        close_time=event_time,
        candle_registry=_candle_buffer,   # CandleBuffer
        pivot_registry=_pivot_registry,   # shared PivotBufferRegistry
        timeframe=TIMEFRAME,
        symbols=EXPECTED_SYMBOLS,
        n=5,
        eps=1e-9,
        strict=False,
        hit_strict=True,
    )

    # --- Snapshot all pivots into pivot_list (async) ---
    try:
        rows = _gather_pivot_list_rows(event_time=event_time, timeframe=TIMEFRAME)
        _async_insert_pivot_list(rows)
    except Exception as e:
        print(f"[pivot_list snapshot] error: {e}")

    # --- Decision engine + broker sync with one shared DB connection ---
    sigmem = SignalMemory()
    conn = get_pg_conn()
    try:
        # This will:
        #   - read pivots
        #   - raise signals
        #   - insert into pivot_loop_log + signals
        #   - send eligible orders to broker
        #   - update basic order fields (order_sent, broker_* ids, etc.)
        run_decision_event(
            exchange=exchange,
            symbols=[s for (_, s) in EXPECTED_SYMBOLS],  # flat list of symbol names
            timeframe=TIMEFRAME,
            event_time=event_time,
            signal_memory=sigmem,
            groups=SYMBOL_GROUPS,
            conn=conn,
        )

        # After decision & order send, reconcile with broker:
        #   - fill slippage_pips, actual_entry_*
        #   - detect closed trades and fill actual_exit_*, profit_pips, profit_ccy
        #   - send Telegram for broker-closed trades
        sync_broker_orders(conn)

    finally:
        try:
            conn.close()
        except Exception:
            pass


def get_pivot_registry() -> PivotBufferRegistry:
    return _pivot_registry
