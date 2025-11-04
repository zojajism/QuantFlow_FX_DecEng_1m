# English-only comments

from typing import List, Tuple, Any, Dict
from pivots.pivot_buffer import PivotBufferRegistry, Pivot
from pivots.pivot_finder import compute_pivots
from adapters.candle_adapter import candles_for_pivots
from buffers.candle_buffer import CandleBuffer

# --- Adapter: get last N CLOSED candles in the required schema ---
def get_last_n_candles(
    candle_buffer: CandleBuffer,
    exchange: str,
    symbol: str,
    timeframe: str,
    n: int = 400
) -> List[Dict[str, Any]]:
    """
    Reads from your CandleBuffer and returns list[dict] with:
    High, Low, CloseTime, OpenTime
    """
    return candles_for_pivots(candle_buffer, exchange, symbol, timeframe, n=n)

def update_pivot_buffers_for_symbol(
    candle_registry,                   # kept name for compatibility; it's CandleBuffer instance
    pivot_registry: PivotBufferRegistry,
    exchange: str,
    symbol: str,
    timeframe: str,
    *,
    n: int = 5,
    eps: float = 1e-9,
    strict: bool = False,
    hit_strict: bool = True
):
    candles = get_last_n_candles(candle_registry, exchange, symbol, timeframe, n=400)

    peaks, lows = compute_pivots(
        candles,
        n=n, eps=eps,
        high_key="High", low_key="Low",
        time_key="CloseTime", open_time_key="OpenTime",
        strict=strict, hit_strict=hit_strict
    )

    pb = pivot_registry.get(exchange, symbol, timeframe)

    # Avoid duplicates by exact time
    existing_peak_times = {p.time for p in pb.iter_peaks_newest_first()}
    existing_low_times  = {p.time for p in pb.iter_lows_newest_first()}

    for p in peaks:
        if p.time not in existing_peak_times:
            pb.add_peak(p)

    for q in lows:
        if q.time not in existing_low_times:
            pb.add_low(q)

def execute_strategy(
    close_time: Any,  # hashable: datetime/epoch/int/str
    candle_registry,  # CandleBuffer instance
    pivot_registry: PivotBufferRegistry,
    timeframe: str,
    symbols: List[Tuple[str, str]],  # list of (exchange, symbol)
    *,
    n: int = 3,
    eps: float = 1e-9,
    strict: bool = False,
    hit_strict: bool = True
):
    """
    Called once all required symbols have delivered the candle for `close_time`.
    1) Update pivot buffers per symbol (using your exact pivot logic)
    2) Print a small debug snapshot (later: real rules)
    """
    for (exch, sym) in symbols:
        update_pivot_buffers_for_symbol(
            candle_registry=candle_registry,
            pivot_registry=pivot_registry,
            exchange=exch,
            symbol=sym,
            timeframe=timeframe,
            n=n, eps=eps, strict=strict, hit_strict=hit_strict
        )

    # Debug snapshot focused on the queried close_time
    print(f"[execute_strategy] close_time={close_time}")
    for (exch, sym) in symbols:
        pb = pivot_registry.get(exch, sym, timeframe)
        p_at_t = pb.get_peak_by_time(close_time)
        l_at_t = pb.get_low_by_time(close_time)
        lp = pb.latest_peak()
        ll = pb.latest_low()
        print(f"  {exch}:{sym} | peak@t={_fmt_pivot(p_at_t)} low@t={_fmt_pivot(l_at_t)} | "
              f"latest_peak={_fmt_pivot(lp)} latest_low={_fmt_pivot(ll)}")

def _fmt_pivot(p: Pivot | None) -> str:
    if p is None:
        return "-"
    price_str = f"{p.price:.5f}" if isinstance(p.price, float) else str(p.price)
    return f"(time={p.time}, open={p.open_time}, price={price_str}, hit={p.is_hit})"
