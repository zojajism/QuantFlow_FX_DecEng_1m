# English-only comments

from datetime import datetime
from typing import List, Tuple, Any, Dict

from pivots.pivot_buffer import PivotBufferRegistry, Pivot
from pivots.pivot_finder import compute_pivots
from adapters.candle_adapter import candles_for_pivots
from buffers.candle_buffer import CandleBuffer, Keys


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
    """
    Compute pivots for one (exchange, symbol, timeframe) and append
    new peaks/lows to PivotBufferRegistry, avoiding duplicates by time.
    """
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
    close_time: Any,
    candle_registry,                # CandleBuffer
    pivot_registry: PivotBufferRegistry,
    timeframe: str,
    symbols: List[Tuple[str, str]],
    *,
    n: int = 5,
    eps: float = 1e-9,
    strict: bool = False,
    hit_strict: bool = True
):
    """
    Called once all required symbols have delivered the candle for `close_time`.
    1) Update pivot buffers per symbol (using your exact pivot logic)
    2) Print a small debug snapshot (later: real rules)
    3) Dump full peak/low lists for a target symbol (EUR/USD by default)
    """
    print(f"[execute_strategy] close_time={close_time}", flush=True)

    # 1) Update pivots per symbol + show candle counts (to surface empty cases)
    for (exch, sym) in symbols:
        key = Keys(exchange=exch, symbol=sym, timeframe=timeframe)
        cnt_before = candle_registry.get_len(key)
        print(f"  [{exch}:{sym}] candles before compute = {cnt_before}", flush=True)

        update_pivot_buffers_for_symbol(
            candle_registry=candle_registry,
            pivot_registry=pivot_registry,
            exchange=exch,
            symbol=sym,
            timeframe=timeframe,
            n=n, eps=eps, strict=strict, hit_strict=hit_strict
        )

        pb = pivot_registry.get(exch, sym, timeframe)
        print(
            f"  [{exch}:{sym}] peaks={pb.count_peaks()} lows={pb.count_lows()}",
            flush=True
        )

    # 2) Per-symbol snapshot at this close_time
    '''
    for (exch, sym) in symbols:
        pb = pivot_registry.get(exch, sym, timeframe)
        p_at_t = pb.get_peak_by_time(close_time)
        l_at_t = pb.get_low_by_time(close_time)
        lp = pb.latest_peak()
        ll = pb.latest_low()
        print(
            f"  SNAP [{exch}:{sym}] "
            f"peak@t={_fmt_pivot(p_at_t)} low@t={_fmt_pivot(l_at_t)} | "
            f"latest_peak={_fmt_pivot(lp)} latest_low={_fmt_pivot(ll)}",
            flush=True
        )
    '''

    # 3) Dump full lists for a target symbol (newest -> oldest)
    target_exch = "OANDA"
    target_sym  = "GBP/USD"   # change here if you want another

    pb = pivot_registry.get(target_exch, target_sym, timeframe)
    print(f"\n------ All Peaks and Lows for {target_exch}:{target_sym} ({timeframe}) ------", flush=True)

    print("Peaks (newest -> oldest):", flush=True)
    printed_any = False
    for p in pb.iter_peaks_newest_first():
        print("  ", _fmt_pivot(p), flush=True)
        printed_any = True
    if not printed_any:
        print("  (none)", flush=True)

    print("Lows (newest -> oldest):", flush=True)
    printed_any = False
    for p in pb.iter_lows_newest_first():
        print("  ", _fmt_pivot(p), flush=True)
        printed_any = True
    if not printed_any:
        print("  (none)", flush=True)


def _fmt_time_min(t) -> str:
    """Format to 'YYYY-MM-DD HH:MM' for datetime/int/str."""
    if t is None:
        return "-"
    try:
        if isinstance(t, datetime):
            return t.strftime("%Y-%m-%d %H:%M")
        # epoch seconds (int/float)?
        if isinstance(t, (int, float)):
            return datetime.utcfromtimestamp(int(t)).strftime("%Y-%m-%d %H:%M")
        # ISO string or other -> best-effort slice
        s = str(t)
        # typical ISO: 'YYYY-MM-DD HH:MM:SS' or 'YYYY-MM-DDTHH:MM:SS'
        # take first 16 chars to get YYYY-MM-DD HH:MM
        return s.replace("T", " ")[:16]
    except Exception:
        return str(t)


def _fmt_pivot(p: Pivot | None) -> str:
    if p is None:
        return "-"
    price_str = f"{p.price:.5f}" if isinstance(p.price, float) else str(p.price)
    return f"(open={_fmt_time_min(p.open_time)}, price={price_str}, hit={p.is_hit})"
