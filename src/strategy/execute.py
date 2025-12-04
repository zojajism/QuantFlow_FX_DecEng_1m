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
    Reads from CandleBuffer and returns list[dict] with:
    High, Low, CloseTime, OpenTime
    (ASC order; oldest -> newest)
    """
    return candles_for_pivots(candle_buffer, exchange, symbol, timeframe, n=n)


def update_pivot_buffers_for_symbol(
    candle_registry,
    pivot_registry: PivotBufferRegistry,
    exchange: str,
    symbol: str,
    timeframe: str,
    *,
    n: int = 5,
    eps: float = 1e-9,
    strict: bool = False,
    hit_strict: bool = False,
    hit_tolerance_pips: float = 2.0,   # << new
):

    """
    Compute pivots for one (exchange, symbol, timeframe) and merge them into the
    PivotBufferRegistry while keeping buffers time-sorted by CLOSE time (p.time).
    """
    candles = get_last_n_candles(candle_registry, exchange, symbol, timeframe, n=400)

    peaks, lows = compute_pivots(
        candles,
        n=n,
        eps=eps,
        symbol=symbol,                      # << NEW: for pip size
        high_key="High",
        low_key="Low",
        time_key="CloseTime",
        open_time_key="OpenTime",
        strict=strict,
        hit_strict=hit_strict,
        hit_tolerance_pips=hit_tolerance_pips,   # << NEW: 2-pip “near-hit”
    )


    pb = pivot_registry.get(exchange, symbol, timeframe)

    # ---- Merge NEW + EXISTING by CLOSE time, keep chronological order (oldest -> newest) ----
    # Prefer NEW for the same pivot time (so hit flags/price refresh if logic updates)
    # Peaks
    existing_peaks_by_time = {p.time: p for p in pb.iter_peaks_newest_first()}  # newest->oldest
    for p in peaks:  # detector typically yields oldest->newest
        existing_peaks_by_time[p.time] = p

    sorted_peak_times = sorted(existing_peaks_by_time.keys())  # oldest->newest
    pb.highs.clear()
    for t in sorted_peak_times:
        pb.add_peak(existing_peaks_by_time[t])

    # Lows
    existing_lows_by_time = {q.time: q for q in pb.iter_lows_newest_first()}
    for q in lows:
        existing_lows_by_time[q.time] = q

    sorted_low_times = sorted(existing_lows_by_time.keys())  # oldest->newest
    pb.lows.clear()
    for t in sorted_low_times:
        pb.add_low(existing_lows_by_time[t])


def execute_strategy(
    close_time: Any,
    candle_registry,
    pivot_registry: PivotBufferRegistry,
    timeframe: str,
    symbols: List[Tuple[str, str]],
    *,
    n: int = 5,
    eps: float = 1e-9,
    strict: bool = False,
    hit_strict: bool = False,
    hit_tolerance_pips: float = 2.0,   # << new
):

    """
    Called once all required symbols have delivered the candle for `close_time`.
    1) Update pivot buffers per symbol
    2) Print small debug snapshot
    3) Dump full peak/low lists for a target symbol
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
            n=n,
            eps=eps,
            strict=strict,
            hit_strict=hit_strict,
            hit_tolerance_pips=hit_tolerance_pips,   # << pass through
        )


        pb = pivot_registry.get(exch, sym, timeframe)
        print(
            f"  [{exch}:{sym}] peaks={pb.count_peaks()} lows={pb.count_lows()}",
            flush=True
        )

    # 2) Per-symbol snapshot at this close_time (optional block kept commented)
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

    
    # 3) Dump full lists for a target symbol (newest -> oldest by CLOSE time)
    target_exch = "OANDA"
    target_sym  = "EUR/USD"   # change here if you want another

    pb = pivot_registry.get(target_exch, target_sym, timeframe)
    print(f"\n------ All Peaks and Lows for {target_exch}:{target_sym} ({timeframe}) ------", flush=True)

    # Ensure newest->oldest by CLOSE time when printing (even if buffers are already sorted)
    peaks_list = list(pb.iter_peaks_newest_first())
    peaks_list.sort(key=lambda p: p.time, reverse=True)

    print("Peaks (newest -> oldest):", flush=True)
    if not peaks_list:
        print("  (none)", flush=True)
    else:
        for p in peaks_list:
            print("  ", _fmt_pivot(p), flush=True)

    lows_list = list(pb.iter_lows_newest_first())
    lows_list.sort(key=lambda p: p.time, reverse=True)

    print("Lows (newest -> oldest):", flush=True)
    if not lows_list:
        print("  (none)", flush=True)
    else:
        for p in lows_list:
            print("  ", _fmt_pivot(p), flush=True)
   
    '''

def _fmt_time_min(t) -> str:
    """Format to 'YYYY-MM-DD HH:MM' for datetime/int/str."""
    if t is None:
        return "-"
    try:
        if isinstance(t, datetime):
            return t.strftime("%Y-%m-%d %H:%M")
        if isinstance(t, (int, float)):
            return datetime.utcfromtimestamp(int(t)).strftime("%Y-%m-%d %H:%M")
        s = str(t)
        return s.replace("T", " ")[:16]
    except Exception:
        return str(t)


def _fmt_pivot(p: Pivot | None) -> str:
    """
    Print using OpenTime (as requested) but NOTE: ordering is by CLOSE time (p.time).
    """
    if p is None:
        return "-"
    price_str = f"{p.price:.5f}" if isinstance(p.price, float) else str(p.price)
    return f"(close={_fmt_time_min(p.time)}, price={price_str}, hit={p.is_hit})"
