# src/pivots/pivot_finder.py

from typing import List, Tuple, Any, Dict, Optional
from .pivot_buffer import Pivot
from .pivots_simple import detect_pivots


def compute_pivots(
    candles: List[Dict[str, Any]],
    *,
    n: int = 5,
    eps: float = 1e-9,
    symbol: Optional[str] = None,
    pip_size: Optional[float] = None,
    hit_tolerance_pips: float = 2.0,
    high_key: str = "High",
    low_key: str = "Low",
    time_key: str = "CloseTime",
    open_time_key: str = "OpenTime",
    strict: bool = False,
    hit_strict: bool = False,
) -> Tuple[List[Pivot], List[Pivot]]:
    """
    Thin wrapper around detect_pivots() to map outputs into Pivot objects.

    Parameters
    ----------
    candles : list[dict]
        Input candles with at least High/Low and time fields.
    n : int
        Window size for pivot detection (number of bars on each side).
    eps : float
        Tolerance for plateau merging (price equality).
    symbol : str | None
        FX symbol (e.g. "EUR/USD", "USD_JPY") used to infer pip size if pip_size is None.
    pip_size : float | None
        Explicit pip size override. If None, detect_pivots will infer from symbol or use default.
    hit_tolerance_pips : float
        Hit tolerance in pips for non-strict hit mode (hit_strict=False).
    high_key, low_key, time_key, open_time_key : str
        Keys used to read values from each candle dict.
    strict : bool
        Pivot window strictness:
          - False: TV-like (left >= / <=, right > / <)
          - True : strict both sides (> / <)
    hit_strict : bool
        Hit evaluation strictness:
          - True : strictly > / < (no tolerance)
          - False: TV-like >= / <= with hit_tolerance_pips.
    """
    peaks_raw, lows_raw = detect_pivots(
        candles,
        n=n,
        eps=eps,
        symbol=symbol,
        pip_size=pip_size,
        hit_tolerance_pips=hit_tolerance_pips,
        high_key=high_key,
        low_key=low_key,
        time_key=time_key,
        open_time_key=open_time_key,
        strict=strict,
        hit_strict=hit_strict,
    )

    peaks = [
        Pivot(
            time=p["time"],
            open_time=p.get("open_time"),
            price=p["high"],
            is_hit=bool(p.get("hit", False)),
        )
        for p in peaks_raw
    ]

    lows = [
        Pivot(
            time=q["time"],
            open_time=q.get("open_time"),
            price=q["low"],
            is_hit=bool(q.get("hit", False)),
        )
        for q in lows_raw
    ]

    return peaks, lows
