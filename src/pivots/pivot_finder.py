# src/pivots/pivot_finder.py
from typing import List, Tuple, Any, Dict
from .pivot_buffer import Pivot
from .pivots_simple import detect_pivots


def compute_pivots(
    candles: List[Dict[str, Any]],
    *,
    n: int = 3,
    eps: float = 1e-9,
    high_key: str = "High",
    low_key: str  = "Low",
    time_key: str = "CloseTime",
    open_time_key: str = "OpenTime",
    strict: bool = False,
    hit_strict: bool = True,
) -> Tuple[List[Pivot], List[Pivot]]:
    """
    Thin wrapper around detect_pivots() to map outputs into Pivot objects.
    """
    peaks_raw, lows_raw = detect_pivots(
        candles,
        n=n,
        eps=eps,
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
