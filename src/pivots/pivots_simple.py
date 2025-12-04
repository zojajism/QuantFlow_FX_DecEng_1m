# pivots_simple.py
# Minimal pivot (swing high/low) detector with plateau consolidation (LAST bar pick).
# Adds:
#   - open_time in outputs
#   - "hit" flag per pivot (peak: any later higher high? low: any later lower low?)
#   - MIN_PIVOT_DISTANCE filter: enforce minimum candle distance between pivots
#   - symbol-aware pip-size + hit tolerance in pips
#
# API:
#   detect_pivots(
#       candles: list[dict],
#       n: int = 5,
#       eps: float = 1e-9,
#       *,
#       symbol: Optional[str] = None,        # e.g. "EUR/USD", "USD_JPY", "EURUSD"
#       pip_size: Optional[float] = None,    # override; if None, inferred from symbol or default
#       hit_tolerance_pips: float = 2.0,     # tolerance around pivot price for "hit" (non-strict mode)
#       high_key: str = "High",
#       low_key: str  = "Low",
#       time_key: str = "CloseTime",
#       open_time_key: str = "OpenTime",
#       strict: bool = False,                # window test: if False -> TV-like, else fully strict
#       hit_strict: bool = True,             # hit test: strict if True, else TV-like + tolerance
#   ) -> tuple[list[dict], list[dict]]
#
# Returns:
#   peaks: list of {"index": int, "time": any, "open_time": any, "high": float, "hit": bool}
#   lows : list of {"index": int, "time": any, "open_time": any, "low":  float, "hit": bool}

from typing import List, Dict, Tuple, Any, Optional
import numpy as np

from public_module import config_data

# Maximum “plateau gap” in terms of candle index between pivot candidates
# that we *might* want to treat as the same plateau.
# With the current consolidation logic (consecutive True mask), this effectively
# means we only merge immediately adjacent bars that are both pivots.
PLATEAU_MAX_GAP = 1

# Minimum candle distance between two pivots of the same type (peak/low).
# If two candidate pivots are closer than this, we will keep only one of them:
#   - for peaks: keep the one with the higher high
#   - for lows : keep the one with the lower low
MIN_PIVOT_DISTANCE = int(config_data.get("PIVOT_SIDE_CANDLES", [5])[0])

# Default hit-tolerance behavior
HIT_TOLERANCE_PIPS_DEFAULT = float(config_data.get("PIVOT_HIT_TOLERANCE_PIPS", [2])[0])
DEFAULT_PIP_SIZE = 0.0001  # default FX pip size (non-JPY)


def _pip_size_for_symbol(symbol: str) -> float:
    """
    Infer pip size from FX symbol.

    Very simple rule:
      - If quote currency is JPY -> 0.01
      - Otherwise -> 0.0001

    Symbol can be like "EUR/USD", "EUR_USD", or "EURUSD".
    """
    s = symbol.replace("/", "").replace("_", "").upper()
    if len(s) >= 6:
        quote = s[-3:]
    else:
        # Fallback: can't parse, return default
        return DEFAULT_PIP_SIZE

    if quote == "JPY":
        return 0.01
    else:
        return DEFAULT_PIP_SIZE


def _enforce_min_pivot_distance(
    indices: np.ndarray,
    values: np.ndarray,
    *,
    is_peak: bool,
    min_dist: int,
    eps: float,
) -> List[int]:
    """
    Enforce a minimum candle distance between pivots of the same type.

    Strategy:
      - Walk through candidate pivot indices in ascending order.
      - If the new candidate is within `min_dist` candles of the last kept pivot:
          * decide which one to keep:
              - peaks : keep the one with larger `values[idx]`
              - lows  : keep the one with smaller `values[idx]`
          * update the last kept pivot (replace) or drop the new one.
      - Otherwise, accept the new candidate as a separate pivot.
    """
    if min_dist is None or min_dist <= 1 or len(indices) == 0:
        # Nothing to do
        return [int(i) for i in indices]

    kept: List[int] = []
    for idx in indices:
        idx = int(idx)
        if not kept:
            kept.append(idx)
            continue

        last_idx = kept[-1]
        if idx - last_idx < min_dist:
            # Too close -> choose better pivot between idx and last_idx
            v_new = float(values[idx])
            v_old = float(values[last_idx])

            if is_peak:
                # prefer higher high for peaks
                if v_new > v_old + eps:
                    kept[-1] = idx
                # else keep old one
            else:
                # prefer lower low for lows
                if v_new < v_old - eps:
                    kept[-1] = idx
                # else keep old one
        else:
            kept.append(idx)

    return kept


def detect_pivots(
    candles: List[Dict[str, Any]],
    n: int = 5,
    eps: float = 1e-9,
    *,
    symbol: Optional[str] = None,
    pip_size: Optional[float] = None,
    hit_tolerance_pips: float = HIT_TOLERANCE_PIPS_DEFAULT,
    high_key: str = "High",
    low_key: str = "Low",
    time_key: str = "CloseTime",
    open_time_key: str = "OpenTime",
    strict: bool = False,
    hit_strict: bool = False,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Detect swing highs/lows with plateau merge to the LAST bar; include open_time and hit flags."""

    # ---- Extract arrays ----
    m = len(candles)
    H = np.fromiter((c[high_key] for c in candles), dtype=float, count=m)
    L = np.fromiter((c[low_key] for c in candles), dtype=float, count=m)
    T_close = [c[time_key] for c in candles]
    T_open = [c.get(open_time_key, None) for c in candles]  # tolerate missing key

    # ---- Edge cases ----
    if m == 0 or m < 2 * n + 1:
        return [], []

    # ---- Windowed pivot test (vectorized) ----
    left, right = n, m - n
    h_mid = H[left:right]
    l_mid = L[left:right]

    # We allow two modes:
    #   strict=True  -> both sides must be strictly greater/less
    #   strict=False -> TV-like:
    #                     left side: >= / <=
    #                     right side: > / <
    if strict:
        cmp_hi_left = lambda a, b: a > b
        cmp_hi_right = lambda a, b: a > b
        cmp_lo_left = lambda a, b: a < b
        cmp_lo_right = lambda a, b: a < b
    else:
        cmp_hi_left = lambda a, b: a >= b
        cmp_hi_right = lambda a, b: a > b
        cmp_lo_left = lambda a, b: a <= b
        cmp_lo_right = lambda a, b: a < b

    peak_mask = np.ones(right - left, dtype=bool)
    low_mask = np.ones(right - left, dtype=bool)

    for k in range(1, n + 1):
        peak_mask &= (
            cmp_hi_left(h_mid, H[left - k : right - k])
            & cmp_hi_right(h_mid, H[left + k : right + k])
        )
        low_mask &= (
            cmp_lo_left(l_mid, L[left - k : right - k])
            & cmp_lo_right(l_mid, L[left + k : right + k])
        )

    is_peak = np.zeros(m, dtype=bool)
    is_low = np.zeros(m, dtype=bool)
    is_peak[left:right] = peak_mask
    is_low[left:right] = low_mask

    # ---- Plateau consolidation (pick LAST bar) ----
    def consolidate(mask: np.ndarray, vals: np.ndarray) -> np.ndarray:
        """
        Merge consecutive True with near-equal values (<= eps) into one mark.
        We ALWAYS keep the last bar of the plateau.
        Only *adjacent* True indices are considered part of the same plateau.
        """
        if eps is None:
            return mask
        out = np.zeros_like(mask, dtype=bool)
        i = 0
        while i < m:
            if not mask[i]:
                i += 1
                continue
            j = i
            # grow run while subsequent True and near-equal value
            while (
                j + 1 < m
                and mask[j + 1]
                and abs(vals[j + 1] - vals[i]) <= eps
            ):
                j += 1
            # keep LAST bar of plateau (j), not the center
            keep = j
            out[keep] = True
            i = j + 1
        return out

    is_peak = consolidate(is_peak, H)
    is_low = consolidate(is_low, L)

    # ---- Hit evaluation (look to the RIGHT, skipping n bars that formed the pivot) ----

    def _future_high_after(pivot_idx: int) -> float:
        """
        Max High strictly to the right of the pivot,
        starting AFTER the n candles that helped form the pivot on the right side.
        If no candles remain, return -inf.
        """
        start = pivot_idx + n + 1
        if start >= m:
            return float("-inf")
        return float(np.max(H[start:]))

    def _future_low_after(pivot_idx: int) -> float:
        """
        Min Low strictly to the right of the pivot,
        starting AFTER the n candles that helped form the pivot on the right side.
        If no candles remain, return +inf.
        """
        start = pivot_idx + n + 1
        if start >= m:
            return float("inf")
        return float(np.min(L[start:]))

    # ---- Choose hit functions ----
    if hit_strict:
        # Strict: only real breaks (> / <), no tolerance
        peak_hit_fn = lambda idx: bool(_future_high_after(idx) > H[idx])
        low_hit_fn = lambda idx: bool(_future_low_after(idx) < L[idx])
    else:
        # Non-strict (TV-like) + symbol-aware tolerance in pips
        if pip_size is None:
            if symbol is not None:
                pip_size = _pip_size_for_symbol(symbol)
            else:
                pip_size = DEFAULT_PIP_SIZE

        if hit_tolerance_pips <= 0:
            # Degenerate case: behave like pure >= / <=
            peak_hit_fn = lambda idx: bool(
                _future_high_after(idx) >= H[idx]
            )
            low_hit_fn = lambda idx: bool(
                _future_low_after(idx) <= L[idx]
            )
        else:
            hit_tol = float(pip_size) * float(hit_tolerance_pips)

            # HIGH pivot:
            #   hit if future high >= (pivot high - hit_tol)
            #   -> can be higher, equal, or up to `hit_tol` below
            peak_hit_fn = lambda idx, tol=hit_tol: bool(
                _future_high_after(idx) >= H[idx] - tol
            )

            # LOW pivot:
            #   hit if future low <= (pivot low + hit_tol)
            #   -> can be lower, equal, or up to `hit_tol` above
            low_hit_fn = lambda idx, tol=hit_tol: bool(
                _future_low_after(idx) <= L[idx] + tol
            )

    # ---- Build indices and enforce MIN_PIVOT_DISTANCE ----
    peak_idx_raw = np.flatnonzero(is_peak)
    low_idx_raw = np.flatnonzero(is_low)

    # Apply minimum distance filtering separately for peaks and lows
    peak_idx = _enforce_min_pivot_distance(
        peak_idx_raw,
        H,
        is_peak=True,
        min_dist=MIN_PIVOT_DISTANCE,
        eps=eps,
    )
    low_idx = _enforce_min_pivot_distance(
        low_idx_raw,
        L,
        is_peak=False,
        min_dist=MIN_PIVOT_DISTANCE,
        eps=eps,
    )

    # ---- Build compact outputs (with index + times + price + hit) ----
    peaks = [
        {
            "index": int(i),
            "time": T_close[i],
            "open_time": T_open[i],
            "high": float(H[i]),
            "hit": peak_hit_fn(i),
        }
        for i in peak_idx
    ]

    lows = [
        {
            "index": int(i),
            "time": T_close[i],
            "open_time": T_open[i],
            "low": float(L[i]),
            "hit": low_hit_fn(i),
        }
        for i in low_idx
    ]

    return peaks, lows


# --------------------- Optional quick self-test ---------------------
if __name__ == "__main__":
    from datetime import datetime, timedelta

    base = datetime(2025, 1, 1, 9, 30, 0)

    highs = [
        1,
        2,
        3,
        3,
        3,
        2,
        1,
        1.8,
        2.0,
        1.9,
        2.1,
        2.1,
        2.1,
        2.0,
        1.7,
        1.8,
        1.85,
        1.75,
        1.7,
        1.65,
        1.9,
    ]
    lows = [
        0,
        0.5,
        1,
        1,
        1,
        0.8,
        0.6,
        0.7,
        0.8,
        0.75,
        0.6,
        0.6,
        0.6,
        0.65,
        0.7,
        0.72,
        0.8,
        0.7,
        0.68,
        0.66,
        0.7,
    ]
    candles = []
    for i, (h, l) in enumerate(zip(highs, lows)):
        candles.append(
            {
                "High": h,
                "Low": l,
                "OpenTime": base + timedelta(minutes=i),
                "CloseTime": base + timedelta(minutes=i + 1),
            }
        )

    peaks, lows_out = detect_pivots(
        candles,
        n=2,
        eps=1e-9,
        symbol="EUR/USD",
        hit_tolerance_pips=2.0,
        high_key="High",
        low_key="Low",
        time_key="CloseTime",
        open_time_key="OpenTime",
        strict=False,
        hit_strict=False,
    )

    print("Peaks:")
    for p in peaks:
        print(
            f"  idx={p['index']:2d} open={p['open_time']} "
            f"close={p['time']} high={p['high']:.4f} hit={p['hit']}"
        )

    print("Lows:")
    for q in lows_out:
        print(
            f"  idx={q['index']:2d} open={q['open_time']} "
            f"close={q['time']} low ={q['low']:.4f} hit={q['hit']}"
        )
