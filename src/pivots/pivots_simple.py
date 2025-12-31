# pivots_simple.py
# Minimal pivot (swing high/low) detector with plateau consolidation (LAST bar pick).
# Adds:
#   - open_time in outputs
#   - hit flag (strict or tolerance-based)
#   - MIN_PIVOT_DISTANCE filter
#   - symbol-aware pip-size + hit tolerance
#   - hit_distance = number of candles until pivot is hit (NEW & FIXED)

from typing import List, Dict, Tuple, Any, Optional
import numpy as np

from public_module import config_data

PLATEAU_MAX_GAP = 1
MIN_PIVOT_DISTANCE = int(config_data.get("PIVOT_SIDE_CANDLES", [5])[0])
HIT_TOLERANCE_PIPS_DEFAULT = float(config_data.get("PIVOT_HIT_TOLERANCE_PIPS", [2])[0])
DEFAULT_PIP_SIZE = 0.0001


def _pip_size_for_symbol(symbol: str) -> float:
    s = symbol.replace("/", "").replace("_", "").upper()
    if len(s) >= 6:
        quote = s[-3:]
    else:
        return DEFAULT_PIP_SIZE

    return 0.01 if quote == "JPY" else DEFAULT_PIP_SIZE


def _enforce_min_pivot_distance(indices, values, *, is_peak, min_dist, eps):
    if min_dist is None or min_dist <= 1 or len(indices) == 0:
        return [int(i) for i in indices]

    kept = []
    for idx in indices:
        idx = int(idx)
        if not kept:
            kept.append(idx)
            continue

        last_idx = kept[-1]
        if idx - last_idx < min_dist:
            v_new = float(values[idx])
            v_old = float(values[last_idx])

            if is_peak:
                if v_new > v_old + eps:
                    kept[-1] = idx
            else:
                if v_new < v_old - eps:
                    kept[-1] = idx
        else:
            kept.append(idx)

    return kept


# ---------------------------------------------------------------
# NEW FIXED VERSION OF hit_distance (correct tolerance logic)
# ---------------------------------------------------------------
def _hit_distance(
    pivot_idx: int,
    *,
    is_peak: bool,
    H: np.ndarray,
    L: np.ndarray,
    n: int,
    hit_tolerance: float
) -> Optional[int]:

    start = pivot_idx + n + 1
    m = len(H)

    if start >= m:
        return None

    if is_peak:
        pivot_price = H[pivot_idx]
        threshold = pivot_price - hit_tolerance

        for j in range(start, m):
            if H[j] >= threshold:
                return j - pivot_idx

    else:
        pivot_price = L[pivot_idx]
        threshold = pivot_price + hit_tolerance

        for j in range(start, m):
            if L[j] <= threshold:
                return j - pivot_idx

    return None


# ---------------------------------------------------------------


def detect_pivots(
    candles: List[Dict[str, Any]],
    n: int = 5,
    eps: float = 1e-9,
    *,
    symbol: Optional[str] = None,
    pip_size: Optional[float] = None,
    hit_tolerance_pips: float,
    high_key: str = "High",
    low_key: str = "Low",
    time_key: str = "CloseTime",
    open_time_key: str = "OpenTime",
    strict: bool = False,
    hit_strict: bool,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:

    m = len(candles)
    H = np.fromiter((c[high_key] for c in candles), float, count=m)
    L = np.fromiter((c[low_key] for c in candles), float, count=m)
    T_close = [c[time_key] for c in candles]
    T_open = [c.get(open_time_key) for c in candles]

    if m == 0 or m < 2 * n + 1:
        return [], []

    left, right = n, m - n
    h_mid = H[left:right]
    l_mid = L[left:right]

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

    peak_mask = np.ones(right - left, bool)
    low_mask = np.ones(right - left, bool)

    for k in range(1, n + 1):
        peak_mask &= (
            cmp_hi_left(h_mid, H[left - k:right - k])
            & cmp_hi_right(h_mid, H[left + k:right + k])
        )
        low_mask &= (
            cmp_lo_left(l_mid, L[left - k:right - k])
            & cmp_lo_right(l_mid, L[left + k:right + k])
        )

    is_peak = np.zeros(m, bool)
    is_low = np.zeros(m, bool)
    is_peak[left:right] = peak_mask
    is_low[left:right] = low_mask

    def consolidate(mask, vals):
        if eps is None:
            return mask
        out = np.zeros_like(mask, bool)
        i = 0
        while i < m:
            if not mask[i]:
                i += 1
                continue
            j = i
            while j + 1 < m and mask[j + 1] and abs(vals[j + 1] - vals[i]) <= eps:
                j += 1
            out[j] = True
            i = j + 1
        return out

    is_peak = consolidate(is_peak, H)
    is_low = consolidate(is_low, L)

    def _future_high_after(i):
        s = i + n + 1
        if s >= m:
            return float("-inf")
        return float(np.max(H[s:]))

    def _future_low_after(i):
        s = i + n + 1
        if s >= m:
            return float("inf")
        return float(np.min(L[s:]))

    if hit_strict:
        peak_hit_fn = lambda i: bool(_future_high_after(i) >= H[i])
        low_hit_fn = lambda i: bool(_future_low_after(i) <= L[i])
        hit_tol = 0.0
    else:
        if pip_size is None:
            pip_size = _pip_size_for_symbol(symbol) if symbol else DEFAULT_PIP_SIZE
        hit_tol = pip_size * hit_tolerance_pips

        peak_hit_fn = lambda i, tol=hit_tol: bool(_future_high_after(i) >= H[i] - tol)
        low_hit_fn = lambda i, tol=hit_tol: bool(_future_low_after(i) <= L[i] + tol)

    peak_idx = _enforce_min_pivot_distance(
        np.flatnonzero(is_peak), H, is_peak=True, min_dist=MIN_PIVOT_DISTANCE, eps=eps
    )
    low_idx = _enforce_min_pivot_distance(
        np.flatnonzero(is_low), L, is_peak=False, min_dist=MIN_PIVOT_DISTANCE, eps=eps
    )

    peaks = []
    for i in peak_idx:
        hit = peak_hit_fn(i)
        dist = _hit_distance(i, is_peak=True, H=H, L=L, n=n, hit_tolerance=hit_tol)
        peaks.append(
            {
                "index": int(i),
                "time": T_close[i],
                "open_time": T_open[i],
                "high": float(H[i]),
                "hit": hit,
                "hit_distance": dist,
            }
        )

    lows = []
    for i in low_idx:
        hit = low_hit_fn(i)
        dist = _hit_distance(i, is_peak=False, H=H, L=L, n=n, hit_tolerance=hit_tol)
        lows.append(
            {
                "index": int(i),
                "time": T_close[i],
                "open_time": T_open[i],
                "low": float(L[i]),
                "hit": hit,
                "hit_distance": dist,
            }
        )

    return peaks, lows
