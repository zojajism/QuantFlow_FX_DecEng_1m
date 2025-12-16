# file: src/indicators/atr_15m.py
# English-only comments

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from buffers.candle_buffer import Keys
from buffers import buffer_initializer as buffers

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AtrConfig:
    period: int = 14
    src_timeframe: str = "1m"
    dst_timeframe: str = "15m"
    indicator_name: str = "ATR14"
    bucket_minutes: int = 15
    min_capacity_padding: int = 0
    # How many extra 1m candles to request beyond the exact ATR window.
    # This helps when close_time is inside the current 15m bucket (e.g. 03:17),
    # and we still want ATR as-of 03:15.
    extra_tail_1m: int = 15


# In-memory gate: compute once per (exchange, symbol, aligned_end)
_LAST_ASOF: Dict[Tuple[str, str], datetime] = {}


# --------- Normalization helpers ---------

def _norm_1m_candle_for_atr(c: Dict[str, Any]) -> Tuple[float, float, float, datetime]:
    """
    Extract (high, low, close, close_time) from a 1m candle dict.
    Supports multiple common key variants.
    """
    high = c.get("high", c.get("High", c.get("H")))
    low = c.get("low", c.get("Low", c.get("L")))
    close = c.get("close", c.get("Close", c.get("C")))
    ct = c.get("close_time", c.get("CloseTime", c.get("t_close", c.get("time"))))

    if high is None or low is None or close is None or ct is None:
        raise KeyError("1m candle missing required fields for ATR: high/low/close/close_time")

    if not isinstance(ct, datetime):
        raise TypeError("close_time must be a datetime (already parsed in main loop).")

    return float(high), float(low), float(close), ct


def _to_utc_aware(dt: datetime) -> datetime:
    """
    Force a datetime into UTC tz-aware representation.
    - If tz-aware -> convert to UTC
    - If tz-naive -> assume it's already UTC and set tzinfo=UTC
    """
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _floor_to_bucket_end(dt_utc: datetime, bucket_minutes: int) -> datetime:
    """
    dt_utc must be UTC tz-aware.
    Return the most recent aligned bucket end time <= dt_utc:
    xx:00, xx:15, xx:30, xx:45
    """
    minute = (dt_utc.minute // bucket_minutes) * bucket_minutes
    return dt_utc.replace(minute=minute, second=0, microsecond=0)


# --------- Aggregation (1m -> 15m synthetic bars) ---------

def _build_15m_bar_from_15_chunk(one_min_15: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Build a synthetic 15m bar from exactly 15 closed 1m candles (ASC order).
    """
    if len(one_min_15) != 15:
        raise ValueError("Expected exactly 15 x 1m candles for one 15m bar.")

    highs: List[float] = []
    lows: List[float] = []
    closes: List[float] = []

    for c in one_min_15:
        h, l, cl, _ct = _norm_1m_candle_for_atr(c)
        highs.append(h)
        lows.append(l)
        closes.append(cl)

    return {
        "High": max(highs),
        "Low": min(lows),
        "Close": closes[-1],
    }


def _build_last_n_15m_bars_from_1m_aligned(
    one_min: List[Dict[str, Any]],
    n_bars: int,
    aligned_end_utc: datetime,
) -> Optional[List[Dict[str, Any]]]:
    """
    Build the last n_bars synthetic 15m bars aligned to aligned_end_utc:
    - 15m candle ending at aligned_end_utc uses 1m candles with close_time < aligned_end_utc.
    """
    if n_bars <= 0:
        return None

    end_idx: Optional[int] = None
    for i in range(len(one_min) - 1, -1, -1):
        try:
            _, _, _, ct = _norm_1m_candle_for_atr(one_min[i])
        except Exception:
            continue

        if _to_utc_aware(ct) < aligned_end_utc:
            end_idx = i
            break

    if end_idx is None:
        return None

    need_1m = 15 * n_bars
    start_idx = end_idx - need_1m + 1
    if start_idx < 0:
        return None

    slice_ = one_min[start_idx : end_idx + 1]  # ASC

    bars: List[Dict[str, Any]] = []
    for j in range(0, len(slice_), 15):
        chunk = slice_[j : j + 15]
        if len(chunk) != 15:
            return None
        bars.append(_build_15m_bar_from_15_chunk(chunk))

    return bars


# --------- ATR computation ---------

def _true_range(curr_high: float, curr_low: float, prev_close: float) -> float:
    return max(
        curr_high - curr_low,
        abs(curr_high - prev_close),
        abs(curr_low - prev_close),
    )


def _compute_atr_sma(bars_15m: List[Dict[str, Any]], period: int) -> Optional[float]:
    """
    Compute ATR as SMA(TR, period) over synthetic 15m bars.
    Need at least (period + 1) bars to compute period TRs with prev_close.
    """
    if len(bars_15m) < (period + 1):
        return None

    trs: List[float] = []
    start = len(bars_15m) - period
    for i in range(start, len(bars_15m)):
        curr = bars_15m[i]
        prev = bars_15m[i - 1]
        trs.append(_true_range(float(curr["High"]), float(curr["Low"]), float(prev["Close"])))

    return sum(trs) / float(period)


# --------- IndicatorBuffer helpers ---------

def _get_latest_payload(exchange: str, symbol: str, cfg: AtrConfig) -> Optional[Dict[str, Any]]:
    key_15m = Keys(exchange=exchange, symbol=symbol, timeframe=cfg.dst_timeframe)
    dq = buffers.INDICATOR_BUFFER.map.get((key_15m, cfg.indicator_name))
    if not dq:
        return None
    return dq[-1]


def get_latest_atr_15m(exchange: str, symbol: str, *, cfg: AtrConfig = AtrConfig()) -> float | None:
    last = _get_latest_payload(exchange, symbol, cfg)
    if not last:
        return None
    atr = last.get("atr")
    return float(atr) if atr is not None else None


def get_latest_atr_asof_15m(exchange: str, symbol: str, *, cfg: AtrConfig = AtrConfig()) -> Optional[datetime]:
    last = _get_latest_payload(exchange, symbol, cfg)
    if not last:
        return None
    asof = last.get("asof")
    if isinstance(asof, datetime):
        return _to_utc_aware(asof)
    return None


# --------- Public API ---------

def ATR_Update(exchange: str, symbol: str, close_time: datetime, *, cfg: AtrConfig = AtrConfig()) -> bool:
    """
    Call this on every received 1m candle close for (exchange, symbol).

    Optimized behavior:
    - Always compute ATR "as of" most recent aligned 15m boundary <= close_time
    - Compute only once per boundary (in-memory gate)
    - Read smaller slice from CandleBuffer (exact window + small tail)
    - Minimal INFO logging only on real updates
    """
    if not isinstance(close_time, datetime):
        raise TypeError("close_time must be a datetime.")

    ct_utc = _to_utc_aware(close_time)
    aligned_end = _floor_to_bucket_end(ct_utc, cfg.bucket_minutes)  # UTC aware

    # ---- #1: in-memory gate (fastest) ----
    gate_key = (exchange, symbol)
    prev_asof = _LAST_ASOF.get(gate_key)
    if prev_asof is not None and prev_asof == aligned_end:
        return False

    # Optional debug trace
    logger.debug(
        f"[ATR DEBUG] {exchange} {symbol} ct={ct_utc.isoformat()} aligned_end={aligned_end.isoformat()}"
    )

    key_1m = Keys(exchange=exchange, symbol=symbol, timeframe=cfg.src_timeframe)

    bars_needed = (cfg.period + 1)                      # 15 bars for period=14
    need_for_atr = bars_needed * 15                     # 225 1m candles
    need_1m = need_for_atr + int(cfg.extra_tail_1m)     # 240 by default
    need_1m += int(cfg.min_capacity_padding)

    buffers.CANDLE_BUFFER.ensure_capacity(key_1m, need_1m)

    # ---- #2: smaller read ----
    one_min = buffers.CANDLE_BUFFER.last_n(key_1m, need_1m)

    if len(one_min) < need_for_atr:
        logger.debug(
            f"[ATR DEBUG] {exchange} {symbol} warmup: have={len(one_min)} need_for_atr={need_for_atr}"
        )
        return False

    bars_15m = _build_last_n_15m_bars_from_1m_aligned(
        one_min,
        n_bars=bars_needed,
        aligned_end_utc=aligned_end,
    )

    if not bars_15m:
        logger.debug(
            f"[ATR DEBUG] {exchange} {symbol} alignment_failed aligned_end={aligned_end.isoformat()}"
        )
        return False

    atr = _compute_atr_sma(bars_15m, period=cfg.period)
    if atr is None:
        logger.debug(f"[ATR DEBUG] {exchange} {symbol} atr_none")
        return False

    key_15m = Keys(exchange=exchange, symbol=symbol, timeframe=cfg.dst_timeframe)
    payload = {
        "indicator": cfg.indicator_name,
        "period": cfg.period,
        "atr": float(atr),
        "asof": aligned_end,  # UTC tz-aware
        "source": "agg_from_1m_aligned",
    }

    buffers.INDICATOR_BUFFER.append(key_15m, cfg.indicator_name, payload)

    # update gate after storing
    _LAST_ASOF[gate_key] = aligned_end

    # ---- #3: minimal INFO log only on update ----
    logger.info(
        f"[ATR] {exchange} {symbol} {cfg.indicator_name}={float(atr):.5f} asof={aligned_end.isoformat()}"
    )
    return True
