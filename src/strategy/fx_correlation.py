from typing import Dict, List, Tuple
from datetime import datetime
import public_module
from database.db_general import get_pg_conn
import logging


logger = logging.getLogger(__name__)


def _usd_orientation(symbol: str) -> str:
    """
    Return USD orientation for a symbol:
      - "USD_BASE"  if symbol starts with "USD/"  (e.g. "USD/JPY")
      - "USD_QUOTE" if symbol ends with "/USD"    (e.g. "EUR/USD")
      - "NONE"      otherwise
    """
    if symbol.startswith("USD/"):
        return "USD_BASE"
    if symbol.endswith("/USD"):
        return "USD_QUOTE"
    return "NONE"


def expected_corr_sign(sym1: str, sym2: str) -> int:
    """
    Return expected correlation sign between two USD-related pairs:

      +1  -> expected positive correlation
      -1  -> expected negative correlation
       0  -> unknown (non-USD pair or cannot determine)

    Rule:
      - both USD_BASE  => +1
      - both USD_QUOTE => +1
      - one base / one quote => -1
    """
    o1 = _usd_orientation(sym1)
    o2 = _usd_orientation(sym2)

    if o1 == "NONE" or o2 == "NONE":
        return 0

    if o1 == o2:
        return +1
    return -1

def is_signal_confirmed_by_correlation(
    signal_symbol: str,
    confirming_symbols: List[str],
    threshold: float = 60.0,
) -> bool:
    """
    Return True if at least 3 confirming symbols have strong correlation
    with the signal symbol in both 5m and 15m timeframes, taking into account
    expected sign (+ for same USD orientation, - for opposite).

    Rules:
      - For expected positive correlation:
            corr_5m  >=  threshold
        and corr_15m >=  threshold
      - For expected negative correlation:
            corr_5m  <= -threshold
        and corr_15m <= -threshold
      - If expected sign is 0 (unknown), the confirming symbol fails.
      - Missing data in either timeframe => confirming symbol fails.
      - We require at least 3 confirming symbols that pass.
    """
    cache_5m = public_module.correlation_cache.get("5m", {})
    cache_15m = public_module.correlation_cache.get("15m", {})

    good_count = 0

    for confirm_sym in confirming_symbols:
        if confirm_sym == signal_symbol:
            continue

        # Canonical unordered pair key
        a, b = sorted((signal_symbol, confirm_sym))
        corr_5m = cache_5m.get((a, b))
        corr_15m = cache_15m.get((a, b))

        logger.info(f"signal_symbol: {signal_symbol}, confirm_sym: {confirm_sym}, corr_5m: {corr_5m}, corr_15m: {corr_15m}")

        if corr_5m is None:
            continue

        sign = expected_corr_sign(signal_symbol, confirm_sym)
        if sign == 0:
            # Unknown USD orientation, treat as failing
            continue

        if sign > 0:
            ok = (corr_5m >= threshold)
        else:
            ok = (corr_5m <= -threshold)

        if ok:
            good_count += 1
            if good_count >= 3:
                return True

    return False


def refresh_correlation_cache(
    *,
    num_period: int = 50,
    timeframes: Tuple[str, ...] = ("5m", "15m"),
) -> None:
    """
    Load the latest Mataf correlations for the given timeframes and num_period
    from fx_correlation_mataf into public_module.correlation_cache.

    Only USD-related symbols are loaded (pairs where at least one side contains USD).
    The cache is fully overwritten each time this function runs.
    """
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            # 1) Find the latest as_of_time for these timeframes / num_period
            tf_placeholders = ", ".join(["%s"] * len(timeframes))
            cur.execute(
                f"""
                SELECT MAX(as_of_time)
                FROM fx_correlation_mataf
                WHERE num_period = %s
                  AND timeframe IN ({tf_placeholders})
                """,
                [num_period, *timeframes],
            )
            row = cur.fetchone()
            if not row or row[0] is None:
                logger.warning(
                    "[corr_cache] No as_of_time found for num_period=%s, timeframes=%s",
                    num_period,
                    timeframes,
                )
                return

            latest_as_of: datetime = row[0]

            # 2) Load all USD-related rows for that snapshot
            cur.execute(
                f"""
                SELECT base_symbol, ref_symbol, timeframe, corr_value
                FROM fx_correlation_mataf
                WHERE num_period = %s
                  AND as_of_time = %s
                  AND timeframe IN ({tf_placeholders})
                  AND (
                       base_symbol LIKE 'USD/%%'
                    OR base_symbol LIKE '%%/USD'
                    OR ref_symbol LIKE 'USD/%%'
                    OR ref_symbol LIKE '%%/USD'
                  )
                """,
                [num_period, latest_as_of, *timeframes],
            )
            rows = cur.fetchall()

    # 3) Build new cache: timeframe -> (sym_a, sym_b) -> corr_value
    new_cache: Dict[str, Dict[Tuple[str, str], float]] = {
        tf: {} for tf in timeframes
    }

    for base_sym, ref_sym, tf, corr_val in rows:
        # Store under canonical unordered key
        a, b = sorted((base_sym, ref_sym))
        try:
            v = float(corr_val)
        except Exception:
            continue
        new_cache.setdefault(tf, {})[(a, b)] = v

    # 4) Overwrite global cache in public_module
    public_module.correlation_cache = new_cache
    public_module.correlation_as_of_time = latest_as_of

    logger.info(
        "[corr_cache] Refreshed correlation cache: as_of_time=%s, "
        "num_period=%s, timeframes=%s, row_count=%d",
        latest_as_of,
        num_period,
        timeframes,
        len(rows),
    )
