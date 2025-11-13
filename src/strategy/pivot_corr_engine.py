# file: src/strategy/pivot_corr_engine.py
# English-only comments

from __future__ import annotations
from datetime import datetime
from decimal import Decimal
import threading
from typing import Any, Dict, List, Optional, Tuple

import psycopg  # psycopg3

# Access CandleBuffer to read prices
from buffers import buffer_initializer as buffers
from buffers.candle_buffer import Keys

# Shared registry provider for pivots
from pivots.pivot_registry_provider import get_pivot_registry

# Cross-trigger signal dedup
from signals.signal_registry import get_signal_registry


# --------------------------------------------------------------------
# Core State Classes
# --------------------------------------------------------------------

class SignalMemory:
    """
    Keeps a memory of processed signals WITHIN ONE run_decision_event call.

    This is only a local memory for the current event.
    For cross-event deduplication across multiple events, we use signal_registry.
    """
    def __init__(self) -> None:
        self._seen: set[str] = set()
        self._lock = threading.Lock()

    def remember(self, uid: str) -> bool:
        """
        Remember a unique signal key.
        Return True if it is NEW, False if seen before during this run.
        """
        with self._lock:
            if uid in self._seen:
                return False
            self._seen.add(uid)
            return True


# --------------------------------------------------------------------
# Public API
# --------------------------------------------------------------------

def run_decision_event(
    *,
    exchange: str,
    symbols: List[str],
    timeframe: str,
    event_time: datetime,                 # just-closed candle time (trigger)
    signal_memory: SignalMemory,
    groups: Optional[Dict[str, List[str]]] = None,  # SAME/OPP groups
    conn: Optional[psycopg.Connection] = None,      # DB connection
    max_lookback: int = 50,
    window: int = 3                                   # +/- minutes for pivot matching
) -> None:
    """
    Main correlation engine for pivot-based signals.

    Strategy (current version):

      • We have a set of symbols, partitioned into two groups (SAME and OPP).
        Example:
          SAME: [EUR/USD, GBP/USD, AUD/USD]
          OPP : [USD/CHF, DXY/DXY]

      • For each ref_symbol in `symbols` and ref_type in {"HIGH", "LOW"}:
          1) Read ref pivots of that type (peaks for HIGH, lows for LOW).
          2) For each ref pivot (ref_symbol, ref_type, ref_time, ref_price, ref_hit):
             - Find matching pivots in SAME group with SAME type, within ±window minutes.
             - Find matching pivots in OPP group with OPPOSITE type, within ±window minutes.
             - For each symbol in the universe, define "hit" as:
                 * ref_symbol: ref_hit
                 * peers: found=True and hit=True (their pivot already hit)
             - total_hits = number of symbols that are "hit" (True).

      • Decision rule:
          If total_hits >= 3 (across all 5), then this configuration is valid.
          On this configuration we will EMIT SIGNALS on those symbols that:

            - are NOT "DXY/DXY" (DXY can only confirm, never be a target),
            - have a matched pivot (found=True),
            - are NOT hit yet (hit=False) in this configuration.

          In other words: "3 or more hits -> trade on the remaining found-but-not-hit symbols".

      • Direction (BUY/SELL):
          - SAME/OPP groups only determine direction, not the validity of the signal.
          - We anchor direction on ref_type (ref pivot type):
              ref_type == "HIGH":
                 SAME group → SELL (we are selling into a group high)
                 OPP  group → BUY  (inverse correlation)
              ref_type == "LOW":
                 SAME group → BUY  (we are buying from a group low)
                 OPP  group → SELL (inverse correlation)

      • Entry price (position_price):
          - For now, we take the latest (most recent) candle close
            from CandleBuffer for that (exchange, symbol, timeframe).
          - This is Option 1: simple and consistent with a 1m strategy.
          - In the future, we can switch to using Tick price as entry.

      • Target:
          - We use the pivot price of the target symbol as "pivot_price":
               - For ref symbol: ref_price
               - For peers: peer pivot price
          - target_pips = (pivot_price - position_price) / pip_size(symbol)
          - target_price = position_price + target_pips * pip_size(symbol)

      • Dedup:
          - Within one run_decision_event:
              local key = (symbol, side, found_at_minute)
          - Across multiple events:
              use signal_registry.remember(symbol, side, found_at_minute)

      • Logging:
          - We always write rows into pivot_loop_log for SAME & OPP peers.
          - We write signals into signals table when they are generated.
    """
    pivot_reg = get_pivot_registry()
    sig_registry = get_signal_registry()

    # Resolve group A / B for SAME/OPP
    same_group, opposite_group = _resolve_two_groups(groups, symbols)

    # Pre-cache pivots per (symbol, "HIGH"/"LOW")
    cache: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}

    def pivots_for(sym: str, ptype: str) -> List[Dict[str, Any]]:
        """
        Fetch pivots from registry (with max_lookback) and cache them.
        Each pivot dict has shape:
          {
            "time": datetime,
            "open_time": datetime | None,
            "price": float,
            "hit": bool,
          }
        """
        key = (sym, ptype)
        if key not in cache:
            cache[key] = _collect_pivots_from_registry(
                exchange, sym, timeframe, ptype, max_lookback=max_lookback
            )
        return cache[key]

    # Batches for DB writes
    batch_rows_looplog: List[tuple] = []
    batch_rows_signals: List[tuple] = []

    # LOCAL (per-event) dedup:
    # key: (symbol, side, found_at_minute)
    emitted_local: set[Tuple[str, str, datetime]] = set()

    # ----------------------------------------------------------------
    # Main loop over each ref symbol and HIGH/LOW type
    # ----------------------------------------------------------------
    for ref_symbol in symbols:
        # Determine which are SAME vs OPP relative to ref_symbol
        if ref_symbol in same_group:
            peers_same = [s for s in same_group if s != ref_symbol]
            peers_opp = list(opposite_group)
        elif ref_symbol in opposite_group:
            peers_same = [s for s in opposite_group if s != ref_symbol]
            peers_opp = list(same_group)
        else:
            # Fallback: if symbol not in any group, treat all others as SAME
            peers_same = [s for s in symbols if s != ref_symbol]
            peers_opp = []

        for ref_type in ("HIGH", "LOW"):
            # 1) Ref pivots
            ref_pivots = pivots_for(ref_symbol, ref_type)

            # Newest -> oldest within max_lookback
            for _, rp in enumerate(ref_pivots, start=1):
                pivot_time = rp["time"]           # datetime of pivot (close time)
                ref_price = rp["price"]          # float pivot price
                ref_hit   = bool(rp["hit"])      # pivot already hit or not

                # SAME-type peers (peaks or lows depending on ref_type)
                rows_same, _ = _compare_ref_with_peers(
                    ref_symbol=ref_symbol,
                    ref_type=ref_type,
                    ref_time=pivot_time,
                    peers=peers_same,
                    peer_type=ref_type,
                    pivots_fetcher=pivots_for,
                    window=window,
                )

                # OPP-type peers (inverse type for correlation)
                opposite_type = "LOW" if ref_type == "HIGH" else "HIGH"
                rows_opp, _ = _compare_ref_with_peers(
                    ref_symbol=ref_symbol,
                    ref_type=ref_type,
                    ref_time=pivot_time,
                    peers=peers_opp,
                    peer_type=opposite_type,
                    pivots_fetcher=pivots_for,
                    window=window,
                )

                # ----------------------------------------------------
                # 1) Pivot loop log rows (for debugging/analysis)
                # ----------------------------------------------------
                for rec in rows_same:
                    found_at = rec["time"] if rec["found"] else None
                    delta_minute = _signed_minutes(found_at, pivot_time) if found_at else None
                    batch_rows_looplog.append((
                        event_time,
                        ref_symbol,
                        ref_type,
                        "SAME",
                        pivot_time,
                        ref_hit,
                        rec["symbol"],
                        bool(rec["found"]),
                        bool(rec["hit"]),
                        delta_minute,
                        found_at,
                    ))

                for rec in rows_opp:
                    found_at = rec["time"] if rec["found"] else None
                    delta_minute = _signed_minutes(found_at, pivot_time) if found_at else None
                    batch_rows_looplog.append((
                        event_time,
                        ref_symbol,
                        ref_type,
                        "OPP",
                        pivot_time,
                        ref_hit,
                        rec["symbol"],
                        bool(rec["found"]),
                        bool(rec["hit"]),
                        delta_minute,
                        found_at,
                    ))

                # ----------------------------------------------------
                # 2) Decision logic: count hits across all symbols
                # ----------------------------------------------------
                comp_by_symbol: Dict[str, Dict[str, Any]] = {
                    c["symbol"]: c for c in (rows_same + rows_opp)
                }

                # ref symbol uses its own hit flag
                hit_map: Dict[str, bool] = {ref_symbol: bool(ref_hit)}

                # peers: hit if found AND hit
                for c in comp_by_symbol.values():
                    hit_map[c["symbol"]] = bool(c["found"] and c["hit"])

                total_hits = sum(1 for v in hit_map.values() if v)
                confirm_syms_str = ", ".join([s for s, v in hit_map.items() if v])

                # DEBUG (optional – uncomment if you want more logs)
                # print(
                #     f"[DEBUG] ref={ref_symbol} {ref_type} @ {pivot_time} | "
                #     f"total_hits={total_hits} | hits=[{confirm_syms_str}]"
                # )

                # Rule: we require at least 3 hits across the basket.
                if total_hits < 3:
                    # Not enough confirmation; no signals for this pivot.
                    continue

                # If we don't have a DB connection, we can still print signals,
                # but we won't write them to the DB. So we do NOT early-return here.

                # ----------------------------------------------------
                # 3) For each symbol, decide if it becomes a signal target
                # ----------------------------------------------------
                for tgt in symbols:
                    # Never generate a signal ON DXY itself
                    if tgt == "DXY/DXY":
                        continue

                    # Determine pivot presence and hit status for target
                    tgt_found: bool
                    tgt_hit: bool
                    tgt_pivot_time: Optional[datetime]
                    tgt_pivot_price: Optional[Decimal]

                    if tgt == ref_symbol:
                        # Target is the ref itself
                        tgt_found = True
                        tgt_hit = ref_hit
                        tgt_pivot_time = pivot_time
                        tgt_pivot_price = Decimal(str(ref_price))
                    else:
                        c = comp_by_symbol.get(tgt)
                        if not c or not c.get("found"):
                            # We only signal on symbols that have a matched pivot
                            continue
                        tgt_found = True
                        tgt_hit = bool(c["hit"])
                        tgt_pivot_time = c["time"]
                        tgt_pivot_price = Decimal(str(c["price"])) if c["price"] is not None else None

                    # We only want symbols whose pivot is FOUND but NOT HIT yet
                    if (not tgt_found) or tgt_hit:
                        continue

                    if tgt_pivot_time is None or tgt_pivot_price is None:
                        # Safety check; normally pivot_time and price must exist
                        continue

                    # ------------------------------------------------
                    # 4) Entry price: latest close from CandleBuffer
                    # ------------------------------------------------
                    position_price = _get_latest_close_price(exchange, tgt, timeframe)
                    if position_price is None:
                        # This should be rare if CandleBuffer is loaded
                        # We skip this target, but other targets may still work
                        print(
                            f"[SKIP] target={tgt} | reason=no latest candle in CANDLE_BUFFER "
                            f"(exchange={exchange}, tf={timeframe})"
                        )
                        continue

                    # ------------------------------------------------
                    # 5) Determine side (BUY/SELL) based on groups and ref_type
                    # ------------------------------------------------
                    side = _decide_side(ref_type, tgt, same_group, opposite_group)

                    # Normalize pivot time to minute for dedup keys
                    found_at_minute = tgt_pivot_time.replace(second=0, microsecond=0)

                    local_key = (tgt.upper(), side, found_at_minute)

                    # 1) In-run dedup
                    if local_key in emitted_local:
                        continue
                    emitted_local.add(local_key)

                    # 2) Cross-trigger dedup (across events)
                    if not sig_registry.remember(tgt, side, found_at_minute):
                        # This signal was already emitted in a previous event
                        continue

                    # 3) Compute targets
                    target_pips = _pips(tgt, tgt_pivot_price, position_price)
                    target_price = _price_from_pips(tgt, position_price, target_pips)

                    signal_memory.remember(
                        f"{tgt}|{side}|{found_at_minute.isoformat()}"
                    )

                    # 4) Print human-readable signal info
                    print(
                        "[SIGNAL] "
                        f"event_time={event_time} | target={tgt} | side={side} | "
                        f"position_price={position_price} | target_price={target_price} | "
                        f"target_pips={target_pips} | "
                        f"confirm_symbols=[{confirm_syms_str}] | "
                        f"ref={ref_symbol} {ref_type} @ {pivot_time} | "
                        f"pivot_time(target)={tgt_pivot_time}"
                    )

                    # 5) Queue row for signals table INSERT (if conn is available)
                    if conn is not None:
                        batch_rows_signals.append((
                            event_time,             # event_time (trigger)
                            tgt,                    # signal_symbol
                            confirm_syms_str,       # confirm_symbols
                            side,                   # position_type
                            position_price,         # position_price
                            target_pips,            # target_pips
                            target_price,           # target_price
                            ref_symbol,             # ref_symbol (context)
                            ref_type,               # ref_type (context)
                            pivot_time,             # pivot_time (ref anchor)
                            found_at_minute,        # found_at (target pivot time)
                        ))

    # ----------------------------------------------------------------
    # 4) Batch DB writes
    # ----------------------------------------------------------------
    if conn:
        if batch_rows_looplog:
            _insert_pivot_loop_log(conn, batch_rows_looplog)
        if batch_rows_signals:
            # You can comment this print out if it is too noisy
            print(batch_rows_signals)
            _insert_signals(conn, batch_rows_signals)


# --------------------------------------------------------------------
# Peer comparison helpers
# --------------------------------------------------------------------

def _compare_ref_with_peers(
    *,
    ref_symbol: str,
    ref_type: str,
    ref_time: datetime,     # ref pivot time anchor
    peers: List[str],
    peer_type: str,
    pivots_fetcher,
    window: int,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """
    For a given ref pivot (ref_symbol, ref_type, ref_time),
    scan each peer symbol and attempt to find a pivot of peer_type
    whose time is within +/- `window` minutes of ref_time.

    Returns
    -------
    comparisons : List[Dict[str, Any]]
        One dict per peer, with keys:
          symbol, type, found, time, hit, delta_min, price
    stats : Dict[str, int]
        Simple stats, currently: {"hit": <count_of_hit_peers>}
    """
    comparisons: List[Dict[str, Any]] = []
    hit_counter = 0

    for peer in peers:
        piv_list = pivots_fetcher(peer, peer_type)
        matched = _find_pivot_in_window(piv_list, ref_time, window_minutes=window)

        if matched is None:
            comparisons.append({
                "symbol": peer,
                "type": peer_type,
                "found": False,
                "time": None,
                "hit": False,
                "delta_min": None,
                "price": None,
            })
            continue

        mp = matched
        m_time = mp["time"]
        m_hit = bool(mp["hit"])
        m_price = mp["price"]
        delta_m_abs = abs(_signed_minutes(m_time, ref_time))

        if m_hit:
            hit_counter += 1

        comparisons.append({
            "symbol": peer,
            "type": peer_type,
            "found": True,
            "time": m_time,
            "hit": m_hit,
            "delta_min": delta_m_abs,
            "price": m_price,
        })

    return comparisons, {"hit": hit_counter}


def _find_pivot_in_window(
    pivots: List[Dict[str, Any]],
    ref_time: datetime,
    window_minutes: int,
) -> Optional[Dict[str, Any]]:
    """
    Select the best pivot from a list that falls within +/- window_minutes
    of ref_time. "Best" = smallest absolute time difference.

    Assumes pivots are ordered newest-first.
    """
    best: Optional[Tuple[int, Dict[str, Any]]] = None  # (abs_delta_minutes, pivot_dict)

    for p in pivots:
        t = p["time"]
        if not isinstance(t, datetime):
            continue

        delta = abs(_signed_minutes(t, ref_time))
        if delta <= window_minutes:
            if best is None or delta < best[0]:
                best = (delta, p)
                if delta == 0:
                    # Exact match is the best we can get
                    break

    return best[1] if best else None


# --------------------------------------------------------------------
# Registry access & DB IO helpers
# --------------------------------------------------------------------

def _insert_pivot_loop_log(conn: psycopg.Connection, rows: List[tuple]) -> None:
    """
    Insert many rows into pivot_loop_log in one batch.

    Columns:
      (event_time, ref_symbol, ref_type, peer_type, pivot_time, ref_is_hit,
       symbol_compare, is_found, is_hit, delta_minute, found_at)
    """
    sql = """
        INSERT INTO pivot_loop_log (
            event_time,
            ref_symbol,
            ref_type,
            peer_type,
            pivot_time,
            ref_is_hit,
            symbol_compare,
            is_found,
            is_hit,
            delta_minute,
            found_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    conn.commit()


def _insert_signals(conn: psycopg.Connection, rows: List[tuple]) -> None:
    """
    Insert generated signals in one batch.

    Columns:
      (event_time, signal_symbol, confirm_symbols, position_type,
       position_price, target_pips, target_price,
       ref_symbol, ref_type, pivot_time, found_at)
    """
    sql = """
        INSERT INTO signals (
            event_time,
            signal_symbol,
            confirm_symbols,
            position_type,
            position_price,
            target_pips,
            target_price,
            ref_symbol,
            ref_type,
            pivot_time,
            found_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    conn.commit()


def _collect_pivots_from_registry(
    exchange: str,
    symbol: str,
    timeframe: str,
    pivot_type: str,
    max_lookback: int,
) -> List[Dict[str, Any]]:
    """
    Read pivots for (exchange, symbol, timeframe) from PivotBufferRegistry,
    map them to simple dicts, and return newest-first.

    The actual Pivot class is defined elsewhere and has:
      - time       (close time)
      - open_time  (open time)
      - price      (peak or low price)
      - is_hit     (whether this pivot has been hit by price)
    """
    reg = get_pivot_registry()
    pb = reg.get(exchange, symbol, timeframe)
    if pb is None:
        return []

    out: List[Dict[str, Any]] = []

    if pivot_type.upper() == "HIGH":
        it = pb.iter_peaks_newest_first()
    else:
        it = pb.iter_lows_newest_first()

    for p in it:
        # We rely on Pivot attributes defined in your project
        time_t = getattr(p, "time", None)
        open_time_t = getattr(p, "open_time", None)
        price_v = getattr(p, "price", None)
        hit_v = getattr(p, "is_hit", False)

        out.append({
            "time": time_t,
            "open_time": open_time_t,
            "price": price_v,
            "hit": bool(hit_v),
        })

        if len(out) >= max_lookback:
            break

    return out


# --------------------------------------------------------------------
# Candle & Price/Pip helpers
# --------------------------------------------------------------------

def _get_latest_close_price(
    exchange: str,
    symbol: str,
    timeframe: str,
) -> Optional[Decimal]:
    """
    Get the latest (most recent) close price for a symbol/timeframe
    from CandleBuffer.

    We do NOT search by time; we simply take the last candle
    currently stored in the buffer.

    This matches "Option 1" discussed:
      - For now, entry price = last candle close.
      - Later, we can change to Tick price or exact time-based lookup.
    """
    try:
        cb = getattr(buffers, "CANDLE_BUFFER", None)
        if cb is None:
            return None

        key = Keys(exchange, symbol, timeframe)
        candles = cb.last_n(key, 1)
        if not candles:
            return None

        c = candles[0]

        # Try common close field names (based on your DB and collector schema)
        for attr in ("Close", "close", "c", "ClosePrice", "C"):
            if isinstance(c, dict) and attr in c:
                return Decimal(str(c[attr]))

        # If you store close under a different key, add it here
        return None
    except Exception:
        return None


def _pip_size(symbol: str) -> Decimal:
    """
    Return pip size (in price units) for the given symbol.

    Simplified rule:
      - If symbol contains "JPY" or "DXY" => pip = 0.01
      - Otherwise => pip = 0.0001
    """
    s = symbol.upper()
    if "JPY" in s or "DXY" in s:
        return Decimal("0.01")
    return Decimal("0.0001")


def _pips(symbol: str, pivot_price: Decimal, close_price: Decimal) -> Decimal:
    """
    (pivot_price - close_price) expressed in pips (signed).

    Positive => pivot above price,
    Negative => pivot below price.
    """
    size = _pip_size(symbol)
    return (pivot_price - close_price) / size


def _price_from_pips(symbol: str, price: Decimal, pips: Decimal) -> Decimal:
    """
    Convert pips back to a price:
        price + pips * pip_size(symbol)
    """
    size = _pip_size(symbol)
    return price + (pips * size)


# --------------------------------------------------------------------
# Small utilities
# --------------------------------------------------------------------

def _signed_minutes(t1: Optional[datetime], t2: Optional[datetime]) -> int:
    """
    Signed difference in minutes:

      > 0  if t1 is after t2
      < 0  if t1 is before t2
      = 0  if same minute
    """
    if t1 is None or t2 is None:
        return 0
    return int((t1 - t2).total_seconds() // 60)


def _resolve_two_groups(
    groups: Optional[Dict[str, List[str]]],
    symbols: List[str],
) -> Tuple[List[str], List[str]]:
    """
    Resolve two symbol groups from `groups` dict.

    Example:
      {
          "GROUP_A": ["EUR/USD", "GBP/USD", "AUD/USD"],
          "GROUP_B": ["USD/CHF", "DXY/DXY"]
      }

    We only care about the lists, not keys.

    Returns:
      same_group, opposite_group
    """
    if not groups:
        return list(symbols), []

    vals = [list(v) for v in groups.values() if v]
    if not vals:
        return list(symbols), []

    if len(vals) == 1:
        same = vals[0]
        opp = [s for s in symbols if s not in same]
        return same, opp

    # If there are 2+ lists, just use the first two.
    return vals[0], vals[1]


def _decide_side(
    ref_type: str,
    symbol: str,
    same_group: List[str],
    opposite_group: List[str],
) -> str:
    """
    Decide BUY/SELL direction for a target symbol based on:

      - ref_type: "HIGH" or "LOW"
      - which group the target symbol belongs to (SAME or OPP)

    Current rule:

      If ref_type == "HIGH":
         SAME group → SELL
         OPP  group → BUY

      If ref_type == "LOW":
         SAME group → BUY
         OPP  group → SELL

    If a symbol is in neither list, we treat it as SAME-group for direction.
    """
    symbol_upper = symbol.upper()

    in_same = symbol in same_group
    in_opp  = symbol in opposite_group

    if ref_type.upper() == "HIGH":
        if in_opp:
            return "buy"
        # default and SAME:
        return "sell"
    else:  # ref_type == "LOW"
        if in_opp:
            return "sell"
        # default and SAME:
        return "buy"
