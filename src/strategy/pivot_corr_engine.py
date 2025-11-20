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

from telegram_notifier import notify_telegram, ChatType

from buffers.tick_registry_provider import get_tick_registry

from signals.open_signal_registry import get_open_signal_registry, OpenSignal

from orders.order_executor import send_market_order, OrderExecutionResult

MAX_TICK_AGE_SEC = 10  # if last tick older than this (in seconds), fallback to candle close

# Minimum pips distance to send order to broker
MIN_PIPS_FOR_ORDER = Decimal("5")

# Simple default units for now; later this will be driven by risk model
DEFAULT_ORDER_UNITS = 100000

# NOTE: We DO NOT apply any min-pip filter to signal generation itself.
# All signals are emitted and logged. Final "send or not send" filter
# is applied before order sending (MIN_PIPS_FOR_ORDER).


# --------------------------------------------------------------------
# Core State Classes
# --------------------------------------------------------------------

class SignalMemory:
    """
    Keeps a memory of processed signals WITHIN ONE run_decision_event call.

    This is only a local memory for the current event.
    For cross-event deduplication across multiple events, we use SignalRegistry.
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
    event_time: datetime,                 # just-closed candle time (trigger, UTC naive)
    signal_memory: SignalMemory,
    groups: Optional[Dict[str, List[str]]] = None,  # SAME/OPP groups
    conn: Optional[psycopg.Connection] = None,      # DB connection
    max_lookback: int = 50,
    window: int = 3                                   # +/- minutes for pivot matching
) -> None:
    """
    Main correlation engine for pivot-based signals.

    Responsibilities:
      - reading pivots
      - computing correlation hits
      - deciding which symbols become "signal targets"
      - logging ALL such signals (no pip-size filters here)
      - deduping so we don't spam duplicates
      - optionally sending orders to broker when |pips| >= MIN_PIPS_FOR_ORDER

    Risk & capital sizing logic will later live in a dedicated
    Order Management layer. For now we use DEFAULT_ORDER_UNITS.
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
            "type": "HIGH" or "LOW",
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

    # New: order-related updates for existing signals rows
    batch_order_updates: List[tuple] = []

    # LOCAL (per-event) dedup:
    # key: (symbol, side, found_at_minute)
    emitted_local: set[Tuple[str, str, datetime]] = set()

    tick_registry = get_tick_registry()
    open_sig_registry = get_open_signal_registry()

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
                ref_price = rp["price"]           # float pivot price
                ref_hit = bool(rp["hit"])         # pivot already hit or not

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

                # Rule: we require at least 3 hits across the basket.
                if total_hits < 3:
                    continue

                # ----------------------------------------------------
                # 3) For each symbol, decide if it becomes a signal target
                # ----------------------------------------------------
                for tgt in symbols:
                    # Never generate a signal ON DXY itself
                    if tgt == "DXY/DXY":
                        continue

                    tgt_found: bool
                    tgt_hit: bool
                    tgt_pivot_time: Optional[datetime]
                    tgt_pivot_price: Optional[Decimal]
                    tgt_pivot_type: Optional[str]  # "HIGH" or "LOW"

                    if tgt == ref_symbol:
                        # Target is the ref itself
                        tgt_found = True
                        tgt_hit = ref_hit
                        tgt_pivot_time = pivot_time
                        tgt_pivot_price = Decimal(str(ref_price))
                        tgt_pivot_type = ref_type
                    else:
                        c = comp_by_symbol.get(tgt)
                        if not c or not c.get("found"):
                            # We only signal on symbols that have a matched pivot
                            continue
                        tgt_found = True
                        tgt_hit = bool(c["hit"])
                        tgt_pivot_time = c["time"]
                        tgt_pivot_price = (
                            Decimal(str(c["price"])) if c["price"] is not None else None
                        )
                        tgt_pivot_type = c.get("type")

                    # We only want symbols whose pivot is FOUND but NOT HIT yet
                    if (not tgt_found) or tgt_hit:
                        continue

                    if (
                        tgt_pivot_time is None
                        or tgt_pivot_price is None
                        or tgt_pivot_type is None
                    ):
                        # Safety check; normally time, price and type must exist
                        continue

                    # ------------------------------------------------
                    # 4) Determine side (BUY/SELL) based on TARGET pivot type
                    # ------------------------------------------------
                    side = "buy" if tgt_pivot_type == "HIGH" else "sell"

                    # ------------------------------------------------
                    # 5) Entry price: try Tick first, then candle-close fallback
                    # ------------------------------------------------
                    position_price, price_source = _get_entry_price_with_tick_fallback(
                        tick_registry=tick_registry,
                        exchange=exchange,
                        symbol=tgt,
                        timeframe=timeframe,
                        side=side,
                        event_time=event_time,
                    )
                    if position_price is None:
                        print(
                            f"[SKIP] target={tgt} | reason=no price available "
                            f"(exchange={exchange}, tf={timeframe}, side={side}, source={price_source})"
                        )
                        continue

                    # Normalize pivot time to minute for dedup keys (and DB found_at)
                    found_at_minute = tgt_pivot_time.replace(second=0, microsecond=0)

                    local_key = (tgt.upper(), side, found_at_minute)

                    # 1) In-run dedup
                    if local_key in emitted_local:
                        continue
                    emitted_local.add(local_key)

                    # 2) Cross-trigger dedup (across events)
                    if not sig_registry.remember(tgt, side, found_at_minute):
                        # This signal was already emitted in a previous event
                        print(
                            "[DEDUP] Skipping duplicate signal from registry: "
                            f"symbol={tgt}, side={side}, found_at={found_at_minute}"
                        )
                        continue

                    # 3) Compute targets (NO min-pip filter here for signal creation)
                    target_pips = _pips(tgt, tgt_pivot_price, position_price)
                    target_price = _price_from_pips(tgt, position_price, target_pips)

                    signal_memory.remember(
                        f"{tgt}|{side}|{found_at_minute.isoformat()}"
                    )

                    # Print human-readable signal info
                    print(
                        "[SIGNAL] "
                        f"event_time={event_time} | target={tgt} | side={side} | "
                        f"position_price={position_price} | target_price={target_price} | "
                        f"target_pips={target_pips} | "
                        f"confirm_symbols=[{confirm_syms_str}] | "
                        f"ref={ref_symbol} {ref_type} @ {pivot_time} | "
                        f"pivot_time(target)={tgt_pivot_time} | "
                        f"pivot_type(target)={tgt_pivot_type} | "
                        f"price_source={price_source}"
                    )

                    # Telegram notification (multi-line, nicely formatted)
                    try:
                        profit_est = (target_pips / Decimal("10000")) * Decimal("5000")
                        msg = (
                            "⚡ Pivot Correlation Signal\n"
                            f"Symbol:         {tgt}\n"
                            f"Side:           {side.upper()}\n"
                            f"Price source:   {price_source}\n\n"
                            f"Entry price:    {position_price}\n"
                            f"Target price:   {target_price}\n"
                            f"Distance:       {target_pips} pips\n"
                            f"Est. Profit:    ${profit_est}\n\n"
                            f"Ref pivot:      {ref_symbol}  ({ref_type})\n"
                            f"Ref pivot time: {pivot_time.strftime('%Y-%m-%d %H:%M')}\n\n"
                            f"Target pivot time: {tgt_pivot_time.strftime('%Y-%m-%d %H:%M')}\n"
                            f"Event time:        {event_time.strftime('%Y-%m-%d %H:%M')}\n\n"
                            f"Confirm symbols: {confirm_syms_str}"
                        )
                        notify_telegram(msg, ChatType.INFO)
                    except Exception as e:
                        print(f"[WARN] telegram notify failed: {e}")

                    # ------------------------------------------------
                    # 6) Decide whether to send order to broker
                    # ------------------------------------------------
                    send_to_broker = abs(target_pips) >= MIN_PIPS_FOR_ORDER
                    order_info: Optional[OrderExecutionResult] = None

                    if send_to_broker:
                        try:
                            instrument = _to_oanda_instrument(tgt)
                            client_order_id = (
                                f"qf-{event_time.strftime('%Y%m%d%H%M%S')}-"
                                f"{instrument.replace('_', '')}"
                            )
                            order_units = DEFAULT_ORDER_UNITS

                            order_info = send_market_order(
                                symbol=instrument,
                                side=side,
                                units=order_units,
                                tp_price=target_price,
                                client_order_id=client_order_id,
                            )
                        except Exception as e:
                            print(f"[ORDER] Failed to send order for {tgt}: {e}")
                            order_info = None

                    # ------------------------------------------------
                    # 7) Register OpenSignal (with optional order info)
                    # ------------------------------------------------
                    open_sig_registry.add_signal(
                        OpenSignal(
                            exchange=exchange,
                            symbol=tgt,
                            timeframe=timeframe,
                            side=side,
                            event_time=event_time,
                            target_price=target_price,
                            position_price=position_price,
                            created_at=event_time,
                            order_env=(order_info.env if order_info else None),
                            broker_order_id=(
                                order_info.broker_order_id if order_info else None
                            ),
                            broker_trade_id=(
                                order_info.broker_trade_id if order_info else None
                            ),
                            order_units=(
                                order_info.units if order_info else None
                            ),
                            actual_entry_time=(
                                order_info.actual_entry_time if order_info else None
                            ),
                            actual_entry_price=(
                                order_info.actual_entry_price if order_info else None
                            ),
                            actual_tp_price=(
                                order_info.actual_tp_price if order_info else None
                            ),
                            order_status=(
                                order_info.status if order_info else "none"
                            ),
                            exec_latency_ms=(
                                order_info.exec_latency_ms if order_info else None
                            ),
                        )
                    )

                    # ------------------------------------------------
                    # 8) Queue DB updates for order-related columns
                    # ------------------------------------------------
                    if order_info is not None:
                        batch_order_updates.append((
                            # SET columns
                            order_info.env,               # order_env
                            True,                         # order_sent
                            order_info.order_sent_time,   # order_sent_time
                            order_info.broker_order_id,   # broker_order_id
                            order_info.broker_trade_id,   # broker_trade_id
                            None,                         # allocation_block (later)
                            order_info.units,             # order_units
                            order_info.actual_entry_time, # actual_entry_time
                            order_info.actual_entry_price,# actual_entry_price
                            order_info.actual_tp_price,   # actual_tp_price
                            None,                         # actual_exit_time
                            None,                         # actual_exit_price
                            order_info.status,            # order_status
                            None,                         # slippage_pips
                            None,                         # profit_pips
                            None,                         # profit_ccy
                            order_info.exec_latency_ms,   # exec_latency_ms
                            # WHERE keys
                            tgt,                          # signal_symbol
                            side,                         # position_type
                            event_time,                   # event_time
                            found_at_minute,              # found_at
                        ))

                    # ------------------------------------------------
                    # 9) Queue row for signals table INSERT
                    # ------------------------------------------------
                    if conn is not None:
                        batch_rows_signals.append((
                            event_time,             # event_time (trigger)
                            tgt,                    # signal_symbol
                            confirm_syms_str,       # confirm_symbols
                            side,                   # position_type
                            price_source,           # price_source
                            position_price,         # position_price
                            target_pips,            # target_pips
                            target_price,           # target_price
                            ref_symbol,             # ref_symbol (context)
                            ref_type,               # ref_type (context)
                            pivot_time,             # pivot_time (ref anchor)
                            found_at_minute,        # found_at (target pivot time)
                        ))

    # ----------------------------------------------------------------
    # 10) Batch DB writes
    # ----------------------------------------------------------------
    if conn:
        if batch_rows_looplog:
            _insert_pivot_loop_log(conn, batch_rows_looplog)
        if batch_rows_signals:
            _insert_signals(conn, batch_rows_signals)
        if batch_order_updates:
            _update_signals_with_orders(conn, batch_order_updates)


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
    """
    sql = """
        INSERT INTO signals (
            event_time,
            signal_symbol,
            confirm_symbols,
            position_type,
            price_source,
            position_price,
            target_pips,
            target_price,
            ref_symbol,
            ref_type,
            pivot_time,
            found_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    conn.commit()


def _update_signals_with_orders(
    conn: psycopg.Connection,
    rows: List[tuple],
) -> None:
    """
    Batch update for order-related columns in signals table.
    """
    if not rows:
        return

    sql = """
        UPDATE signals
           SET order_env        = %s,
               order_sent       = %s,
               order_sent_time  = %s,
               broker_order_id  = %s,
               broker_trade_id  = %s,
               allocation_block = %s,
               order_units      = %s,
               actual_entry_time  = %s,
               actual_entry_price = %s,
               actual_tp_price    = %s,
               actual_exit_time   = %s,
               actual_exit_price  = %s,
               order_status       = %s,
               slippage_pips      = %s,
               profit_pips        = %s,
               profit_ccy         = %s,
               exec_latency_ms    = %s
         WHERE signal_symbol = %s
           AND position_type = %s
           AND event_time    = %s
           AND found_at      = %s
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
        time_t = getattr(p, "time", None)
        open_time_t = getattr(p, "open_time", None)
        price_v = getattr(p, "price", None)
        hit_v = getattr(p, "is_hit", False)

        out.append({
            "time": time_t,
            "open_time": open_time_t,
            "price": price_v,
            "hit": bool(hit_v),
            "type": pivot_type.upper(),
        })

        if len(out) >= max_lookback:
            break

    return out


# --------------------------------------------------------------------
# Candle, Tick & Price/Pip helpers
# --------------------------------------------------------------------

def _get_entry_price_with_tick_fallback(
    *,
    tick_registry,
    exchange: str,
    symbol: str,
    timeframe: str,
    side: str,
    event_time: datetime,
) -> Tuple[Optional[Decimal], str]:
    """
    Determine entry price for a signal:

      1) Try to use the latest Tick from TickRegistry:
           - If a tick exists and abs(age_sec) <= MAX_TICK_AGE_SEC:
               BUY  -> ASK
               SELL -> BID
      2) Otherwise, fallback to the latest candle close from CandleBuffer.

    Returns:
      (price, source) where source in {"tick", "candle_close", "none"}.
    """
    price_source = "none"
    try:
        tick = tick_registry.get_last_tick(exchange, symbol)
    except Exception:
        tick = None

    if tick is not None and isinstance(event_time, datetime):
        try:
            age_sec = abs((tick.time - event_time).total_seconds())
        except Exception:
            age_sec = None

        if age_sec is not None and age_sec <= MAX_TICK_AGE_SEC:
            side_upper = side.upper()
            if side_upper == "BUY":
                px = Decimal(str(tick.ask))
            else:  # SELL
                px = Decimal(str(tick.bid))
            price_source = "tick"
            return px, price_source

    # Fallback to latest candle close
    close_px = _get_latest_close_price(exchange, symbol, timeframe)
    if close_px is not None:
        price_source = "candle_close"
        return close_px, price_source

    return None, price_source


def _get_latest_close_price(
    exchange: str,
    symbol: str,
    timeframe: str,
) -> Optional[Decimal]:
    """
    Get the latest (most recent) close price for a symbol/timeframe
    from CandleBuffer.
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

        for attr in ("Close", "close", "c", "ClosePrice", "C"):
            if isinstance(c, dict) and attr in c:
                return Decimal(str(c[attr]))

        return None
    except Exception:
        return None


def _pip_size(symbol: str) -> Decimal:
    """
    Return pip size (in price units) for the given symbol.
    """
    s = symbol.upper()
    if "JPY" in s or "DXY" in s:
        return Decimal("0.01")
    return Decimal("0.0001")


def _pips(symbol: str, pivot_price: Decimal, price: Decimal) -> Decimal:
    """
    (pivot_price - price) expressed in pips (signed).
    """
    size = _pip_size(symbol)
    return (pivot_price - price) / size


def _price_from_pips(symbol: str, price: Decimal, pips: Decimal) -> Decimal:
    """
    Convert pips back to a price:
        price + pips * pip_size(symbol)
    """
    size = _pip_size(symbol)
    return price + (pips * size)


def _to_oanda_instrument(symbol: str) -> str:
    """
    Convert internal symbol like 'EUR/USD' to OANDA instrument 'EUR_USD'.
    """
    return symbol.replace("/", "_")


# --------------------------------------------------------------------
# Small utilities
# --------------------------------------------------------------------

def _signed_minutes(t1: Optional[datetime], t2: Optional[datetime]) -> int:
    """
    Signed difference in minutes.
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
    OLD DIRECTION LOGIC (kept for reference, currently unused).
    """
    symbol_upper = symbol.upper()

    in_same = symbol in same_group
    in_opp = symbol in opposite_group

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
