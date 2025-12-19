# file: src/signals/open_signal_registry.py
# English-only comments

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from threading import Lock
from typing import Dict, List, Optional, Any, Tuple

import psycopg

from telegram_notifier import notify_telegram, ChatType

from database.db_signals import fetch_open_signals_for_open_registry

import logging

logger = logging.getLogger(__name__)

def _pip_size(symbol: str) -> Decimal:
    # JPY pairs are typically 0.01 pip size in price terms, and you also special-case DXY
    return Decimal("0.01") if ("JPY" in symbol or "DXY" in symbol) else Decimal("0.0001")


def _to_decimal(x: Any) -> Decimal:
    # Safe conversion for floats/Decimals/strings
    if isinstance(x, Decimal):
        return x
    return Decimal(str(x))


def _pips_distance(price: Decimal, target: Decimal, pip: Decimal) -> Decimal:
    # Absolute distance in pips
    return (price - target).copy_abs() / pip


@dataclass
class OpenSignal:
    exchange: str
    symbol: str
    timeframe: str
    side: str  # "buy" or "sell"
    event_time: datetime
    target_price: Decimal
    position_price: Decimal
    created_at: datetime

    # --- New tracking fields (in-memory) ---
    # Distance metrics are in pips (absolute distance to target).
    nearest_pips_to_target: Optional[Decimal] = None
    farthest_pips_to_target: Optional[Decimal] = None

    # Last tick price used for this signal (BUY->bid, SELL->ask)
    last_tick_price: Optional[Decimal] = None

    # Marks whether metrics changed and should be flushed to DB
    dirty: bool = False

    # Order-related fields (optional at first)
    order_env: Optional[str] = None          # "demo" / "live"
    broker_order_id: Optional[str] = None
    broker_trade_id: Optional[str] = None
    order_units: Optional[int] = None
    actual_entry_time: Optional[datetime] = None
    actual_entry_price: Optional[Decimal] = None
    actual_tp_price: Optional[Decimal] = None
    order_status: str = "none"               # none/pending/open/closed/...
    exec_latency_ms: Optional[int] = None


class OpenSignalRegistry:
    """
    In-memory registry of open signals that we want to track with ticks.

    This is NOT about order execution logic itself. It only tracks:
      - when a tick reaches target_price for a signal
      - sends a Telegram notification
      - updates the DB row for that signal
      - removes the signal from memory

    Now it also stores optional order-related info for signals whose
    orders were actually sent to the broker, and can be pruned when
    broker closes the trade (sync_broker_orders).
    """

    def __init__(self) -> None:
        self._signals_by_symbol: Dict[str, List[OpenSignal]] = {}
        self._lock = Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_count(self) -> int:
        with self._lock:
            return sum(len(v) for v in self._signals_by_symbol.values())
        
    def add_signal(self, sig: OpenSignal) -> None:
        """Register a new open signal for tracking."""
        with self._lock:
            lst = self._signals_by_symbol.setdefault(sig.symbol, [])
            lst.append(sig)


    def bootstrap_from_db(self, conn: psycopg.Connection) -> int:
        """
        Load ALL open signals from DB into memory.

        NOTE:
        - This does not modify SignalRegistry and does not affect dedup logic.
        - This is only to populate OpenSignalRegistry so it can track ticks
          and update nearest/farthest pips.

        The DB SELECT is delegated to db_signals.py (as requested).
        """
        if fetch_open_signals_for_open_registry is None:
            raise RuntimeError(
                "fetch_open_signals_for_open_registry is not available. "
                "Implement and export it from database/db_signals.py"
            )

        rows = fetch_open_signals_for_open_registry(conn)  # type: ignore
        loaded = 0

        with self._lock:
            for row in rows:
                sig = OpenSignal(
                    exchange=str(row["exchange"]),
                    symbol=str(row["symbol"]),
                    timeframe=str(row["timeframe"]),
                    side=str(row["side"]).lower(),
                    event_time=row["event_time"],
                    target_price=_to_decimal(row["target_price"]),
                    position_price=_to_decimal(row["position_price"]),
                    created_at=row["created_at"],
                    # If your DB loader returns these, we accept them; otherwise remain None
                    nearest_pips_to_target=_to_decimal(row["nearest_pips_to_target"]) if row.get("nearest_pips_to_target") is not None else None,  # type: ignore
                    farthest_pips_to_target=_to_decimal(row["farthest_pips_to_target"]) if row.get("farthest_pips_to_target") is not None else None,  # type: ignore
                    last_tick_price=_to_decimal(row["last_tick_price"]) if row.get("last_tick_price") is not None else None,  # type: ignore
                    order_env=row.get("order_env"),
                    broker_order_id=row.get("broker_order_id"),
                    broker_trade_id=row.get("broker_trade_id"),
                    order_units=row.get("order_units"),
                    actual_entry_time=row.get("actual_entry_time"),
                    actual_entry_price=_to_decimal(row["actual_entry_price"]) if row.get("actual_entry_price") is not None else None,  # type: ignore
                    actual_tp_price=_to_decimal(row["actual_tp_price"]) if row.get("actual_tp_price") is not None else None,  # type: ignore
                    order_status=str(row.get("order_status") or "none"),
                    exec_latency_ms=row.get("exec_latency_ms"),
                )

                self._signals_by_symbol.setdefault(sig.symbol, []).append(sig)
                loaded += 1

        return loaded
    
    def attach_order_info(
        self,
        *,
        symbol: str,
        side: str,
        event_time: datetime,
        order_env: str,
        broker_order_id: Optional[str],
        broker_trade_id: Optional[str],
        order_units: Optional[int],
        actual_entry_time: Optional[datetime],
        actual_entry_price: Optional[Decimal],
        actual_tp_price: Optional[Decimal],
        order_status: str,
        exec_latency_ms: Optional[int],
    ) -> None:
        """
        Find the matching OpenSignal (by symbol, side, event_time) and
        attach order-related information to it.
        """
        with self._lock:
            signals = self._signals_by_symbol.get(symbol)
            if not signals:
                return

            for sig in signals:
                if (
                    sig.side.lower() == side.lower()
                    and sig.event_time == event_time
                ):
                    sig.order_env = order_env
                    sig.broker_order_id = broker_order_id
                    sig.broker_trade_id = broker_trade_id
                    sig.order_units = order_units
                    sig.actual_entry_time = actual_entry_time
                    sig.actual_entry_price = actual_entry_price
                    sig.actual_tp_price = actual_tp_price
                    sig.order_status = order_status
                    sig.exec_latency_ms = exec_latency_ms
                    break

    def remove_by_broker(
        self,
        *,
        symbol: str,
        side: str,
        event_time: datetime,
    ) -> None:
        """
        Remove a signal when broker confirms the trade is closed.

        Called from sync_broker_orders() using the same key triple
        (symbol, side, event_time) that we use for attach_order_info.
        """
        with self._lock:
            signals = self._signals_by_symbol.get(symbol)
            if not signals:
                return

            survivors: List[OpenSignal] = []
            for sig in signals:
                if (
                    sig.side.lower() == side.lower()
                    and sig.event_time == event_time
                ):
                    # Drop this one
                    continue
                survivors.append(sig)

            if survivors:
                self._signals_by_symbol[symbol] = survivors
            else:
                self._signals_by_symbol.pop(symbol, None)
            
            logger.warning(f"Dropped from open_signal_registry: symbol:{symbol}, side: {side}, event_time: {event_time}")

    def process_tick_for_symbol(
        self,
        *,
        exchange: str,
        symbol: str,
        bid: float,
        ask: float,
        now: datetime,
        conn: Optional[psycopg.Connection] = None,
    ) -> None:
        """
        Called on each tick for a given symbol.

        For each open signal on that symbol:
          - if BUY  -> check price >= target_price
          - if SELL -> check price <= target_price

        If a signal hits:
          - send Telegram notification
          - update DB row (hit_price, hit_time)
          - remove it from registry
        """
        with self._lock:
            signals = self._signals_by_symbol.get(symbol)
            if not signals:
                return

            survivors: List[OpenSignal] = []
            for sig in signals:
                # Basic sanity check: same exchange
                if sig.exchange != exchange:
                    survivors.append(sig)
                    continue

                # Decide which price to use for comparison.
                # For now we use:
                #   BUY  -> bid
                #   SELL -> ask
                if sig.side.lower() == "buy":
                    price_to_check = Decimal(str(bid))
                    hit = price_to_check >= sig.target_price
                else:
                    price_to_check = Decimal(str(ask))
                    hit = price_to_check <= sig.target_price

                # Save last tick price (side-based) and mark dirty
                sig.last_tick_price = price_to_check

                # Update distance metrics even if not hit
                self._update_distance_metrics(sig=sig, price_to_check=price_to_check)
                
                if not hit:
                    survivors.append(sig)
                    continue

                # Signal reached its target
                self._on_signal_hit(
                    sig=sig,
                    hit_price=price_to_check,
                    hit_time=now,
                    conn=conn,
                )

            # Update survivors list for this symbol
            if survivors:
                self._signals_by_symbol[symbol] = survivors
            else:
                # No more open signals on this symbol
                self._signals_by_symbol.pop(symbol, None)

    def flush_distance_metrics(self, conn: psycopg.Connection) -> int:
        """
        Persist nearest/farthest pips metrics + last_tick_price to DB for signals that changed.

        We intentionally DO NOT use the identity id column.
        We update using:
          symbol + side + event_time + target_price
        plus open constraints:
          order_sent=true AND actual_exit_time IS NULL

        Also updates updatetime = NOW() in PostgreSQL.
        """
        to_flush: List[Tuple[OpenSignal, Decimal, Decimal, Optional[Decimal]]] = []

        with self._lock:
            for _, signals in self._signals_by_symbol.items():
                for sig in signals:
                    if not sig.dirty:
                        continue
                    if sig.nearest_pips_to_target is None or sig.farthest_pips_to_target is None:
                        continue
                    to_flush.append((sig, sig.nearest_pips_to_target, sig.farthest_pips_to_target, sig.last_tick_price))
                    # Mark clean now; if DB fails, we'll mark dirty again on next ticks.
                    sig.dirty = False

        if not to_flush:
            logger.info(
                f"[open_signals] flush_distance_metrics: No updated Distance Metrics for open signals"
            )            
            return 0

        sql = """
            UPDATE public.signals
               SET nearest_pips_to_target = %s,
                   farthest_pips_to_target = %s,
                   last_tick_price = %s,
                   tick_update_time = NOW()
             WHERE signal_symbol = %s
               AND position_type = %s
               AND event_time    = %s
               AND target_price  = %s
               AND order_sent = true
               AND actual_exit_time IS NULL
        """

        updated = 0
        try:
            with conn.cursor() as cur:
                for sig, nearest, farthest, last_tick_price in to_flush:
                    cur.execute(
                        sql,
                        (
                            nearest,
                            farthest,
                            last_tick_price,
                            sig.symbol,
                            sig.side,
                            sig.event_time,
                            sig.target_price,
                        ),
                    )
                    updated += 1
            conn.commit()

            logger.info(
                f"[open_signals] flush_distance_metrics: for {updated} open signals"
            )
                    
        except Exception as e:
            print(f"[WARN] flush_distance_metrics failed: {e}")

        return updated


    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------


    def _update_distance_metrics(self, *, sig: OpenSignal, price_to_check: Decimal) -> None:
        """
        Update nearest/farthest absolute distance to target in pips.
        Also marks signal dirty if values change.
        """
        pip = _pip_size(sig.symbol)
        dist_pips = _pips_distance(price=price_to_check, target=sig.target_price, pip=pip)

        # First tick seen for this signal
        if sig.nearest_pips_to_target is None or sig.farthest_pips_to_target is None:
            sig.nearest_pips_to_target = dist_pips
            sig.farthest_pips_to_target = dist_pips
            sig.dirty = True
            return

        changed = False
        if dist_pips < sig.nearest_pips_to_target:
            sig.nearest_pips_to_target = dist_pips
            changed = True
        if dist_pips > sig.farthest_pips_to_target:
            sig.farthest_pips_to_target = dist_pips
            changed = True

        # last_tick_price changed every tick; we only flush when metrics changed OR you can force flush always.
        # If you want last_tick_price always flushed, set dirty=True unconditionally on each tick.
        if changed:
            sig.dirty = True


    def _on_signal_hit(
        self,
        *,
        sig: OpenSignal,
        hit_price: Decimal,
        hit_time: datetime,
        conn: Optional[psycopg.Connection],
    ) -> None:
        """Handle a signal that has reached its target."""

        # 1) Telegram notification
        try:
            # Calculate actual realized pips at hit
            pip_size = Decimal("0.01") if ("JPY" in sig.symbol or "DXY" in sig.symbol) else Decimal("0.0001")
            pips_realized = (hit_price - sig.position_price) / pip_size

            # For BUY, positive pips are profit; for SELL reverse sign
            if sig.side.lower() == "sell":
                pips_realized = -pips_realized

            # Dollar profit with assumed $5000 position size
            profit_usd = pips_realized / Decimal("10000") * Decimal("5000")

            '''
            msg = (
                "🎯 TARGET HIT\n"
                f"Symbol:         {sig.symbol}\n"
                f"Side:           {sig.side.upper()}\n\n"
                f"Entry price:    {sig.position_price}\n"
                f"Target price:   {sig.target_price}\n"
                f"Hit price:      {hit_price}\n\n"
                f"Pips gained:    {pips_realized:.1f}\n"
                f"Profit:         ${profit_usd:.2f}\n\n"
                f"Event time:     {sig.event_time.strftime('%Y-%m-%d %H:%M')}\n"
                f"Hit time:       {hit_time.strftime('%Y-%m-%d %H:%M')}\n"
            )
            
            notify_telegram(msg, ChatType.INFO)
            
            '''
            
        except Exception as e:
            print(f"[WARN] telegram notify (target hit) failed: {e}")

        # 2) DB update (if connection is provided)
        if conn is None:
            return

        try:
            sql = """
                UPDATE signals
                   SET hit_price = %s,
                       hit_time  = %s
                 WHERE signal_symbol = %s
                   AND position_type = %s
                   AND event_time    = %s
                   AND target_price  = %s
            """
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (
                        hit_price,
                        hit_time,
                        sig.symbol,
                        sig.side,
                        sig.event_time,
                        sig.target_price,
                    ),
                )
            conn.commit()
        except Exception as e:
            print(f"[WARN] failed to update signals(hit_price, hit_time): {e}")


# ----------------------------------------------------------------------
# Global provider
# ----------------------------------------------------------------------

_GLOBAL_OPEN_SIGNAL_REGISTRY: Optional[OpenSignalRegistry] = None


def get_open_signal_registry() -> OpenSignalRegistry:
    global _GLOBAL_OPEN_SIGNAL_REGISTRY
    if _GLOBAL_OPEN_SIGNAL_REGISTRY is None:
        _GLOBAL_OPEN_SIGNAL_REGISTRY = OpenSignalRegistry()
    return _GLOBAL_OPEN_SIGNAL_REGISTRY
