# file: src/signals/open_signal_registry.py
# English-only comments

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from threading import Lock
from typing import Dict, List, Optional

import psycopg

from telegram_notifier import notify_telegram, ChatType


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

    def add_signal(self, sig: OpenSignal) -> None:
        """Register a new open signal for tracking."""
        with self._lock:
            lst = self._signals_by_symbol.setdefault(sig.symbol, [])
            lst.append(sig)

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

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

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
