# file: src/orders/order_executor.py
# English-only comments

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Optional, List, Tuple
import logging

from dotenv import load_dotenv
import psycopg

from .broker_oanda import BrokerClient, create_client_from_env
from telegram_notifier import notify_telegram, ChatType

import math

import public_module

from signals import open_signal_registry

def truncate(value: float, decimals: int) -> float:
    factor = 10 ** decimals
    return math.trunc(value * factor) / factor

logger = logging.getLogger(__name__)

# Load env variables from src/data/.env (for local/dev runs)
ROOT_DIR = Path(__file__).resolve().parents[1]  # this is "src"
ENV_PATH = ROOT_DIR / "data" / ".env"
load_dotenv(dotenv_path=ENV_PATH, override=False)


# ----------------------------------------------------------------------
# Data structures
# ----------------------------------------------------------------------


@dataclass
class OrderExecutionResult:
    """
    Summary of an order sent to the broker at SEND-TIME.

    This is created immediately after create_market_order and used to:
      - populate signals table with basic order info
      - seed OpenSignalRegistry with env, ids, etc.
    More detailed fields (profit, final status) are filled by sync_broker_orders.
    """

    env: str                          # "demo" / "live"
    order_sent_time: datetime         # when we sent the order to the broker

    broker_order_id: Optional[str]    # OANDA order ID
    broker_trade_id: Optional[str]    # OANDA trade ID (position opened/managed)
    units: int                        # units we requested (signed by side)

    # Entry information as known right after send (may be None)
    actual_entry_time: Optional[datetime]
    actual_entry_price: Optional[Decimal]
    actual_tp_price: Optional[Decimal]

    # Initial status at send-time ("pending", "open", etc.)
    status: str

    # Network/processing latency between our send and broker response
    exec_latency_ms: Optional[int]

    # Raw full broker response (optional for debugging)
    raw_response: Optional[Dict[str, Any]] = None


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


def _pip_size(symbol: str) -> Decimal:
    s = symbol.upper()
    if "JPY" in s or "DXY" in s:
        return Decimal("0.01")
    return Decimal("0.0001")


def _pips_diff(symbol: str, p1: Decimal, p2: Decimal) -> Decimal:
    """
    p1 - p2 expressed in pips.
    """
    size = _pip_size(symbol)
    return (p1 - p2) / size


def _is_terminal_status(status: str) -> bool:
    """
    Any status that means the trade is NOT open anymore.
    Keep it simple and robust:
      - not terminal: open, pending
      - terminal: everything else (closed/cancelled/rejected/error/unknown/...)
    """
    s = (status or "").strip().lower()
    return s not in ("open", "pending")


def _remove_terminal_from_open_registry(*, symbol: str, side: str, event_time: datetime, final_status: str) -> None:
    """
    Best-effort: remove a terminal (non-open) signal from OpenSignalRegistry.
    This is intentionally isolated so broker sync stays safe.
    """
    if not _is_terminal_status(final_status):
        return


    try:
        open_sig_registry = open_signal_registry.get_open_signal_registry()
        open_sig_registry.remove_by_broker(symbol=symbol, side=side, event_time=event_time)
    except Exception as e:
        logger.warning("[OrderSync] Failed to remove from OpenSignalRegistry: %s", e)



# ----------------------------------------------------------------------
# Broker client
# ----------------------------------------------------------------------


def get_broker_client() -> BrokerClient:
    """
    Factory for a BrokerClient instance, using environment variables.
    """
    client = create_client_from_env()
    logger.info(
        "Initialized BrokerClient for env=%s, account_id=%s",
        client.config.env,
        client.config.account_id,
    )
    return client

# ----------------------------------------------------------------------
# High-level ORDER SEND
# ----------------------------------------------------------------------

def update_account_summary():
    client = create_client_from_env()
    client.update_account_summary()

def send_market_order(
    symbol: str,
    side: str,
    units: int,
    tp_price: Optional[Decimal] = None,
    sl_price: Optional[Decimal] = None,
    client_order_id: Optional[str] = None,
) -> OrderExecutionResult:
    """
    High-level helper to send a simple market order with optional TP.

    Returns an OrderExecutionResult object summarizing:
      - env, order_sent_time
      - broker_order_id, broker_trade_id
      - units, entry time/price, TP price
      - initial status, exec latency
    """
    client = get_broker_client()

    logger.info(
        "Sending market order: symbol=%s side=%s units=%s tp_price=%s sl_price=%s env=%s",
        symbol,
        side,
        units,
        tp_price,
        sl_price,
        client.config.env,
    )

    before = datetime.now(timezone.utc)
    response = client.create_market_order(
        instrument=symbol,
        side=side,
        units=units,
        tp_price=tp_price,
        sl_price=sl_price,
        client_order_id=client_order_id,
    )
    after = datetime.now(timezone.utc)

    # Compute latency in ms
    exec_latency_ms: Optional[int] = int((after - before).total_seconds() * 1000)

    # ------------------------------------------------------------------
    # Parse broker_order_id and broker_trade_id from OANDA response
    # ------------------------------------------------------------------
    oft = response.get("orderFillTransaction", {}) or {}
    octx = response.get("orderCreateTransaction", {}) or {}

    # Order ID: prefer explicit orderID, fallback to createTransaction id
    broker_order_id: Optional[str] = (
        oft.get("orderID")
        or octx.get("id")
        or response.get("lastTransactionID")
    )
    if broker_order_id is not None:
        broker_order_id = str(broker_order_id)

    # Trade ID: several possible locations depending on account settings
    trade_id: Optional[str] = None

    trade_opened = oft.get("tradeOpened") or {}
    if trade_opened:
        trade_id = trade_opened.get("tradeID") or trade_opened.get("id")

    if trade_id is None:
        trades_opened = oft.get("tradesOpened") or []
        if trades_opened:
            t0 = trades_opened[0] or {}
            trade_id = t0.get("tradeID") or t0.get("id")

    # Last fallback: if broker returns direct "tradeID" at top level
    if trade_id is None:
        trade_id = oft.get("tradeID")

    if trade_id is not None:
        trade_id = str(trade_id)

    # ------------------------------------------------------------------
    # Entry time & price from fill transaction (if available)
    # ------------------------------------------------------------------
    actual_entry_time: Optional[datetime] = None
    actual_entry_price: Optional[Decimal] = None

    price_str = oft.get("price")
    time_str = oft.get("time")

    if price_str is not None:
        try:
            actual_entry_price = Decimal(str(price_str))
        except Exception:
            actual_entry_price = None

    if time_str:
        try:
            actual_entry_time = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
        except Exception:
            actual_entry_time = None

    # TP price: we can use the explicit arg as our "intended TP";
    # broker may manage TP as a separate order, but we store the target here.
    actual_tp_price = tp_price

    # Status at send-time: we mark as "pending"; later sync_broker_orders
    # will update based on the actual trade state.
    initial_status = "pending"

    return OrderExecutionResult(
        env=client.config.env,
        order_sent_time=before,
        broker_order_id=broker_order_id,
        broker_trade_id=trade_id,
        units=units,
        actual_entry_time=actual_entry_time,
        actual_entry_price=actual_entry_price,
        actual_tp_price=actual_tp_price,
        status=initial_status,
        exec_latency_ms=exec_latency_ms,
        raw_response=response,
    )


# ----------------------------------------------------------------------
# BROKER SYNC (open & closed states)
# ----------------------------------------------------------------------


def sync_broker_orders(conn: psycopg.Connection) -> None:


    """
    Synchronize all broker-managed trades with the signals table.

    Behaviour:
      - Load all signals with order_sent=true and broker_trade_id not null,
        whose order_status is not 'closed'.
      - Hit OANDA:
          * get_open_trades() once
          * get_trade(trade_id) for non-open trades
      - Update:
          * order_status (open/closed/cancelled/...)
          * actual_entry_time/price
          * actual_exit_time/price
          * slippage_pips
          * profit_pips, profit_ccy
      - Send one "BROKER CLOSE" telegram when a trade transitions to closed.
    """
    client = get_broker_client()
    
    # calling this function to have balance, margine available updated in public module
    client.update_account_summary()
    logger.info(f"From order sync - Balance: {public_module.balance}")
    logger.info(f"From order sync - Available margin: {public_module.margin_available}")

    env = client.config.env  # 'demo' or 'live'

    logger.info("[OrderSync] Starting broker sync for env=%s", env)

    # 1) Load candidate signals from DB
    sql_select = """
        SELECT
            event_time,
            signal_symbol,
            position_type,
            target_price,
            position_price,
            order_units,
            broker_order_id,
            broker_trade_id,
            actual_entry_time,
            actual_entry_price,
            actual_tp_price,
            order_status
        FROM signals
        WHERE order_env = %s
          AND order_sent = TRUE
          AND broker_trade_id IS NOT NULL
          AND order_status <> 'closed'
    """

    with conn.cursor() as cur:
        cur.execute(sql_select, (env,))
        rows = cur.fetchall()

    if not rows:
        logger.info("[OrderSync] No broker-managed signals to sync.")
        return

    # 2) Fetch all open trades once
    open_data = client.get_open_trades()
    open_trades: List[Dict[str, Any]] = open_data.get("trades", []) or []
    open_by_id: Dict[str, Dict[str, Any]] = {t.get("id"): t for t in open_trades}

    logger.info("[OrderSync] Loaded %d open trades from broker.", len(open_by_id))

    updates: List[Tuple] = []

    # ---------------------------------------------------------
    # Process each signal row
    # ---------------------------------------------------------
    for (
        event_time,
        symbol,
        position_type,
        target_price,
        position_price,
        order_units,
        broker_order_id,
        broker_trade_id,
        actual_entry_time,
        actual_entry_price,
        actual_tp_price,
        order_status,
    ) in rows:

        trade_id = str(broker_trade_id) if broker_trade_id is not None else None
        if not trade_id:
            continue

        symbol_str = str(symbol)
        side = str(position_type).lower()

        # ---------- Case A: trade still appears in openTrades ----------
        if trade_id in open_by_id:
            t = open_by_id[trade_id]

            if actual_entry_price is None:
                price_str = t.get("price")
                if price_str is not None:
                    try:
                        actual_entry_price = Decimal(str(price_str))
                    except Exception:
                        actual_entry_price = None

            if actual_entry_time is None:
                open_time_str = t.get("openTime")
                if open_time_str:
                    try:
                        actual_entry_time = datetime.fromisoformat(
                            open_time_str.replace("Z", "+00:00")
                        )
                    except Exception:
                        actual_entry_time = None

            # Slippage
            slippage_pips: Optional[Decimal] = None
            if actual_entry_price is not None and position_price is not None:
                planned = Decimal(str(position_price))
                actual = Decimal(str(actual_entry_price))
                if side == "buy":
                    slippage_pips = _pips_diff(symbol_str, actual, planned)
                else:
                    slippage_pips = _pips_diff(symbol_str, planned, actual)

            updates.append(
                (
                    "open",
                    actual_entry_time,
                    actual_entry_price,
                    actual_tp_price,
                    slippage_pips,
                    None,
                    None,
                    event_time,
                    symbol_str,
                    position_type,
                    target_price,
                )
            )
            continue

        # ---------- Case B: trade is not open anymore -> query detail ----------
        try:
            trade_detail = client.get_trade(trade_id)
        except Exception as e:
            logger.warning(
                "[OrderSync] Failed to fetch trade %s from broker: %s",
                trade_id,
                e,
            )
            continue

        trade_obj = trade_detail.get("trade") or trade_detail.get("orderFillTransaction") or {}
        state = str(trade_obj.get("state", "")).upper()

        # Exit time & price
        close_time_str = trade_obj.get("closeTime") or trade_obj.get("time")
        exit_price_str = (
            trade_obj.get("averageClosePrice")
            or trade_obj.get("closePrice")
            or trade_obj.get("price")
        )
        realized_pl_str = trade_obj.get("realizedPL")

        actual_exit_time: Optional[datetime] = None
        if close_time_str:
            try:
                actual_exit_time = datetime.fromisoformat(
                    close_time_str.replace("Z", "+00:00")
                )
            except Exception:
                actual_exit_time = None

        actual_exit_price: Optional[Decimal] = None
        if exit_price_str is not None:
            try:
                actual_exit_price = Decimal(str(exit_price_str))
            except Exception:
                actual_exit_price = None

        # Ensure entry info is filled if still missing
        if actual_entry_price is None:
            open_price_str = trade_obj.get("price") or trade_obj.get("openPrice")
            if open_price_str is not None:
                try:
                    actual_entry_price = Decimal(str(open_price_str))
                except Exception:
                    actual_entry_price = None

        if actual_entry_time is None:
            open_time_str = trade_obj.get("openTime")
            if open_time_str:
                try:
                    actual_entry_time = datetime.fromisoformat(
                        open_time_str.replace("Z", "+00:00")
                    )
                except Exception:
                    actual_entry_time = None

        # Slippage
        slippage_pips: Optional[Decimal] = None
        if actual_entry_price is not None and position_price is not None:
            planned = Decimal(str(position_price))
            actual = Decimal(str(actual_entry_price))
            if side == "buy":
                slippage_pips = _pips_diff(symbol_str, actual, planned)
            else:
                slippage_pips = _pips_diff(symbol_str, planned, actual)

        # Profit
        profit_ccy: Optional[Decimal] = None
        profit_pips: Optional[Decimal] = None

        if realized_pl_str is not None:
            try:
                profit_ccy = Decimal(str(realized_pl_str))
            except Exception:
                profit_ccy = None

        if actual_entry_price is not None and actual_exit_price is not None:
            if side == "buy":
                profit_pips = _pips_diff(symbol_str, actual_exit_price, actual_entry_price)
            else:
                profit_pips = _pips_diff(symbol_str, actual_entry_price, actual_exit_price)

        # Final status
        if state == "CLOSED":
            final_status = "closed"
        elif state in ("CANCELLED", "CANCELLED_BY_CLIENT"):
            final_status = "cancelled"
        elif state in ("OPEN", "PENDING"):
            final_status = "open"
        elif state:
            final_status = state.lower()
        else:
            final_status = "closed"

        updates.append(
            (
                final_status,
                actual_entry_time,
                actual_entry_price,
                actual_tp_price,
                slippage_pips,
                profit_pips,
                profit_ccy,
                event_time,
                symbol_str,
                position_type,
                target_price,
            )
        )

        # ----------------- Terminal status -> remove from OpenSignalRegistry -----------------
        _remove_terminal_from_open_registry(
            symbol=symbol_str,
            side=side,
            event_time=event_time,
            final_status=final_status,
        )

        # ----------------- Broker close Telegram (single notif) -----------------
        if profit_ccy >= 0:
            BROKER_CLOSE = "🎯 BROKER CLOSE"
        else:
            BROKER_CLOSE = "♨️ BROKER CLOSE"

        if final_status == "closed" and actual_exit_price is not None:
            try:
                msg_lines: List[str] = [
                    f"{BROKER_CLOSE}",
                    f"Symbol:       {symbol_str}",
                    f"Side:           {side.upper()}",
                    f"Units:          {order_units}",
                    "",
                    f"Order ID:     {broker_order_id}",
                    f"Trade ID:     {trade_id}",
                    "",
                    f"Entry price:  {truncate(actual_entry_price,5)}",
                    f"Exit price:     {truncate(actual_exit_price,5)}",
                ]

                if profit_pips is not None:
                    msg_lines.append(f"Pips:            {truncate(profit_pips,2)}")
                if profit_ccy is not None:
                    msg_lines.append(f"Profit:       $ {truncate(profit_ccy,2)}")

                msg_lines.append("")
                msg_lines.append(f"Event time:    {event_time.strftime('%Y-%m-%d %H:%M')}")
                if actual_exit_time is not None:
                    msg_lines.append(f"Close time:     {actual_exit_time.strftime('%Y-%m-%d %H:%M')}")

                notify_telegram("\n".join(msg_lines), ChatType.INFO)
            except Exception as e:
                logger.warning("[OrderSync] Telegram notify failed: %s", e)

        # Also persist exit info into DB (for closed trades with price)
        if final_status == "closed" and actual_exit_price is not None:
            sql_exit = """
                UPDATE signals
                SET
                    actual_exit_time = %s,
                    actual_exit_price = %s
                WHERE event_time = %s
                  AND signal_symbol = %s
                  AND position_type = %s
                  AND target_price = %s
            """
            with conn.cursor() as cur:
                cur.execute(
                    sql_exit,
                    (
                        actual_exit_time,
                        actual_exit_price,
                        event_time,
                        symbol_str,
                        position_type,
                        target_price,
                    ),
                )
            conn.commit()

    # Apply bulk updates for status, entry, slippage, profit
    if updates:
        sql_update = """
            UPDATE signals
            SET
                order_status      = %s,
                actual_entry_time = %s,
                actual_entry_price= %s,
                actual_tp_price   = %s,
                slippage_pips     = %s,
                profit_pips       = %s,
                profit_ccy        = %s
            WHERE event_time     = %s
              AND signal_symbol  = %s
              AND position_type  = %s
              AND target_price   = %s
        """
        with conn.cursor() as cur:
            cur.executemany(sql_update, updates)
        conn.commit()

    logger.info("[OrderSync] Finished broker sync, updated %d rows.", len(updates))


# ----------------------------------------------------------------------
# Simple utilities for manual checks
# ----------------------------------------------------------------------


def print_account_summary() -> None:
    """
    Utility for quick manual checks: prints account summary.
    """
    client = get_broker_client()
    summary = client.get_account_summary()

    account = summary.get("account", {})
    account_id = account.get("id")
    balance = account.get("balance")
    nav = account.get("NAV")
    margin_available = account.get("marginAvailable")

    print("=== OANDA Account Summary ===")
    print(f"Account ID       : {account_id}")
    print(f"Balance          : {balance}")
    print(f"NAV              : {nav}")
    print(f"Margin available : {margin_available}")
    print("=============================")


def print_open_trades() -> None:
    """
    Utility for quick manual checks: prints open trades list.
    """
    client = get_broker_client()
    data = client.get_open_trades()

    trades = data.get("trades", []) or []
    print("=== OANDA Open Trades ===")
    if not trades:
        print("No open trades.")
    else:
        for t in trades:
            trade_id = t.get("id")
            instrument = t.get("instrument")
            current_units = t.get("currentUnits")
            price = t.get("price")
            realized_pl = t.get("realizedPL")
            unrealized_pl = t.get("unrealizedPL")
            print(
                f"Trade {trade_id}: {instrument} units={current_units} "
                f"price={price} realizedPL={realized_pl} unrealizedPL={unrealized_pl}"
            )
    print("===========================")


# ----------------------------------------------------------------------
# Logging setup for running this module directly
# ----------------------------------------------------------------------


def _configure_basic_logging() -> None:
    """
    Configure a simple console logger if the user runs this module directly.
    """
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        )


if __name__ == "__main__":
    # Manual test entrypoint.
    #
    # Run with:
    #   python -m src.orders.order_executor
    #
    # Make sure you have these env vars set (or in src/data/.env):
    #   OANDA_API_KEY
    #   OANDA_ACCOUNT_ID
    #   OANDA_ENV = "practice"  (or "live" if you know what you're doing)
    _configure_basic_logging()

    print_account_summary()
    print_open_trades()

    # Example test sync (requires valid DB connection string):
    # with psycopg.connect("postgresql://quantflow_user:pwd@host:5432/quantflow_db") as conn:
    #     sync_broker_orders(conn)

    # Example interactive test order (commented out by default):
    # test_symbol = "EUR_USD"
    # test_side = "buy"
    # test_units = 1000
    # test_tp_price: Optional[Decimal] = None
    #
    # confirm = input(
    #     f"\nSend test market order on {test_symbol} side={test_side} "
    #     f"units={test_units}? (y/N): "
    # ).strip().lower()
    #
    # if confirm == "y":
    #     resp = send_market_order(
    #         symbol=test_symbol,
    #         side=test_side,
    #         units=test_units,
    #         tp_price=test_tp_price,
    #         client_order_id="quantflow-test-order",
    #     )
    #     print("\nOrder placed. Execution summary:")
    #     print(resp)
    # else:
    #     print("Skipped sending test order.")
