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
from signals.open_signal_registry import get_open_signal_registry  # NEW: for registry sync

logger = logging.getLogger(__name__)

# Load env variables from src/data/.env (for local/dev runs)
ROOT_DIR = Path(__file__).resolve().parents[1]  # this is "src"
ENV_PATH = ROOT_DIR / "data" / ".env"
load_dotenv(dotenv_path=ENV_PATH, override=False)


# ----------------------------------------------------------------------
# Data model for order execution
# ----------------------------------------------------------------------
@dataclass
class OrderExecutionResult:
    """
    Rich result object returned by send_market_order().

    Used by:
      - pivot_corr_engine.py (to populate signals table order_* fields)
      - OpenSignalRegistry (for in-memory tracking)

    Fields:
      env:
        "demo" / "live" (matches signals.order_env check constraint)
      symbol:
        Broker instrument, e.g. "EUR_USD"
      side:
        "buy" or "sell"
      units:
        Trade size as passed to send_market_order (positive integer)
      tp_price:
        Requested take-profit price (if any)

      order_sent_time:
        UTC timestamp when we started the broker call

      broker_order_id:
        OANDA order id (if we can extract it)
      broker_trade_id:
        OANDA trade id (if we can extract it)

      actual_entry_time:
        When the trade was opened (best-effort from broker response)
      actual_entry_price:
        Execution price (best-effort)
      actual_tp_price:
        TP price actually registered on the trade (best-effort; falls
        back to tp_price if we cannot find a more precise value)

      status:
        One of: "none", "pending", "open", "closed", "cancelled",
        "rejected", "error"  (must match signals.order_status_check)

      exec_latency_ms:
        Milliseconds between send start and response receive.

      raw_response:
        Original JSON response (for debugging/auditing).
    """

    env: str
    symbol: str
    side: str
    units: int
    tp_price: Optional[Decimal]
    order_sent_time: datetime

    broker_order_id: Optional[str] = None
    broker_trade_id: Optional[str] = None

    actual_entry_time: Optional[datetime] = None
    actual_entry_price: Optional[Decimal] = None
    actual_tp_price: Optional[Decimal] = None

    status: str = "none"
    exec_latency_ms: Optional[int] = None

    raw_response: Optional[Dict[str, Any]] = None


# ----------------------------------------------------------------------
# Small helpers
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


# ----------------------------------------------------------------------
# Broker client factory
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
# High-level send-order helper
# ----------------------------------------------------------------------
def send_market_order(
    symbol: str,
    side: str,
    units: int,
    tp_price: Optional[Decimal] = None,
    client_order_id: Optional[str] = None,
) -> OrderExecutionResult:
    """
    High-level helper to send a simple market order with optional TP.

    Parameters
    ----------
    symbol : str
        Instrument symbol, e.g. "EUR_USD".
    side : str
        "buy" or "sell".
    units : int
        Trade size in units. This is the OANDA 'units' field.
        Positive for buy, negative for sell; if you pass positive
        the sign will be normalized based on 'side'.
    tp_price : Decimal, optional
        Absolute TP price, e.g. Decimal("1.09500").
    client_order_id : str, optional
        Optional id for tracking (will be set as clientExtensions.id in OANDA).

    Returns
    -------
    OrderExecutionResult
        Rich result object used by strategy and order-management layer.
    """
    client = get_broker_client()
    env_label = client.config.env  # "demo" / "live"

    side_norm = side.lower().strip()

    before = datetime.now(timezone.utc)
    print("Before send:", before.isoformat(timespec="milliseconds"))

    try:
        response = client.create_market_order(
            instrument=symbol,
            side=side_norm,
            units=units,
            tp_price=tp_price,
            client_order_id=client_order_id,
        )
        after = datetime.now(timezone.utc)
        print("After receive:", after.isoformat(timespec="milliseconds"))

        latency_ms = int((after - before).total_seconds() * 1000)

        logger.info("Order response from broker: %s", response)

        # Best-effort parsing of OANDA-style response
        trade_tx = None
        if isinstance(response, dict):
            trade_tx = (
                response.get("orderFillTransaction")
                or response.get("tradeOpened")
                or response.get("tradeReduced")
                or response.get("orderCreateTransaction")
            )

        broker_order_id: Optional[str] = None
        broker_trade_id: Optional[str] = None
        actual_entry_price: Optional[Decimal] = None
        actual_entry_time: Optional[datetime] = None
        actual_tp_price: Optional[Decimal] = None

        if isinstance(trade_tx, dict):
            # Try various fields OANDA uses
            broker_order_id = trade_tx.get("orderID") or trade_tx.get("id")
            broker_trade_id = trade_tx.get("tradeID") or trade_tx.get("id")

            price_str = trade_tx.get("price") or trade_tx.get("openPrice")
            if price_str is not None:
                try:
                    actual_entry_price = Decimal(str(price_str))
                except Exception:
                    actual_entry_price = None

            time_str = trade_tx.get("time") or trade_tx.get("openTime")
            if time_str:
                try:
                    actual_entry_time = datetime.fromisoformat(
                        time_str.replace("Z", "+00:00")
                    )
                except Exception:
                    actual_entry_time = after

            # Sometimes TP appears on related fields; if not, we fall back to tp_price
            tp_str = trade_tx.get("takeProfitOnFill", {}).get("price")
            if tp_str is not None:
                try:
                    actual_tp_price = Decimal(str(tp_str))
                except Exception:
                    actual_tp_price = tp_price
            else:
                actual_tp_price = tp_price
        else:
            # No detailed transaction object; still return basic info
            actual_tp_price = tp_price
            actual_entry_time = after

        # Decide initial status (will be refined by sync_broker_orders)
        if broker_trade_id:
            status = "open"
        elif broker_order_id:
            status = "pending"
        else:
            status = "error"

        return OrderExecutionResult(
            env=env_label,
            symbol=symbol,
            side=side_norm,
            units=units,
            tp_price=tp_price,
            order_sent_time=before,
            broker_order_id=str(broker_order_id) if broker_order_id else None,
            broker_trade_id=str(broker_trade_id) if broker_trade_id else None,
            actual_entry_time=actual_entry_time,
            actual_entry_price=actual_entry_price,
            actual_tp_price=actual_tp_price,
            status=status,
            exec_latency_ms=latency_ms,
            raw_response=response,
        )

    except Exception as e:
        after = datetime.now(timezone.utc)
        latency_ms = int((after - before).total_seconds() * 1000)

        logger.exception("Exception while sending market order: %s", e)
        try:
            notify_telegram(
                f"⛔️ Order send failed\n"
                f"Symbol: {symbol}\nSide: {side_norm.upper()}\nUnits: {units}\n"
                f"Error: {e}",
                ChatType.ALERT,
            )
        except Exception:
            # Do not let Telegram errors propagate
            pass

        # Return an error result instead of raising, so caller
        # (pivot_corr_engine) can still log/update DB safely.
        return OrderExecutionResult(
            env=env_label,
            symbol=symbol,
            side=side_norm,
            units=units,
            tp_price=tp_price,
            order_sent_time=before,
            broker_order_id=None,
            broker_trade_id=None,
            actual_entry_time=None,
            actual_entry_price=None,
            actual_tp_price=tp_price,
            status="error",
            exec_latency_ms=latency_ms,
            raw_response={"error": str(e)},
        )


# ----------------------------------------------------------------------
# DB-sync for broker orders (open & closed)
# ----------------------------------------------------------------------
def sync_broker_orders(conn: psycopg.Connection) -> None:
    """
    Synchronize all signals that have been sent to the broker with the
    actual trade state in OANDA.

    Behaviour:
      - Reads all signals with order_sent = true AND broker_trade_id not null
        AND order_env = current env AND order_status != 'closed'.
      - Fetches open trades once, to know which tradeIDs are still open.
      - For each signal:
          * If trade is OPEN:
                - ensure actual_entry_price/time are filled
                - compute slippage_pips if possible
                - set order_status='open'
          * If trade is not in openTrades:
                - fetch trade detail
                - if state='CLOSED':
                    - fill actual_exit_time, actual_exit_price
                    - compute profit_pips, profit_ccy
                    - compute slippage_pips if still null
                    - set order_status='closed'
                    - send Telegram notification
                    - remove from OpenSignalRegistry
                - if state other -> set order_status accordingly
    """
    client = get_broker_client()
    env = client.config.env  # 'demo' or 'live'
    open_sig_registry = get_open_signal_registry()  # NEW: sync registry

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

    for (
        event_time,
        symbol,
        position_type,
        target_price,
        position_price,
        order_units,
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

        # Case A: trade still appears in openTrades
        if trade_id in open_by_id:
            t = open_by_id[trade_id]

            # Fill entry info from open trade if missing
            if actual_entry_price is None:
                price_str = t.get("price")
                if price_str is not None:
                    actual_entry_price = Decimal(str(price_str))

            if actual_entry_time is None:
                open_time_str = t.get("openTime")
                if open_time_str:
                    try:
                        actual_entry_time = datetime.fromisoformat(
                            open_time_str.replace("Z", "+00:00")
                        )
                    except Exception:
                        actual_entry_time = None

            # Compute slippage if possible
            slippage_pips: Optional[Decimal] = None
            if actual_entry_price is not None and position_price is not None:
                planned = Decimal(str(position_price))
                actual = Decimal(str(actual_entry_price))
                # For slippage, sign "actual - planned" from perspective of execution
                if side == "buy":
                    slippage_pips = _pips_diff(symbol_str, actual, planned)
                else:
                    slippage_pips = _pips_diff(symbol_str, planned, actual)

            # Prepare UPDATE for the OPEN trade
            updates.append(
                (
                    "open",                     # order_status
                    actual_entry_time,
                    actual_entry_price,
                    actual_tp_price,
                    slippage_pips,
                    None,                       # profit_pips
                    None,                       # profit_ccy
                    event_time,
                    symbol_str,
                    position_type,
                    target_price,
                )
            )
            continue

        # Case B: trade is not open anymore -> query trade detail
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

        # Fallbacks for fields
        close_time_str = trade_obj.get("closeTime") or trade_obj.get("time")
        exit_price_str = (
            trade_obj.get("price")
            or trade_obj.get("averageClosePrice")
            or trade_obj.get("closePrice")
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
            actual_exit_price = Decimal(str(exit_price_str))

        # If we still don't know entry price/time, try to pick from trade object
        if actual_entry_price is None:
            open_price_str = trade_obj.get("price") or trade_obj.get("openPrice")
            if open_price_str is not None:
                actual_entry_price = Decimal(str(open_price_str))

        if actual_entry_time is None:
            open_time_str = trade_obj.get("openTime")
            if open_time_str:
                try:
                    actual_entry_time = datetime.fromisoformat(
                        open_time_str.replace("Z", "+00:00")
                    )
                except Exception:
                    actual_entry_time = None

        # Compute slippage if possible
        slippage_pips: Optional[Decimal] = None
        if actual_entry_price is not None and position_price is not None:
            planned = Decimal(str(position_price))
            actual = Decimal(str(actual_entry_price))
            if side == "buy":
                slippage_pips = _pips_diff(symbol_str, actual, planned)
            else:
                slippage_pips = _pips_diff(symbol_str, planned, actual)

        # Compute realized profit if available
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

        # Decide final order_status
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

        # Telegram notification only if closed (normal close with price)
        if final_status == "closed" and actual_exit_price is not None:
            try:
                msg_lines = [
                    "✅ BROKER CLOSE",
                    f"Symbol:        {symbol_str}",
                    f"Side:          {side.upper()}",
                    f"Units:         {order_units}",
                    "",
                    f"Entry price:   {actual_entry_price}",
                    f"Exit price:    {actual_exit_price}",
                ]

                if profit_pips is not None:
                    msg_lines.append(f"Pips:          {profit_pips:.1f}")
                if profit_ccy is not None:
                    msg_lines.append(f"Profit:        {profit_ccy}")

                msg_lines.append("")
                msg_lines.append(f"Event time:    {event_time}")
                if actual_exit_time is not None:
                    msg_lines.append(f"Close time:    {actual_exit_time}")

                notify_telegram("\n".join(msg_lines), ChatType.INFO)
            except Exception as e:
                logger.warning("[OrderSync] Telegram notify failed: %s", e)

        # Also persist exit info into DB via separate statement
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

        # NEW: also remove from OpenSignalRegistry when broker says it's closed
        if final_status == "closed":
            try:
                open_sig_registry.remove_by_broker(
                    symbol=symbol_str,
                    side=side,
                    event_time=event_time,
                )
            except Exception as e:
                logger.warning(
                    "[OrderSync] Failed to remove signal from OpenSignalRegistry: %s",
                    e,
                )

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
    test_symbol = "EUR_USD"
    test_side = "buy"
    test_units = 1000
    test_tp_price: Optional[Decimal] = None

    confirm = input(
        f"\nSend test market order on {test_symbol} side={test_side} "
        f"units={test_units}? (y/N): "
    ).strip().lower()

    if confirm == "y":
        resp = send_market_order(
            symbol=test_symbol,
            side=test_side,
            units=test_units,
            tp_price=test_tp_price,
            client_order_id="quantflow-test-order",
        )
        print("\nOrder placed. Result object:")
        print(resp)
    else:
        print("Skipped sending test order.")
