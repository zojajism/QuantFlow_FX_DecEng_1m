# file: src/orders/order_executor.py
# English-only comments

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Optional
import logging

# Adjust this import depending on your package layout.
# If 'src' is your root package, you might use:
#   from orders.broker_oanda import BrokerClient, create_client_from_env
# If you use relative imports inside a package, this works:
from .broker_oanda import BrokerClient, create_client_from_env
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Try the Docker volume location first
# Load env variables from src/data/.env
ROOT_DIR = Path(__file__).resolve().parents[1]  # this is "src"
ENV_PATH = ROOT_DIR / "data" / ".env"
load_dotenv(dotenv_path=ENV_PATH)

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


def send_market_order(
    symbol: str,
    side: str,
    units: int,
    tp_price: Optional[Decimal] = None,
    client_order_id: Optional[str] = None,
) -> Dict[str, Any]:
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
    Dict[str, Any]
        Parsed JSON response from the broker.
    """
    client = get_broker_client()

    logger.info(
        "Sending market order: symbol=%s side=%s units=%s tp_price=%s env=%s",
        symbol,
        side,
        units,
        tp_price,
        client.config.env,
    )


    print("Before send:", datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))

    response = client.create_market_order(
        instrument=symbol,
        side=side,
        units=units,
        tp_price=tp_price,
        client_order_id=client_order_id,
    )
    
    print("After receive:", datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))

    logger.info("Order response from broker: %s", response)
    return response


def print_account_summary() -> None:
    """
    Utility for quick manual checks: prints account summary.
    """
    client = get_broker_client()
    summary = client.get_account_summary()

    # You can adjust these keys based on actual OANDA response structure.
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

    trades = data.get("trades", [])
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
    # This block is only for manual testing.
    # You can run it with:
    #   python -m src.orders.order_executor
    #
    # Make sure you have these env vars set:
    #   OANDA_API_KEY
    #   OANDA_ACCOUNT_ID
    #   OANDA_ENV = "practice"  (or "live" if you know what you're doing)
    _configure_basic_logging()

    # Example manual usage:
    # 1) Print account summary
    print_account_summary()

    # 2) Print open trades
    print_open_trades()

    # 3) Send a small test order on demo (EDIT THESE VALUES BEFORE RUNNING)
    #
    # NOTE: For safety, units is small by default. Increase only if you are
    # sure about the environment and account you are using.
    #
    test_symbol = "EUR_USD"
    test_side = "buy"
    test_units = 1000  # small test size; adjust as needed
    test_tp_price: Optional[Decimal] = None
    # Example TP: Uncomment and adjust if you want to test TP on-fill
    # test_tp_price = Decimal("1.09500")

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
        print("\nOrder placed. Raw response:")
        print(resp)
    else:
        print("Skipped sending test order.")
