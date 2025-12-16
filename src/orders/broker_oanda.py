# file: src/orders/broker_oanda.py
# English-only comments

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, Optional
import os
import public_module

import requests
import logging

logger = logging.getLogger(__name__)


@dataclass
class BrokerConfig:
    """
    Simple configuration holder for OANDA connection.
    """
    base_url: str          # e.g. "https://api-fxpractice.oanda.com/v3"
    api_key: str           # OANDA REST API token
    account_id: str        # OANDA account ID
    env: str = "demo"      # "demo" / "live" (for your own reference/logging)


class BrokerClient:
    """
    Minimal OANDA-like client.

    Responsibilities:
    - Hold connection config
    - Build headers
    - Send GET/POST requests
    - Provide simple high-level methods:
        * create_market_order(...)
        * get_open_trades()
        * get_account_summary()
        * get_trade(trade_id)
    """

    def __init__(self, config: BrokerConfig) -> None:
        self.config = config

    # ------------------------------------------------------------------
    # Internal HTTP helpers
    # ------------------------------------------------------------------
    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.config.api_key}",
            "Content-Type": "application/json",
        }

    def _post(self, path: str, json_body: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.config.base_url}{path}"
        resp = requests.post(url, headers=self._headers(), json=json_body, timeout=10)
        resp.raise_for_status()
        return resp.json()

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.config.base_url}{path}"
        resp = requests.get(url, headers=self._headers(), params=params, timeout=10)
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------
    # Formatting helpers
    # ------------------------------------------------------------------
    def _fmt_price(self, instrument: str, price: Decimal) -> str:
        """
        OANDA expects instrument-specific precision.
        Common: JPY pairs = 3 decimals, others = 5 decimals.
        """
        inst = instrument.upper()
        if "JPY" in inst:
            return f"{price:.3f}"
        return f"{price:.5f}"

    # ------------------------------------------------------------------
    # Public trading / account API
    # ------------------------------------------------------------------
    def create_market_order(
        self,
        instrument: str,
        side: str,
        units: int,
        tp_price: Optional[Decimal] = None,
        sl_price: Optional[Decimal] = None,
        client_order_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create a MARKET order with optional Take Profit and Stop Loss.

        Parameters
        ----------
        instrument : str
            OANDA instrument, e.g. "EUR_USD".
        side : str
            "buy" or "sell".
        units : int
            Positive for buy, negative for sell. If you pass positive, this
            method will set the sign based on 'side'.
        tp_price : Decimal, optional
            Absolute TP price (e.g. Decimal("1.09500")).
        sl_price : Decimal, optional
            Absolute SL price (e.g. Decimal("1.09000")).
        client_order_id : str, optional
            Optional client extension id for tracking in your system.

        Returns
        -------
        Dict[str, Any]
            Parsed JSON response from OANDA.
        """

        side_normalized = side.lower().strip()
        signed_units = units

        if side_normalized == "buy":
            signed_units = abs(units)
        elif side_normalized == "sell":
            signed_units = -abs(units)
        else:
            raise ValueError(f"Unsupported side: {side}")

        order_payload: Dict[str, Any] = {
            "order": {
                "instrument": instrument,
                "units": str(signed_units),
                "type": "MARKET",
                "timeInForce": "FOK",        # Fill-or-Kill for market style
                "positionFill": "DEFAULT",   # Let OANDA handle netting/hedging
            }
        }

        # Optional clientExtensions for your own tracking
        if client_order_id:
            order_payload["order"]["clientExtensions"] = {"id": client_order_id}

        # Optional Take Profit on fill
        if tp_price is not None:
            order_payload["order"]["takeProfitOnFill"] = {
                "price": self._fmt_price(instrument, tp_price)
            }

        # Optional Stop Loss on fill
        if sl_price is not None:
            order_payload["order"]["stopLossOnFill"] = {
                "price": self._fmt_price(instrument, sl_price)
            }

        path = f"/accounts/{self.config.account_id}/orders"
        return self._post(path, order_payload)

    def get_open_trades(self) -> Dict[str, Any]:
        """
        Fetch open trades from OANDA.
        """
        path = f"/accounts/{self.config.account_id}/openTrades"
        return self._get(path)

    def get_account_summary(self) -> Dict[str, Any]:
        """
        Fetch account summary (balance, margin, NAV, etc.).
        """
        path = f"/accounts/{self.config.account_id}/summary"
        return self._get(path)

    # -------- NEW: convenience helper for balance + available margin --------
    def update_account_summary(self):
        """
        Convenience helper to return current balance and available margin
        as Decimals.

        Updates public_module.balance and public_module.margin_available.
        """
        logger.info("Getting account summary from broker...")

        data = self.get_account_summary()
        account = data.get("account", {})

        try:
            balance = Decimal(account["balance"])
            margin_available = Decimal(account["marginAvailable"])
            public_module.balance = balance
            public_module.margin_available = margin_available
        except KeyError as exc:
            raise RuntimeError(f"Missing expected key in OANDA account summary: {exc}") from exc

    def get_trade(self, trade_id: str) -> Dict[str, Any]:
        """
        Fetch a single trade by ID.

        This works for both OPEN and CLOSED trades.
        """
        path = f"/accounts/{self.config.account_id}/trades/{trade_id}"
        return self._get(path)


# ----------------------------------------------------------------------
# Helper: build client from environment variables
# ----------------------------------------------------------------------
def create_client_from_env() -> BrokerClient:
    """
    Build a BrokerClient using environment variables.

    Required env vars:
        OANDA_API_KEY
        OANDA_ACCOUNT_ID

    Optional env vars:
        OANDA_ENV      -> "practice" / "demo" / "live"
        OANDA_BASE_URL -> if provided, overrides the default URL resolved from OANDA_ENV
    """
    api_key = os.environ.get("OANDA_API_KEY")
    account_id = os.environ.get("OANDA_ACCOUNT_ID")

    if not api_key:
        raise RuntimeError("Missing OANDA_API_KEY in environment.")
    if not account_id:
        raise RuntimeError("Missing OANDA_ACCOUNT_ID in environment.")

    env = os.environ.get("OANDA_ENV", "practice").lower().strip()
    base_url_env = os.environ.get("OANDA_BASE_URL")

    if base_url_env:
        base_url = base_url_env
    else:
        if env in ("practice", "demo", "paper"):
            base_url = "https://api-fxpractice.oanda.com/v3"
            env_label = "demo"
        elif env in ("live", "real"):
            base_url = "https://api-fxtrade.oanda.com/v3"
            env_label = "live"
        else:
            base_url = "https://api-fxpractice.oanda.com/v3"
            env_label = "demo"
        env = env_label

    config = BrokerConfig(
        base_url=base_url,
        api_key=api_key,
        account_id=account_id,
        env=env,
    )
    return BrokerClient(config)
