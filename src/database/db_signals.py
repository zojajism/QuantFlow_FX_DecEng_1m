# file: src/database/db_signals.py
# English-only comments

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List
import os

import psycopg
from dotenv import load_dotenv


# ----------------------------------------------------------------------
# Load .env from src/data/.env
# ----------------------------------------------------------------------
ROOT_DIR = Path(__file__).resolve().parents[1]  # -> src
ENV_PATH = ROOT_DIR / "data" / ".env"
load_dotenv(dotenv_path=ENV_PATH)


@dataclass
class PgConfig:
    host: str
    port: int
    user: str
    password: str
    dbname: str


def _load_pg_config() -> PgConfig:
    return PgConfig(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        user=os.environ.get("POSTGRES_USER", "postgres"),
        password=os.environ.get("POSTGRES_PASSWORD", ""),
        dbname=os.environ.get("POSTGRES_DB", "postgres"),
    )


def _get_conn() -> psycopg.Connection:
    cfg = _load_pg_config()
    return psycopg.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        password=cfg.password,
        dbname=cfg.dbname,
    )


def fetch_recent_signals(hours_back: int = 5) -> List[Dict[str, Any]]:
    """
    Fetch signals whose found_at is within the last `hours_back` hours.

    We only select fields needed for deduplication in SignalRegistry:
        - signal_symbol
        - position_type
        - found_at
    """
    sql = """
        SELECT
            signal_symbol,
            position_type,
            found_at
        FROM signals
        WHERE found_at IS NOT NULL
          AND found_at >= NOW() - (%s * INTERVAL '1 hour')
        ORDER BY found_at ASC;
    """

    params = (hours_back,)

    rows: List[Dict[str, Any]] = []

    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            for (signal_symbol, position_type, found_at) in cur.fetchall():
                rows.append(
                    {
                        "signal_symbol": signal_symbol,
                        "position_type": position_type,
                        "found_at": found_at,
                    }
                )

    return rows


def fetch_open_signals_for_open_registry(conn: psycopg.Connection) -> List[Dict[str, Any]]:
    """
    Fetch ALL signals that were sent to broker and are still open (not exited yet).

    This is used ONLY for bootstrapping OpenSignalRegistry after app restart.
    We do NOT use the identity id column.

    Returned keys match what OpenSignalRegistry.bootstrap_from_db(...) expects.
    """
    sql = """
        SELECT
            signal_symbol,
            position_type,
            event_time,
            target_price,
            position_price,
            record_time,
            order_env,
            broker_order_id,
            broker_trade_id,
            order_units,
            actual_entry_time,
            actual_entry_price,
            actual_tp_price,
            order_status,
            exec_latency_ms,
            nearest_pips_to_target,
            farthest_pips_to_target
        FROM public.signals
        WHERE order_sent = true
          AND order_status = 'open'
        ORDER BY event_time ASC;
    """

    rows: List[Dict[str, Any]] = []
    with conn.cursor() as cur:
        cur.execute(sql)
        for (
            signal_symbol,
            position_type,
            event_time,
            target_price,
            position_price,
            record_time,
            order_env,
            broker_order_id,
            broker_trade_id,
            order_units,
            actual_entry_time,
            actual_entry_price,
            actual_tp_price,
            order_status,
            exec_latency_ms,
            nearest_pips_to_target,
            farthest_pips_to_target,
        ) in cur.fetchall():
            rows.append(
                {
                    # OpenSignalRegistry naming
                    "exchange": "OANDA",          # your current system is OANDA; keep as a constant for now
                    "symbol": signal_symbol,
                    "timeframe": "1m",            # if you store timeframe elsewhere later, update here
                    "side": position_type,        # buy/sell
                    "event_time": event_time,
                    "target_price": target_price,
                    "position_price": position_price,
                    "created_at": record_time,

                    # Optional order info (nice to have in OpenSignal)
                    "order_env": order_env,
                    "broker_order_id": broker_order_id,
                    "broker_trade_id": broker_trade_id,
                    "order_units": order_units,
                    "actual_entry_time": actual_entry_time,
                    "actual_entry_price": actual_entry_price,
                    "actual_tp_price": actual_tp_price,
                    "order_status": order_status,
                    "exec_latency_ms": exec_latency_ms,

                    # Distance metrics (may be NULL)
                    "nearest_pips_to_target": nearest_pips_to_target,
                    "farthest_pips_to_target": farthest_pips_to_target,
                }
            )

    return rows
