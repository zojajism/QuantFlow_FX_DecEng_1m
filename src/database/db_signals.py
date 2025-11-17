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
