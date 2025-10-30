import os
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
import psycopg
from psycopg.rows import tuple_row  # default row factory; returns tuples

from buffers.candle_buffer import Keys

load_dotenv()

# ----------------- Client -----------------
def get_pg_conn() -> psycopg.Connection:
    """
    Open a new PostgreSQL connection using environment variables.
    """
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        dbname=os.getenv("POSTGRES_DB"),
        autocommit=True,
        row_factory=tuple_row,  # rows as tuples (like ClickHouse client)
    )

# ----------------- Queries -----------------
def get_last_timestamp(
    exchange: str,
    symbol: str,
    timeframe: str,
) -> Optional[Any]:
    """
    Returns the MAX(open_time) from candles or None if empty.
    """
    sql = """
        SELECT MAX(open_time) AS last_candle_open_time
        FROM candles
        WHERE exchange = %s
          AND symbol   = %s
          AND timeframe= %s
    """
    with get_pg_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (exchange, symbol, timeframe))
        row = cur.fetchone()
        # row[0] will be None if no rows
        return row[0] if row else None

def get_candles_from_db(key: Keys, limit: int) -> List[Dict[str, Any]]:
    """
    Fetch latest `limit` candles in DESC order, then return ASC list of dicts.
    """
    sql = """
        SELECT
            open_time,
            close_time,
            open,
            high,
            low,
            close,
            volume
        FROM candles
        WHERE exchange = %s
          AND symbol   = %s
          AND timeframe= %s
        ORDER BY open_time DESC
        LIMIT %s
    """

    with get_pg_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (key.exchange, key.symbol, key.timeframe, limit))
        rows = cur.fetchall()
    
    # Convert to list of dicts in ASC order
    candles = [
        {
            "exchange": key.exchange,
            "symbol": key.symbol,
            "timeframe": key.timeframe,
            "open_time": row[0],
            "close_time": row[1],
            "open": row[2],
            "high": row[3],
            "low": row[4],
            "close": row[5],
            "volume": row[6],
        }
        for row in reversed(rows)
    ]
    return candles

def get_indicators_from_db(key: Keys, limit: int) -> List[Dict[str, Any]]:
    """
    Fetch latest `limit` indicators in DESC order, then return ASC list of dicts.
    """
    sql = """
        SELECT
            timestamp,
            indicator,
            value
        FROM indicators
        WHERE exchange = %s
          AND symbol   = %s
          AND timeframe= %s
        ORDER BY timestamp DESC
        LIMIT %s
    """
    with get_pg_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (key.exchange, key.symbol, key.timeframe, limit))
        rows = cur.fetchall()

    indicators = [
        {
            "exchange": key.exchange,
            "symbol": key.symbol,
            "timeframe": key.timeframe,
            "timestamp": row[0],
            "indicator": row[1],
            "value": row[2],
        }
        for row in reversed(rows)
    ]
    return indicators

def get_last_bios_from_db(exchange: str, symbol: str, timeframe: str) -> str:
    sql = """
            select bios from bios_signal bs 
            where 
                exchange = %s
                and symbol = %s
                and timeframe = %s
            order by "timestamp" desc
            limit 1
        """
    with get_pg_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (exchange, symbol, timeframe))
        rows = cur.fetchall()

    for row in reversed(rows):
      return row[0]
    
