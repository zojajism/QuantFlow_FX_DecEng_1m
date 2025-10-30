import os
import math
import asyncio
import logging
import traceback
from datetime import datetime, timezone
from typing import Dict

from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
import asyncpg

logger = logging.getLogger(__name__)
load_dotenv()

# ---------- PG POOL ----------
_POOL: asyncpg.Pool | None = None

async def get_pg_pool() -> asyncpg.Pool:
    """
    Create (once) and return a global asyncpg pool.
    """
    global _POOL
    if _POOL is None:
        _POOL = await asyncpg.create_pool(
            host=os.getenv("POSTGRES_HOST", "127.0.0.1"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=os.getenv("POSTGRES_DB", "quantflow_db"),  # keep your var name
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD", ""),
            min_size=1,
            max_size=int(os.getenv("POSTGRES_POOL_MAX", "10")),
            command_timeout=60,
        )
    return _POOL

# ---------- HELPERS ----------
def _ensure_utc_ts(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def _is_bad_number(x) -> bool:
    return (x is None) or (isinstance(x, float) and math.isnan(x))

# ---------- MAIN ASYNC BATCH INSERT ----------
@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(1),
    retry=retry_if_exception_type(Exception),
    reraise=True,
)
async def insert_indicators_to_db_async(
    exchange: str,
    symbol: str,
    timeframe: str,
    timestamp: datetime,
    close_price: float,
    results: Dict[str, float],
) -> None:
    """
    Fully-async insert of indicators using asyncpg.
    Builds a batch and inserts via executemany (1 statement run N times).
    """
    try:
        if not results:
            logger.warning("Skipped indicators insert: empty results dict")
            return

        ts = _ensure_utc_ts(timestamp)

        # Build records: (exchange, symbol, timeframe, timestamp, indicator, value, close_price)
        records = []
        for name, val in results.items():
            if _is_bad_number(val):
                logger.debug("Skipping %s: value is NaN/None", name)
                continue
            records.append((
                exchange,
                symbol,
                timeframe,
                ts,
                name,
                float(val),
                float(close_price),
            ))

        if not records:
            logger.debug("No valid indicator rows to insert.")
            return

        sql = """
            INSERT INTO indicators
                (exchange, symbol, timeframe, timestamp, indicator, value, close_price)
            VALUES ($1,$2,$3,$4,$5,$6,$7)
        """

        pool = await get_pg_pool()
        async with pool.acquire() as conn:
            await conn.executemany(sql, records)

        logger.info("Indicators inserted: %s %s ts=%s count=%d", symbol, timeframe, ts, len(records))

    except Exception as e:
        logger.error(
            "Error inserting indicators (PostgreSQL/asyncpg) | sym=%s tf=%s ts=%s | %s: %s\n%s",
            symbol, timeframe, timestamp, type(e).__name__, str(e), traceback.format_exc()
        )
        raise
