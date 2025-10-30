import os
import math
import logging
import traceback
from decimal import Decimal
from typing import Iterable, List, Dict, Any, Optional
from datetime import datetime, timezone

from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
import asyncpg

logger = logging.getLogger(__name__)
load_dotenv()

# ---------- asyncpg pool ----------
_POOL: Optional[asyncpg.Pool] = None

async def get_pg_pool() -> asyncpg.Pool:
    global _POOL
    if _POOL is None:
        _POOL = await asyncpg.create_pool(
            host=os.getenv("POSTGRES_HOST", "127.0.0.1"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=os.getenv("POSTGRES_DB", "quantflow_db"),
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD", ""),
            min_size=1,
            max_size=int(os.getenv("POSTGRES_POOL_MAX", "10")),
            command_timeout=60,
        )
    return _POOL

# ---------- utils ----------
def _is_bad_number(x: Any) -> bool:
    if x is None: return True
    if isinstance(x, float): return math.isnan(x)
    if isinstance(x, Decimal): return x.is_nan()
    return False

def _sanitize_str(x: Any) -> str:
    return "" if x is None else str(x)

def _utc(dt: datetime) -> datetime:
    if dt.tzinfo is None: return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

# ---------- batch insert (asyncpg) ----------
@retry(stop=stop_after_attempt(3), wait=wait_fixed(1),
       retry=retry_if_exception_type(Exception), reraise=True)
async def insert_bios_signal_to_db(
    exchange: str,
    symbol: str,
    timeframe: str,
    rows: Iterable[Dict[str, Any]],
) -> None:
    """
    Batch insert into bios_signal (async, no threads).
    rows: each dict has timestamp, close_price, score, raw_signal, bios, final_signal
    """
    records: List[tuple] = []
    for r in rows:
        ts = r.get("timestamp")
        close_price = r.get("close_price")
        score = r.get("score")
        if ts is None or _is_bad_number(close_price) or _is_bad_number(score):
            logger.debug("Skipping row: %s", r); continue
        records.append((
            exchange,
            symbol,
            timeframe,
            _utc(ts),
            float(close_price),
            float(score),
            _sanitize_str(r.get("raw_signal")),
            _sanitize_str(r.get("bios")),
            _sanitize_str(r.get("final_signal")),
        ))

    if not records:
        logger.debug("No valid bios/signal records to insert."); return

    sql = """
        INSERT INTO bios_signal
          (exchange, symbol, timeframe, timestamp, close_price, score, raw_signal, bios, final_signal)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
    """

    try:
        pool = await get_pg_pool()
        async with pool.acquire() as conn:
            await conn.executemany(sql, records)
        logger.info("Inserted %d rows into bios_signal", len(records))
    except Exception as e:
        logger.error(
            "PostgreSQL/asyncpg insert_bios_signal_to_db error | sym=%s tf=%s | %s: %s\n%s",
            symbol, timeframe, type(e).__name__, str(e), traceback.format_exc()
        )
        raise

# ---------- single-row helper (async) ----------
async def insert_bios_signal_to_db_async(
    exchange: str,
    symbol: str,
    timeframe: str,
    timestamp: datetime,
    close_price: Any,
    score: Any,
    bios: Optional[str] = None,
    raw_signal: Optional[str] = None,
    final_signal: Optional[str] = None,
) -> None:
    await insert_bios_signal_to_db(
        exchange, symbol, timeframe,
        rows=[{
            "timestamp": timestamp,
            "close_price": close_price,
            "score": score,
            "bios": bios,
            "raw_signal": raw_signal,
            "final_signal": final_signal,
        }],
    )
