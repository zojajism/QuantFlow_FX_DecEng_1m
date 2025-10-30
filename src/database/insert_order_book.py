# English-only comments

import os
import logging
import traceback
from typing import Optional

import asyncpg
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

logger = logging.getLogger(__name__)
load_dotenv()

# ---------- asyncpg pool ----------
_POOL: Optional[asyncpg.Pool] = None

async def get_pg_pool() -> asyncpg.Pool:
    """
    Lazily create a global asyncpg pool using .env settings.
    """
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

async def close_pg_pool() -> None:
    """
    Close the global pool (optional; call on graceful shutdown).
    """
    global _POOL
    if _POOL is not None:
        await _POOL.close()
        _POOL = None

# ---------- single-row insert ----------
@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(1),
    retry=retry_if_exception_type(Exception),
    reraise=True,
)
async def insert_order_book_to_db_async(
    exchange: str,
    symbol: str,
    timeframe: str,
    side: str,          # "BUY" or "SELL"
    event: str,
    balance: float,     # USD balance after/for the order
    price: float,       # execution price
    quantity: float,    # position size in base asset
    stop_loss: float,   # SL price
    take_profit: float, # TP price
    fees: float,        # total fees (USD)
) -> None:
    """
    Insert a single row into order_book using simple scalar values.
    """

    # Basic guardrails
    if side not in ("BUY", "SELL"):
        raise ValueError(f"Invalid side: {side}")

    sql = """
        INSERT INTO order_book
            (exchange, symbol, timeframe, side, event, balance, price, quantity, stop_loss, take_profit, fees)
        VALUES
            ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
    """

    try:
        pool = await get_pg_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                sql,
                str(exchange),
                str(symbol),
                str(timeframe),
                str(side),
                str(event),
                float(balance),
                float(price),
                float(quantity),
                float(stop_loss),
                float(take_profit),
                float(fees),
            )
        logger.info(
            "Inserted order_book row | %s %s %s %s price=%.8f qty=%.8f",
            exchange, symbol, timeframe, side, price, quantity
        )
    except Exception as e:
        logger.error(
            "PostgreSQL insert_order_book_to_db_async error | %s/%s %s %s | %s: %s\n%s",
            exchange, symbol, timeframe, side, type(e).__name__, str(e), traceback.format_exc()
        )
        raise
