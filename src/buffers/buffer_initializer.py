# English-only comments

from typing import List, Dict, Any
from buffers.candle_buffer import CandleBuffer, Keys
from buffers.indicator_buffer import IndicatorBuffer
from database.db_general import get_candles_from_db, get_indicators_from_db

CANDLE_BUFFER: CandleBuffer = None
INDICATOR_BUFFER: IndicatorBuffer = None

def init_buffers(exchange: str, symbols: List[str], timeframes: List[str]) -> None:
    """
    Initialize global CandleBuffer and IndicatorBuffer.
    Load existing candles/indicators from DB if available.
    """
    global CANDLE_BUFFER, INDICATOR_BUFFER

    # Candle capacity (per Keys)
    def candle_capacity_fn(key: Keys) -> int:
        return 300  # adjust as needed based on your largest window

    # Indicator capacity (per (Keys, indicator))
    def indicator_capacity_fn(key: Keys, indicator: str) -> int:
        return 1  # adjust per indicator if needed

    # Create buffers
    CANDLE_BUFFER = CandleBuffer(capacity_fn=candle_capacity_fn)
    INDICATOR_BUFFER = IndicatorBuffer(capacity_fn=indicator_capacity_fn)

    # Preload from DB (optional)
    for symbol in symbols:
        for timeframe in timeframes:
            key = Keys(exchange=exchange, symbol=symbol, timeframe=timeframe)

            candles = get_candles_from_db(key, candle_capacity_fn(key))
            for candle in candles:
                CANDLE_BUFFER.append(key, candle)

            indicators = get_indicators_from_db(key, 128)  # or any limit you want
            for ind in indicators:
                INDICATOR_BUFFER.append(key, ind["indicator"], ind)

    return CANDLE_BUFFER, INDICATOR_BUFFER
