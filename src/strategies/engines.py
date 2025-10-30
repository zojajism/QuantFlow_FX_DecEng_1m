# English-only comments

# e.g., src/runtime/engines.py
from typing import Dict
from buffers.indicator_buffer_provider import IndicatorBufferProvider
from strategies.decision_engine import DecisionEngine
import buffers.buffer_initializer as buffers
from database.db_general import get_last_bios_from_db

ENGINES: Dict[str, DecisionEngine] = {}  # key: f"{exchange}:{symbol}"

def get_engine(exchange: str, symbol: str) -> DecisionEngine:
    key = f"{exchange}:{symbol}"
    if key not in ENGINES:
        dp = IndicatorBufferProvider(exchange=exchange, indicator_buffer=buffers.INDICATOR_BUFFER)
        ENGINES[key] = DecisionEngine(exchange=exchange, symbol=symbol, dp=dp)
        ENGINES[key].bias_state["1h"] = get_last_bios_from_db("Binance", "ETH/USDT", "1h")
        ENGINES[key].bias_state["4h"] = get_last_bios_from_db("Binance", "ETH/USDT", "4h")
        print(f"Bias for 1h: {ENGINES[key].bias_state['1h']}")
        print(f"Bias for 4h: {ENGINES[key].bias_state['4h']}")
                
    return ENGINES[key]
