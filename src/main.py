import asyncio
from datetime import datetime
import json
from nats.aio.client import Client as NATS
from nats.js import api
import os
from logger_config import setup_logger
from database.insert_indicators import get_pg_pool
from NATS_setup import ensure_streams_from_yaml
import buffers.buffer_initializer as buffers
from buffers.candle_buffer import Keys
from strategies.engines import get_engine
from indicators.indicator_calculator import compute_and_append_on_close
from database.insert_indicators import insert_indicators_to_db_async
from telegram_notifier import notify_telegram, ChatType, start_telegram_notifier, close_telegram_notifier, ChatType
from dotenv import load_dotenv
import yaml
from pathlib import Path
from indicators.pivots_simple import detect_pivots

Candle_SUBJECT = "candles.>"   
Candle_STREAM = "STREAM_CANDLES"   

Tick_SUBJECT = "ticks.>"   
Tick_STREAM  = "STREAM_TICKS"              

Tick_DURABLE_NAME = 'candle-FX-DecEng-1m'
Candle_DURABLE_NAME = 'tick-FX-DecEng-1m'

CONFIG_PATH = Path("/data/config.yaml")
if not CONFIG_PATH.exists():
    CONFIG_PATH = Path(__file__).resolve().parent / "data" / "config.yaml"


# --- Swing helpers (confirmed pivot only) ---
def last_confirmed(items: list[dict], current_len: int, n: int):
    """
    From a list of pivot dicts (each has 'index'), return the last pivot
    whose index <= current_len - n - 1 (needs n bars to the right).
    """
    cutoff = current_len - n - 1
    for it in reversed(items):
        if it["index"] <= cutoff:
            return it
    return None



async def main():

    await start_telegram_notifier()   

    try:

        # Try the Docker volume location first
        env_path = Path("/data/.env")
        # Fallback for local dev
        if not env_path.exists():
            env_path = Path(__file__).resolve().parent / "data" / ".env"
        load_dotenv(dotenv_path=env_path)



        logger = setup_logger()
        logger.info("Starting QuantFlow_Fx_DecEng_1m system...")
        logger.info(
                    json.dumps({
                            "EventCode": 0,
                            "Message": f"Starting QuantFlow_Fx_DecEng_1m system..."
                        })
                )
        notify_telegram(f"❇️ QuantFlow_Fx_DecEng_1m started....", ChatType.ALERT)


        if not CONFIG_PATH.exists():
            raise FileNotFoundError(f"Config file not found: {CONFIG_PATH}")

        with CONFIG_PATH.open("r", encoding="utf-8") as f:
            config_data = yaml.safe_load(f) or {}

        symbols = [str(s) for s in config_data.get("symbols", [])]
        timeframes = [str(t) for t in config_data.get("timeframes", [])]

        nc = NATS()
        await nc.connect(os.getenv("NATS_URL"), user=os.getenv("NATS_USER"), password=os.getenv("NATS_PASS"))
        await ensure_streams_from_yaml(nc, "streams.yaml")
        js = nc.jetstream()

        CANDLE_BUFFER, INDICATOR_BUFFER = buffers.init_buffers("OANDA", symbols, timeframes)
        
        logger.info(
                    json.dumps({
                            "EventCode": 0,
                            "Message": f"Candle buffer initialized."
                        })
                )

        # --- Consumer 1: Tick Engine
        '''
        try:
            await js.delete_consumer(Tick_STREAM, Tick_DURABLE_NAME)
        except Exception:
            pass
        
        await js.add_consumer(
            Tick_STREAM,
            api.ConsumerConfig(
                durable_name=Tick_DURABLE_NAME,
                filter_subject=Tick_SUBJECT,
                ack_policy=api.AckPolicy.EXPLICIT,
                deliver_policy=api.DeliverPolicy.NEW,  
                max_ack_pending=5000,
            )
        )
        '''

        # --- Consumer 2: Candle Engine
        try:
            await js.delete_consumer(Candle_STREAM, Candle_DURABLE_NAME)
        except Exception:
            pass    

        await js.add_consumer(
            Candle_STREAM,
            api.ConsumerConfig(
                durable_name=Candle_DURABLE_NAME,
                filter_subject=Candle_SUBJECT,
                ack_policy=api.AckPolicy.EXPLICIT,
                deliver_policy=api.DeliverPolicy.NEW, 
                max_ack_pending=5000,
            )
        )

        # Pull-based subscription for Tick Engine
        #sub_Tick = await js.pull_subscribe(Tick_SUBJECT, durable=Tick_DURABLE_NAME)
    
        # Pull-based subscription for Candle Engine
        sub_Candle = await js.pull_subscribe(Candle_SUBJECT, durable=Candle_DURABLE_NAME)

        '''
        async def tick_engine_worker():
            while True:
                try:
                    msgs = await sub_Tick.fetch(100, timeout=1)

                    for msg in msgs:
                        tokens = msg.subject.split(".")
                        symbol = tokens[-1]
                        logger.info(f"Received from {msg.subject}")
                            
                        tick_data = json.loads(msg.data.decode("utf-8"))
                        tick_data["tick_time"] = datetime.fromisoformat(tick_data["tick_time"])
                        tick_data["message_datetime"] = datetime.fromisoformat(tick_data["insert_ts"])

                        engine = get_engine(tick_data["exchange"], symbol)

                        await engine._paper_execute_sell_by_tick(float(tick_data["last_price"]))

                        await msg.ack()

                except Exception as e:
                    await asyncio.sleep(0.05)
    
        '''

        async def candle_engine_worker():
            while True:
                try:
                    msgs = await sub_Candle.fetch(100, timeout=1)

                    for msg in msgs:
                        tokens = msg.subject.split(".")
                        timeframe = tokens[-1]
                        candle_data = json.loads(msg.data.decode("utf-8"))
                        symbol = candle_data["symbol"]
                        if symbol == "EUR/USD":
                            logger.info(f"Received from {msg.subject}: ")

                            candle_data["open_time"] = datetime.fromisoformat(candle_data["open_time"])
                            candle_data["close_time"] = datetime.fromisoformat(candle_data["close_time"])
                            candle_data["message_datetime"] = datetime.fromisoformat(candle_data["insert_ts"])
                            
                            key = Keys(candle_data["exchange"], symbol, timeframe)

                            engine = get_engine(candle_data["exchange"], symbol)
                
                            # 1) append candle to CandleBuffer
                            buffers.CANDLE_BUFFER.append(key, candle_data)

                            # 2) compute indicators from CandleBuffer and append to IndicatorBuffer
                            values = compute_and_append_on_close(
                                key=key,
                                close_time=candle_data["close_time"],
                                close_price=candle_data["close"]
                            )
                            #logger.info(f"Indicator values for symbol: {symbol}, timeframe: {timeframe}, close time: {candle_data['close_time']}, Indicator count: {len(values.items())}")
                    
                            # 3) Call DecisionEngine on candle close (update bias or trade)
                            close_ts = int(candle_data["close_time"].timestamp())
                            #await engine.on_candle_close(tf=timeframe, ts=close_ts)

                            # 4) Insert indicators into Database
                            '''
                            await insert_indicators_to_db_async(
                                exchange=candle_data["exchange"],
                                symbol=symbol,
                                timeframe=timeframe,
                                timestamp=candle_data["close_time"],
                                close_price=candle_data["close"],
                                results=values,
                            )                         
                            '''


                            dq = buffers.CANDLE_BUFFER.get_or_create(key)
                            if dq:
                                first_candle = dq[0]
                                last_candle = dq[-1]
                                #print(f"First candle {first_candle['symbol']} : Open_time={first_candle['open_time']} close_time={first_candle['close_time']}, O={first_candle['open']}, C={first_candle['close']}")
                                #print(f"Last  candle {last_candle['symbol']} : Open_time={last_candle['open_time']} close_time={last_candle['close_time']}, O={last_candle['open']}, C={last_candle['close']}")
                                #print(f"Buffer size: {len(dq)}\n")

                                #========================= Pivot detecor ==============================

                                N_SWING = 5  # strength = 5-left + 5-right (TV-like)
                                if len(dq) >= 2 * N_SWING + 1:
                                    candles = list(dq)  # deque -> list of dicts; keeps your structure

                                    # Detect pivots from your buffer with your keys:
                                    peaks, lows = detect_pivots(
                                        candles=candles,
                                        n=N_SWING,
                                        eps=1e-9,                 # plateau merge tolerance in *price units*
                                        high_key="high",
                                        low_key="low",
                                        time_key="close_time",
                                        strict=False              # >= / <= inside window (TV-like)
                                    )

                                    peaks, lows = detect_pivots(
                                        candles=list(dq),      # dq from CandleBuffer
                                        n=N_SWING,
                                        eps=1e-9,
                                        high_key="high",
                                        low_key="low",
                                        time_key="close_time",
                                        open_time_key="open_time",
                                        strict=False,          # window test (>=/<=)
                                        hit_strict=True        # hit test: strictly higher/lower to the right
                                    )

                                    '''
                                    lp = peaks[-1] if peaks else None
                                    ll = lows[-1]  if lows  else None
                                    if lp:
                                        logger.info(f"SWING PEAK  idx={lp['index']} open={lp['open_time'].strftime("%H:%M")} high={lp['high']} hit={lp['hit']}")
                                        print(f"Len Peaks: {len(peaks)}")
                                    if ll:
                                        logger.info(f"SWING LOW   idx={ll['index']} open={ll['open_time'].strftime("%H:%M")} low={ll['low']}  hit={ll['hit']}")
                                        print(f"Len Lows: {len(lows)}")
                                        print("===============")
                                    '''
                                    # sort newest first
                                    peaks_sorted = sorted(peaks, key=lambda x: x["index"], reverse=True)
                                    lows_sorted  = sorted(lows,  key=lambda x: x["index"], reverse=True)

                                    # clear screen
                                    os.system('cls' if os.name == 'nt' else 'clear')

                                    # print summary counts
                                    print(f"Total Peaks: {len(peaks_sorted)} | Total Lows: {len(lows_sorted)}\n")

                                    # print peaks
                                    print("=== PEAKS (newest → oldest) ===")
                                    for p in peaks_sorted:
                                        t_open  = p["open_time"].strftime("%H:%M") if hasattr(p["open_time"], "strftime") else p["open_time"]
                                        t_close = p["time"].strftime("%H:%M") if hasattr(p["time"], "strftime") else p["time"]
                                        print(f"idx = {p['index']:3d}  |  open = {t_open}  |  high = {p['high']:.5f}  |  hit = {p['hit']}")

                                    print("\n=== LOWS (newest → oldest) ===")
                                    for l in lows_sorted:
                                        t_open  = l["open_time"].strftime("%H:%M") if hasattr(l["open_time"], "strftime") else l["open_time"]
                                        t_close = l["time"].strftime("%H:%M") if hasattr(l["time"], "strftime") else l["time"]
                                        print(f"idx = {l['index']:3d}  |  open = {t_open}  | low = {l['low']:.5f}  |  hit = {l['hit']}")                                
                                #======================================================================

                        await msg.ack()

                except Exception as e:
                    await asyncio.sleep(0.05)
        
        logger.info(
                json.dumps({
                            "EventCode": 0,
                            "Message": f"Subscriber Quant Engine starts...."
                        })
                )
    
    
        #await asyncio.gather(tick_engine_worker(), candle_engine_worker())
        await asyncio.gather(candle_engine_worker())

    finally:
        notify_telegram(f"⛔️ QuantFlow_Fx_DecEng_1m App stopped.", ChatType.ALERT)
        await close_telegram_notifier()

    
if __name__ == "__main__":
    asyncio.run(main())
