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

Candle_SUBJECT = "candles.>"   
Candle_STREAM = "STREAM_CANDLES"   

Tick_SUBJECT = "ticks.>"   
Tick_STREAM  = "STREAM_TICKS"              

Tick_DURABLE_NAME = 'candle-FX-DecEng-1m'
Candle_DURABLE_NAME = 'tick-FX-DecEng-1m'

CONFIG_PATH = Path("/data/config.yaml")
if not CONFIG_PATH.exists():
    CONFIG_PATH = Path(__file__).resolve().parent / "data" / "config.yaml"

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

        print(symbols)
        print(timeframes)

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

        print("ready")
        
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
                        logger.info(f"Indicator values for symbol: {symbol}, timeframe: {timeframe}, close time: {candle_data['close_time']}, Indicator count: {len(values.items())}")
                
                        # 3) Call DecisionEngine on candle close (update bias or trade)
                        close_ts = int(candle_data["close_time"].timestamp())
                        #await engine.on_candle_close(tf=timeframe, ts=close_ts)

                        # 4) Insert indicators into Database
                        await insert_indicators_to_db_async(
                            exchange=candle_data["exchange"],
                            symbol=symbol,
                            timeframe=timeframe,
                            timestamp=candle_data["close_time"],
                            close_price=candle_data["close"],
                            results=values,
                        )                         


                        dq = buffers.CANDLE_BUFFER.get_or_create(key)
                        if dq:
                            first_candle = dq[0]
                            last_candle = dq[-1]
                            print(f"First candle {first_candle['symbol']} : Open_time={first_candle['open_time']} close_time={first_candle['close_time']}, O={first_candle['open']}, C={first_candle['close']}")
                            print(f"Last  candle {last_candle['symbol']} : Open_time={last_candle['open_time']} close_time={last_candle['close_time']}, O={last_candle['open']}, C={last_candle['close']}")
                            print(f"Buffer size: {len(dq)}\n")

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
