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
from indicators.indicator_calculator import compute_and_append_on_close
from database.insert_indicators import insert_indicators_to_db_async
from telegram_notifier import notify_telegram, ChatType, start_telegram_notifier, close_telegram_notifier, ChatType
from dotenv import load_dotenv
import yaml
from pathlib import Path
from entry import on_candle_closed, init_entry
from buffers.tick_registry_provider import get_tick_registry

from signals.open_signal_registry import get_open_signal_registry

from database.db_general import get_pg_conn

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

        DB_Conn = get_pg_conn()


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
        init_entry(CANDLE_BUFFER)

        
        logger.info(
                    json.dumps({
                            "EventCode": 0,
                            "Message": f"Candle buffer initialized."
                        })
                )
        
        # --- Consumer 1: Tick Engine (receives NEW messages)
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

        # --- Consumer ۲: Candle Engine
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
        sub_Tick = await js.pull_subscribe(Tick_SUBJECT, durable=Tick_DURABLE_NAME)
    
        # Pull-based subscription for Candle Engine
        sub_Candle = await js.pull_subscribe(Candle_SUBJECT, durable=Candle_DURABLE_NAME)


        async def tick_engine_worker():
            while True:
                try:
                    msgs = await sub_Tick.fetch(100, timeout=1)
                    for msg in msgs:
                        #logger.info(f"Received from {msg.subject}")
                        
                        tick_data = json.loads(msg.data.decode("utf-8"))
                        symbol = tick_data["symbol"]
                        exchange = tick_data["exchange"]
                        if (exchange == "OANDA" and symbol in symbols):

                            raw_tick_time = tick_data.get("tick_time")
                            if raw_tick_time:
                                try:
                                    if raw_tick_time.endswith("Z"):
                                        parsed_time = datetime.fromisoformat(raw_tick_time.replace("Z", "+00:00"))
                                    else:
                                        parsed_time = datetime.fromisoformat(raw_tick_time)
                                except Exception:
                                    parsed_time = datetime.utcnow()
                            else:
                                parsed_time = datetime.utcnow()
                                
                            bid = float(tick_data["bid"])
                            ask = float(tick_data["ask"])
                            #logger.info(f"Tick for {symbol}, tick_time:{parsed_time}")   
                            tick_registry = get_tick_registry()
                            tick_registry.update_tick(exchange, symbol, bid, ask, parsed_time)

                            open_sig_registry = get_open_signal_registry()
                            open_sig_registry.process_tick_for_symbol(
                                exchange=exchange,
                                symbol=symbol,
                                bid=bid,
                                ask=ask,
                                now=parsed_time,
                                conn=DB_Conn,   # or None if you handle DB elsewhere
                            )


                        await msg.ack()
                except Exception as e:
                    logger.error(
                            json.dumps({
                                    "EventCode": -1,
                                    "Message": f"NATS error: Tick, {e}"
                                })
                        )
                    #notify_telegram(f"⛔️ NATS-Error-Tick-Engine \n" + str(e), ChatType.ALERT)                    
                    await asyncio.sleep(0.05)


        async def candle_engine_worker():
            while True:
                try:
                    msgs = await sub_Candle.fetch(100, timeout=1)

                    for msg in msgs:
                        tokens = msg.subject.split(".")
                        timeframe = tokens[-1]
                        candle_data = json.loads(msg.data.decode("utf-8"))
                        symbol = candle_data["symbol"]
                        exchange = candle_data["exchange"]

                        #========== Main section, getting the candles we need ====================================
                        if (exchange == "OANDA" and timeframe in timeframes and symbol in symbols):
                            logger.info(f"Received from {msg.subject}: ")

                            candle_data["open_time"] = datetime.fromisoformat(candle_data["open_time"])
                            candle_data["close_time"] = datetime.fromisoformat(candle_data["close_time"])
                            candle_data["message_datetime"] = datetime.fromisoformat(candle_data["insert_ts"])
                            
                            key = Keys(exchange, symbol, timeframe)
                            buffers.CANDLE_BUFFER.append(key, candle_data)

                            on_candle_closed(exchange, symbol, "1m", candle_data["close_time"])
                        #========== Main section, getting the candles we need ====================================
                        
                        await msg.ack()

                except Exception as e:
                    await asyncio.sleep(0.05)
         
        logger.info(
                json.dumps({
                            "EventCode": 0,
                            "Message": f"Subscriber QuantFlow_Fx_DecEng_1m starts...."
                        })
                )
    
        await asyncio.gather(tick_engine_worker(), candle_engine_worker())

    finally:
        notify_telegram(f"⛔️ QuantFlow_Fx_DecEng_1m App stopped.", ChatType.ALERT)
        await close_telegram_notifier()

    
if __name__ == "__main__":
    asyncio.run(main())
