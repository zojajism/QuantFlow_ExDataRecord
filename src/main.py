import asyncio
from datetime import datetime
import json
from nats.aio.client import Client as NATS
from nats.js import api
import os
from logger_config import setup_logger
from db_writer_pg import insert_candles_to_db_async, insert_tickers_to_db_async
from db_writer_pg import get_pg_pool
from NATS_setup import ensure_streams_from_yaml
from telegram_notifier import notify_telegram, ChatType, start_telegram_notifier, close_telegram_notifier, ChatType


Candle_SUBJECT = "candles.>"   
Candle_STREAM = "STREAM_CANDLES"   

Tick_SUBJECT = "ticks.>"   
Tick_STREAM  = "STREAM_TICKS"              

async def main():
    
    await start_telegram_notifier()
    try:
        logger = setup_logger()
        logger.info("Starting QuantFlow_ExDataRecorder system...")
        logger.info(
                    json.dumps({
                            "EventCode": 0,
                            "Message": f"Starting QuantFlow_ExDataRecorder system..."
                        })
                )
        notify_telegram(f"❇️ Data Writer App started....", ChatType.ALERT)
       
        nc = NATS()
        await nc.connect(os.getenv("NATS_URL"), user=os.getenv("NATS_USER"), password=os.getenv("NATS_PASS"))
        await ensure_streams_from_yaml(nc, "streams.yaml")
    
        js = nc.jetstream()

        try:
            db_pool = await get_pg_pool()
        except Exception as e:
            notify_telegram(f"⛔️ DBConnectionError \n" + str(e), ChatType.ALERT)                    
            raise

        # --- Consumer 1: Tick Engine (receives NEW messages)
        try:
            await js.delete_consumer(Tick_STREAM, "tick-quant-engine")
        except Exception:
            pass
        
        await js.add_consumer(
            Tick_STREAM,
            api.ConsumerConfig(
                durable_name="tick-quant-engine",
                filter_subject=Tick_SUBJECT,
                ack_policy=api.AckPolicy.EXPLICIT,
                deliver_policy=api.DeliverPolicy.NEW,  
                max_ack_pending=5000,
            )
        )

        # --- Consumer 2: Candle Engine (also receives NEW messages)
        try:
            await js.delete_consumer(Candle_STREAM, "candle-quant-engine")
        except Exception:
            pass    

        await js.add_consumer(
            Candle_STREAM,
            api.ConsumerConfig(
                durable_name="candle-quant-engine",
                filter_subject=Candle_SUBJECT,
                ack_policy=api.AckPolicy.EXPLICIT,
                deliver_policy=api.DeliverPolicy.NEW, 
                max_ack_pending=5000,
            )
        )


        # Pull-based subscription for Tick Engine
        sub_Tick = await js.pull_subscribe(Tick_SUBJECT, durable="tick-quant-engine")
    
        # Pull-based subscription for Candle Engine 
        sub_Candle = await js.pull_subscribe(Candle_SUBJECT, durable="candle-quant-engine")


        async def tick_engine_worker():
            while True:
                try:
                    msgs = await sub_Tick.fetch(100, timeout=1)
                    for msg in msgs:
                        logger.info(f"Received from {msg.subject}")
                        
                        tick_data = json.loads(msg.data.decode("utf-8"))
                        tick_data["tick_time"] = datetime.fromisoformat(tick_data["tick_time"])
                        tick_data["message_datetime"] = datetime.fromisoformat(tick_data["insert_ts"])

                        await insert_tickers_to_db_async(db_pool, tick_data)

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
                        logger.info(f"Received from {msg.subject}: ")

                        candle_data = json.loads(msg.data.decode("utf-8"))
                        candle_data["open_time"] = datetime.fromisoformat(candle_data["open_time"])
                        candle_data["close_time"] = datetime.fromisoformat(candle_data["close_time"])
                        candle_data["message_datetime"] = datetime.fromisoformat(candle_data["insert_ts"])
                        
                        await insert_candles_to_db_async(candle_data)

                        await msg.ack()
                except Exception as e:
                    logger.error(
                            json.dumps({
                                    "EventCode": -1,
                                    "Message": f"NATS error: Candle, {e}"
                                })
                        )
                    #notify_telegram(f"⛔️ NATS-Error-Candle-Engine \n" + str(e), ChatType.ALERT)                    
                    await asyncio.sleep(0.05)
        
        logger.info(
                json.dumps({
                            "EventCode": 0,
                            "Message": f"Subscriber Quant Engine starts...."
                        })
                )
    
    
        await asyncio.gather(tick_engine_worker(), candle_engine_worker())
        
    finally:
        notify_telegram(f"⛔️ Data Writer App stopped.", ChatType.ALERT)
        await close_telegram_notifier()

if __name__ == "__main__":
    asyncio.run(main())
