import asyncio
from datetime import datetime
import json
from nats.aio.client import Client as NATS
from nats.js import api
import os
from logger_config import setup_logger
from db_writer_pg import insert_candles_to_db_async, insert_tickers_to_db_async
from db_writer_pg import get_pg_pool

Candle_SUBJECT = "candles.>"   
Candle_STREAM = "STREAM_CANDLES"   

Tick_SUBJECT = "ticks.>"   
Tick_STREAM  = "STREAM_TICKS"              

async def main():

    logger = setup_logger()
    logger.info("Starting QuantFlow_ExDataRecorder system...")
    logger.info(
                json.dumps({
                        "EventCode": 0,
                        "Message": f"Starting QuantFlow_ExDataRecorder system..."
                    })
            )
    db_pool = await get_pg_pool()

    nc = NATS()
    await nc.connect(os.getenv("NATS_URL"), user=os.getenv("NATS_USER"), password=os.getenv("NATS_PASS"))
    
    js = nc.jetstream()
 
    # --- Consumer 1: Tick DB Writer (receives ALL messages independently)
    try:
        await js.delete_consumer(Tick_STREAM, "tick-db-writer")
    except Exception:
        pass
    
    await js.add_consumer(
        Tick_STREAM,
        api.ConsumerConfig(
            durable_name="tick-db-writer",
            filter_subject=Tick_SUBJECT,
            ack_policy=api.AckPolicy.EXPLICIT,
            deliver_policy=api.DeliverPolicy.ALL,  
            max_ack_pending=5000,
        )
    )

    # --- Consumer 2: Candle DB Writer (also receives ALL messages independently)
    try:
        await js.delete_consumer(Candle_STREAM, "candle-db-writer")
    except Exception:
        pass    

    await js.add_consumer(
        Candle_STREAM,
        api.ConsumerConfig(
            durable_name="candle-db-writer",
            filter_subject=Candle_SUBJECT,
            ack_policy=api.AckPolicy.EXPLICIT,
            deliver_policy=api.DeliverPolicy.ALL, 
            max_ack_pending=5000,
        )
    )


    # Pull-based subscription for Tick DB Writer
    sub_Tick = await js.pull_subscribe(Tick_SUBJECT, durable="tick-db-writer")
   
    # Pull-based subscription for Candle DB Writer 
    sub_Candle = await js.pull_subscribe(Candle_SUBJECT, durable="candle-db-writer")


    async def tick_db_worker():
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
                await asyncio.sleep(0.05)
   

    async def candle_db_worker():
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
                await asyncio.sleep(0.05)
    
    logger.info(
            json.dumps({
                         "EventCode": 0,
                         "Message": f"Subscriber starts...."
                    })
            )
       
    
    await asyncio.gather(tick_db_worker(), candle_db_worker())
    
    
if __name__ == "__main__":
    asyncio.run(main())
