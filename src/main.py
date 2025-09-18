import asyncio
from nats.aio.client import Client as NATS
from nats.js import api
import os
from logger_config import setup_logger

Candle_SUBJECT = "candles.>"   
Candle_STREAM = "STREAM_CANDLES"   

Tick_SUBJECT = "ticks.>"   
Tick_STREAM  = "STREAM_TICKS"              

async def main():

    logger = setup_logger()
    logger.info("Starting QuantFlow_ExDataRecorder system...")

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
                for m in msgs:
                    logger.info(f"Received from {m.subject}: {m.data.decode()}")
                    await m.ack()
            except Exception:
                print("A")
                await asyncio.sleep(0.05)

    async def candle_worker():
        while True:
            try:
                msgs = await sub_Candle.fetch(100, timeout=1)
                for m in msgs:
                    logger.info(f"Received from {m.subject}: {m.data.decode()}")
                    await m.ack()
            except Exception:
                print("B")
                await asyncio.sleep(0.05)

    print("Gather starts....")
    await asyncio.gather(tick_db_worker(), candle_worker())

if __name__ == "__main__":
    asyncio.run(main())
