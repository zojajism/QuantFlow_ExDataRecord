import asyncio
import json
import os
from dotenv import load_dotenv
import clickhouse_connect
from datetime import datetime, timezone, timedelta
import logging
import traceback
import logging
import traceback
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

logger = logging.getLogger(__name__)

load_dotenv()

def get_clickhouse_client():
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST"),
        port=int(os.getenv("CLICKHOUSE_PORT")),
        username=os.getenv("CLICKHOUSE_USER"),
        password=os.getenv("CLICKHOUSE_PASSWORD"),
        database=os.getenv("CLICKHOUSE_DATABASE")
    )

    
@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(1),
    retry=retry_if_exception_type(Exception)
)
def _insert_tickers_retry(db_client, record):
    db_client.insert(
        "tickers",
        record,
        column_names=[
            "exchange", "tick_time", "symbol", "base_currency", "quote_currency",
            "bid", "ask", "last_price", "high", "low", "volume", "Message_DateTime"
        ]
    )

async def insert_tickers_to_db_async(db_client, ticker_data):
    try:
        # Validation
        if not ticker_data or any(v is None for v in ticker_data.values()):
            logger.warning(
                        json.dumps({
                                "EventCode": 0,
                                "Message": f"Skipped insert due to invalid ticker data: {ticker_data}"
                            })
                    )
            return

        record = [[
            ticker_data["exchange"], ticker_data["tick_time"], ticker_data["symbol"],
            ticker_data["base_currency"], ticker_data["quote_currency"],
            ticker_data["bid"], ticker_data["ask"], ticker_data["last_price"],
            ticker_data["high"], ticker_data["low"], ticker_data["volume"],
            ticker_data["Message_DateTime"]
        ]]

        # Run insert with retry in async context
        await asyncio.to_thread(_insert_tickers_retry, db_client, record)

        logger.info(
                        json.dumps({
                                "EventCode": 1,
                                "Message": f"Ticker inserted",
                                "exchange": "Binance",
                                "symbol": ticker_data["symbol"],
                                "tick_time": ticker_data["tick_time"].isoformat(),  # datetime -> ISO string,
                                "last_price": ticker_data["last_price"]
                            })
                    )
    except Exception as e:
        logger.error(
                        json.dumps({
                                "EventCode": -1,
                                "Message": f"Error inserting ticker data: {ticker_data} | Exception: {str(e)}\n{traceback.format_exc()}"
                            })
                    )
        

@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(1),
    retry=retry_if_exception_type(Exception)
)
def _insert_candles_retry(db_client, record):
    db_client.insert(
        "candles",
        record,
        column_names=[
            "exchange",
            "symbol",
            "timeframe",
            "open_time",
            "close_time",
            "base_currency",
            "quote_currency",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "Message_DateTime"
        ]
    )

async def insert_candles_to_db_async(candle_data):
   

    client = get_clickhouse_client()
                    
    try:
        # Validation
        if not candle_data or any(v is None for v in candle_data.values()):
            logger.warning(
                        json.dumps({
                                "EventCode": 0,
                                "Message": f"Skipped insert due to invalid candle data: {candle_data}"
                            })
                    )
            return

        record = [[
            candle_data["exchange"],
            candle_data["symbol"],
            candle_data["timeframe"],
            candle_data["open_time"],
            candle_data["close_time"],
            candle_data["base_currency"],
            candle_data["quote_currency"],
            candle_data["open"],
            candle_data["high"],
            candle_data["low"],
            candle_data["close"],
            candle_data["volume"],
            candle_data["Message_DateTime"]
        ]]

        # Retry-aware insert
        await asyncio.to_thread(_insert_candles_retry, client, record)

        logger.info(
                        json.dumps({
                                "EventCode": 2,
                                "Message": f"Candle inserted",
                                "exchange": "Binance",
                                "symbol": candle_data["symbol"],
                                "timeframe": candle_data["timeframe"],
                                "close_time": candle_data["close_time"].isoformat(),  # datetime -> ISO string,
                                "close": candle_data["close"]
                            })    
        )
    except Exception as e:
        logger.error(
                        json.dumps({
                                "EventCode": -1,
                                "Message": f"Error inserting candle data: {candle_data} | Exception: {str(e)}\n{traceback.format_exc()}"
                            })
                    )
