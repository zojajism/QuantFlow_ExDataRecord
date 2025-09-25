import asyncio
import json
import os
from datetime import datetime, timezone
import logging
import traceback
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
import asyncpg
from alert_manager import send_alert

logger = logging.getLogger(__name__)
load_dotenv()

# ---------- PG POOL ----------
_POOL: asyncpg.Pool | None = None

async def get_pg_pool() -> asyncpg.Pool:
    """
    Create (once) and return a global asyncpg pool.
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

# ---------- HELPERS ----------
def _ensure_utc_ts(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        # Assume input is UTC if naive
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def _iso(dt: datetime | None) -> str | None:
    return dt.isoformat(timespec="milliseconds") if dt else None

def _invalid_payload(d: dict | None) -> bool:
    return (not d) or any(v is None for v in d.values())

# ---------- SQL (UPSERTS) ----------
SQL_INSERT_TICKER = """
INSERT INTO public.tickers
    (exchange, symbol, tick_time, base_currency, quote_currency,
     bid, ask, last_price, high, low, volume, message_datetime)
VALUES
    ($1, $2, $3, $4, $5,
     $6, $7, $8, $9, $10, $11, $12)
ON CONFLICT (exchange, symbol, tick_time)
DO UPDATE SET
    base_currency = EXCLUDED.base_currency,
    quote_currency = EXCLUDED.quote_currency,
    bid = EXCLUDED.bid,
    ask = EXCLUDED.ask,
    last_price = EXCLUDED.last_price,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    volume = EXCLUDED.volume,
    message_datetime = EXCLUDED.message_datetime;
"""

SQL_INSERT_CANDLE = """
INSERT INTO public.candles
    (exchange, symbol, timeframe, open_time, close_time,
     base_currency, quote_currency, open, high, low, close, volume, message_datetime)
VALUES
    ($1, $2, $3, $4, $5,
     $6, $7, $8, $9, $10, $11, $12, $13)
ON CONFLICT (exchange, symbol, timeframe, close_time)
DO UPDATE SET
    open_time = EXCLUDED.open_time,
    base_currency = EXCLUDED.base_currency,
    quote_currency = EXCLUDED.quote_currency,
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    volume = EXCLUDED.volume,
    message_datetime = EXCLUDED.message_datetime;
"""

# ---------- RETRY WRAPPERS ----------
@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(1),
    retry=retry_if_exception_type(Exception)
)
async def _insert_ticker_retry(pool: asyncpg.Pool, params: tuple):
    async with pool.acquire() as conn:
        await conn.execute(SQL_INSERT_TICKER, *params)

@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(1),
    retry=retry_if_exception_type(Exception)
)
async def _insert_candle_retry(pool: asyncpg.Pool, params: tuple):
    async with pool.acquire() as conn:
        await conn.execute(SQL_INSERT_CANDLE, *params)

# ---------- PUBLIC API ----------
async def insert_tickers_to_db_async(pool: asyncpg.Pool, ticker_data: dict):
    """
    Upsert one ticker row. Keys expected:
    exchange, symbol, tick_time (datetime), base_currency, quote_currency,
    bid, ask, last_price, high, low, volume, message_datetime (datetime)
    """
    try:
        if _invalid_payload(ticker_data):
            logger.warning(json.dumps({
                "EventCode": 0,
                "Message": f"Skipped insert due to invalid ticker data: {ticker_data}"
            }))
            return

        tick_time = _ensure_utc_ts(ticker_data["tick_time"])
        msg_dt = _ensure_utc_ts(ticker_data.get("message_datetime"))

        params = (
            str(ticker_data["exchange"]),
            str(ticker_data["symbol"]),
            tick_time,
            str(ticker_data["base_currency"]),
            str(ticker_data["quote_currency"]),
            ticker_data["bid"],
            ticker_data["ask"],
            ticker_data["last_price"],
            ticker_data["high"],
            ticker_data["low"],
            ticker_data["volume"],
            msg_dt,
        )

        await _insert_ticker_retry(pool, params)

        logger.info(json.dumps({
            "EventCode": 1,
            "Message": "Ticker inserted",
            "exchange": ticker_data["exchange"],
            "symbol": ticker_data["symbol"],
            "tick_time": _iso(tick_time),
            "last_price": ticker_data["last_price"]
        }))

    except Exception as e:
        logger.error(json.dumps({
            "EventCode": -1,
            "Message": f"Error inserting ticker data: {ticker_data} | Exception: {str(e)}\n{traceback.format_exc()}"
        }))
        
        await send_alert("DBWriterApp-Ticker", str(e))
        

async def insert_candles_to_db_async(candle_data: dict):
    """
    Upsert one candle row. Keys expected:
    exchange, symbol, timeframe,
    open_time (datetime), close_time (datetime),
    base_currency, quote_currency,
    open, high, low, close, volume,
    message_datetime (datetime)
    """
    try:
        if _invalid_payload(candle_data):
            logger.warning(json.dumps({
                "EventCode": 0,
                "Message": f"Skipped insert due to invalid candle data: {candle_data}"
            }))
            return

        open_time = _ensure_utc_ts(candle_data["open_time"])
        close_time = _ensure_utc_ts(candle_data["close_time"])
        msg_dt = _ensure_utc_ts(candle_data.get("message_datetime"))

        params = (
            str(candle_data["exchange"]),
            str(candle_data["symbol"]),
            str(candle_data["timeframe"]),
            open_time,
            close_time,
            str(candle_data["base_currency"]),
            str(candle_data["quote_currency"]),
            candle_data["open"],
            candle_data["high"],
            candle_data["low"],
            candle_data["close"],
            candle_data["volume"],
            msg_dt,
        )

        pool = await get_pg_pool()
        await _insert_candle_retry(pool, params)

        logger.info(json.dumps({
            "EventCode": 2,
            "Message": "Candle inserted",
            "exchange": candle_data["exchange"],
            "symbol": candle_data["symbol"],
            "timeframe": candle_data["timeframe"],
            "close_time": _iso(close_time),
            "close": candle_data["close"]
        }))

    except Exception as e:
        logger.error(json.dumps({
            "EventCode": -1,
            "Message": f"Error inserting candle data: {candle_data} | Exception: {str(e)}\n{traceback.format_exc()}"
        }))
        
        await send_alert("DBWriterApp-Candle", str(e))
        

# ---------- OPTIONAL: batch helpers ----------
async def insert_candles_batch_async(candles: list[dict]):
    """
    Batch insert/upsert candles using executemany-style loop.
    For very large batches consider asyncpg.copy_records_to_table instead.
    """
    if not candles:
        return
    pool = await get_pg_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            for c in candles:
                try:
                    if _invalid_payload(c): 
                        continue
                    await conn.execute(
                        SQL_INSERT_CANDLE,
                        str(c["exchange"]),
                        str(c["symbol"]),
                        str(c["timeframe"]),
                        _ensure_utc_ts(c["open_time"]),
                        _ensure_utc_ts(c["close_time"]),
                        str(c["base_currency"]),
                        str(c["quote_currency"]),
                        c["open"], c["high"], c["low"], c["close"], c["volume"],
                        _ensure_utc_ts(c.get("message_datetime")),
                    )
                except Exception:
                    logger.exception("Failed to insert one candle in batch")
