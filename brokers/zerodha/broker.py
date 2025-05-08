import gzip
import boto3
import json
import logging
import aiohttp
import asyncio
import polars as pl
from io import BytesIO
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import io
from kiteconnect import KiteConnect
from ..base.broker import BaseBroker
from .token_rotator import ZerodhaTokenRotator
from dotenv import load_dotenv
import os

load_dotenv()


class ZerodhaBroker(BaseBroker):
    """
    Broker implementation for Zerodha (Kite Connect) API.

    Handles authentication via token rotator, fetching instrument master data,
    live price quotes, and historical candle data, returning results as Polars DataFrames.
    """
    BASE_URL = "https://api.kite.trade/"
    ZERODHA_API_KEY = os.getenv("ZERODHA_API_KEY")

    def _get_broker_name(self) -> str:
        """
        Internal method to retrieve the broker name.

        Returns:
            str: Literal name of the broker, "Zerodha".
        """
        return 'Zerodha'

    async def initialize(self) -> None:
        """
        Initialize the broker by fetching a valid access token and loading
        the daily instrument master data into a Polars DataFrame.

        Raises:
            Exception: If initialization steps fail (token fetch or master data load).
        """
        try:
            self.logger.info('Initializing ZerodhaBroker')
            # Fetch a fresh access token via token rotator
            self.access_token = await self.fetch_access_token()
            # Load instrument master data
            self.master_data = await self._get_zerodha_master_data()
            # Store as Polars DataFrame for fast filtering
            self.master_df = pl.DataFrame(data=self.master_data)

        except Exception as e:
            self.logger.error(f"Initialization failed: {e}")
            raise Exception("Initialization failed.")

    async def _get_zerodha_master_data(self) -> pl.DataFrame:
        """
        Fetch the daily gzipped CSV of all instruments from Zerodha,
        decompress and parse it into a Polars DataFrame.

        Returns:
            pl.DataFrame: DataFrame containing instrument_token, exchange_token,
            tradingsymbol, name, last_price, expiry, strike, tick_size,
            lot_size, instrument_type, segment, exchange.

        Raises:
            Exception: On HTTP or parsing errors.
        """
        try:
            url = self.BASE_URL + 'instruments'
            headers = {
                "Authorization": f"token {self.ZERODHA_API_KEY}:{self.access_token}",
                "X-Kite-Version": "3",
            }
            async with aiohttp.ClientSession() as session:
                async with session.get(url=url, headers=headers) as response:
                    response.raise_for_status()
                    csv_bytes = await response.read()

            # Parse CSV into Polars, overriding strike type to Float
            df = pl.read_csv(
                io.BytesIO(csv_bytes),
                schema_overrides={
                    "strike": pl.Float64
                }
            )
            return df

        except Exception as e:
            self.logger.exception(e)
            raise

    async def ltp_quote(self, request_data: List[Dict[str, str]]) -> Dict[str, Any]:
        """
        Retrieve the latest traded price (LTP) for a set of instruments.

        Args:
            request_data (List[Dict[str, str]]): List of dicts each containing:
                - exchange_token: str
                - exchange: str
                - instrument_type: str

        Returns:
            Dict[str, Any]: Mapping of "exchange:tradingsymbol" to LTP info.

        Raises:
            ValueError: If an exchange_token is not found in master data.
            Exception: On HTTP failures or API errors.
        """
        try:
            instrument_key_list = []
            # Map each request to an instrument key
            for data in request_data:
                exchange_token = data.get("exchange_token", "")
                exchange = data.get("exchange", "NSE")
                instrument_rows = self.master_df.filter(
                    (pl.col('exchange_token') == int(exchange_token)) &
                    (pl.col('exchange') == exchange)
                )
                if instrument_rows.is_empty():
                    error_msg = f"exchange_token: {exchange_token} not found in master data."
                    self.logger.error(error_msg)
                    raise ValueError(error_msg)
                trading_symbol = instrument_rows['tradingsymbol'][0]
                instrument_key_list.append(f"{exchange}:{trading_symbol}")

            # Chunk requests to avoid URL length limits
            CHUNK_SIZE = 750
            combined_response = {}
            for chunk in [instrument_key_list[i:i+CHUNK_SIZE] for i in range(0, len(instrument_key_list), CHUNK_SIZE)]:
                params = {'instrument_key': ",".join(chunk)}
                url = f"{self.BASE_URL}quote/ltp"
                headers = {
                    'Authorization': f"Bearer {self.access_token}",
                    'Accept': 'application/json'
                }
                async with aiohttp.ClientSession() as session:
                    async with session.get(url=url, headers=headers, params=params) as response:
                        if response.status != 200:
                            text = await response.text()
                            raise Exception(f"LTP HTTP {response.status}: {text}")
                        resp_json = await response.json()
                        if resp_json.get('status') != 'success' or 'data' not in resp_json:
                            raise Exception(f"LTP API error: {resp_json}")
                        combined_response.update(await self.convert_quote(resp_json['data']))
                await asyncio.sleep(1)
            return combined_response

        except Exception as e:
            self.logger.error(f"Exception during LTP response retrieval: {e}")
            raise
    async def historical_data(
        self, exchange, exchange_token, instrument_type, interval, from_date, to_date
    ):
        """
        Proxy to BaseBroker.historical_data for fetching historical candles.

        Returns:
            pl.DataFrame: Candle data frame.
        """
        return await super().historical_data(
            exchange, exchange_token, instrument_type, interval, from_date, to_date
        )

    async def full_market_quote(self, exchange_token, exchange, instrument_type):
        """
        Proxy to BaseBroker.full_market_quote for fetching intraday OHLC and volume.

        Returns:
            dict: Market quote data including 'ohlc', 'volume', 'oi', 'timestamp'.
        """
        return await super().full_market_quote(
            exchange_token, exchange, instrument_type
        )

    async def fetch_access_token(self) -> str:
        """
        Obtain a fresh access token via the ZerodhaTokenRotator.

        Returns:
            str: Valid API access token.

        Raises:
            Exception: If token rotation or retrieval fails.
        """
        token_rotator = ZerodhaTokenRotator(
            config=self.config,
            logger=self.logger
        )
        return token_rotator.get_current_token()
