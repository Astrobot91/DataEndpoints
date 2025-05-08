"""
Upstox broker implementation.

This module contains the UpstoxBroker class which implements the BaseBroker
interface for the Upstox trading platform.
"""

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

from ..base.broker import BaseBroker
from .token_rotator import UpstoxTokenRotator


class UpstoxBroker(BaseBroker):
    """
    Upstox broker implementation.
    
    This class implements the BaseBroker interface for the Upstox trading platform.
    
    Attributes:
        broker_name (str): The name of the broker ('Upstox').
        logger (logging.Logger): Logger instance for the broker.
        access_token (str): The current Upstox API access token.
        master_data (Dict): The Upstox master data containing instrument information.
        master_df (pl.DataFrame): DataFrame representation of the master data.
    """
    
    BASE_URL = "https://api.upstox.com/v2"
    BASE_ORDER_URL = "https://api-hft.upstox.com/v2"
    
    def _get_broker_name(self) -> str:
        """
        Get the name of the broker.
        
        Returns:
            str: The name of the broker ('Upstox').
        """
        return 'Upstox'
    
    async def initialize(self) -> None:
        """
        Initialize the Upstox broker with necessary configurations and data.
        
        This method fetches the access token, retrieves the master data,
        and prepares the broker for use.
        
        Raises:
            Exception: If initialization fails.
        """
        try:
            self.logger.info(f'Initializing UpstoxBroker')
            self.access_token = await self.fetch_access_token()
            self.master_data = await self._get_upstox_master_data()
            self.master_df = pl.DataFrame(data=self.master_data)
            if self.master_df is None:
                raise Exception("Instrument data could not be loaded.")
        except Exception as e:
            self.logger.error(f"Initialization failed: {e}")
            raise

    async def _get_upstox_master_data(self):
        return await super()._get_upstox_master_data()

    async def ltp_quote(self, request_data: List[Dict[str, str]]) -> Dict[str, Any]:
        """
        Get last traded price quotes for specified instruments.
        Handles chunking of requests to respect API limits (max 1000 instruments per request).
        Rate limited to 1 request per second.
        
        Args:
            request_data (List[Dict[str, str]]): List of dictionaries containing
                instrument identifiers like exchange_token, exchange, etc.
                
        Returns:
            Dict[str, Any]: Dictionary containing LTP data for requested instruments.
            
        Raises:
            ValueError: If instrument identifiers are invalid.
            Exception: If quote retrieval fails.
        """
        try:
            # Process all instrument keys first
            instrument_key_list = []
            for data in request_data:
                exchange_token = data.get("exchange_token", "")
                exchange = data.get("exchange", "NSE") 
                instrument_type = data.get("instrument_type", "")

                instrument_rows = self.master_df.filter(
                    (pl.col('exchange_token') == int(exchange_token)),
                    (pl.col('exchange') == f"{exchange}_{instrument_type}"),
                )
                if instrument_rows.is_empty():
                    error_msg = f'exchange_token: {exchange_token} not found in the upstox master file.'
                    self.logger.error(error_msg)
                    raise ValueError(error_msg)
                instrument_key = instrument_rows['instrument_key'][0]
                instrument_key_list.append(instrument_key)

            CHUNK_SIZE = 750
            chunks = [instrument_key_list[i:i + CHUNK_SIZE] 
                     for i in range(0, len(instrument_key_list), CHUNK_SIZE)]
            
            combined_response = {}
            
            for chunk in chunks:
                main_instrument_key = ",".join(chunk)
                
                url = f'{self.BASE_URL}/market-quote/ltp'
                headers = {
                    'Authorization': f'Bearer {self.access_token}',
                    'Accept': 'application/json'
                }
                params = {'instrument_key': main_instrument_key}

                async with aiohttp.ClientSession() as session:
                    async with session.get(url=url, headers=headers, params=params) as response:
                        if response.status == 200:
                            ltp_response = await response.json()
                            if ltp_response['status'] == 'success':
                                if 'data' in ltp_response:
                                    chunk_data = await self.convert_quote(response_data=ltp_response['data'])
                                    combined_response.update(chunk_data)
                                else:
                                    error_msg = f'LTP response data missing for: {params}'
                                    self.logger.error(error_msg)
                                    raise ValueError(error_msg)
                            else:
                                error_msg = f'LTP response retrieval unsuccessful. Details: {ltp_response}'
                                self.logger.error(error_msg)
                                raise Exception(error_msg)
                        else:
                            error_text = await response.text()
                            error_msg = f'Failed to retrieve LTP response: {response.status} - {error_text}, Headers: {headers}, Params: {params}'
                            self.logger.error(error_msg)
                            raise Exception(error_msg)
                            
                # Rate limiting - wait 1 second between chunks
                if chunks.index(chunk) < len(chunks) - 1:  # Don't wait after the last chunk
                    await asyncio.sleep(1)
                    
            return combined_response

        except Exception as e:
            self.logger.error(f'Exception during LTP response retrieval: {e}')
            raise

    async def ohlc_quote(
            self,
            request_data: List[Dict[str, str]],
            interval: str = "1d"  # Added interval parameter with a default
    ) -> Dict[str, Any]:   
        """
        Get OHLC quotes for multiple instruments.
        Handles chunking of requests to respect API limits (max 500 instruments per request, using chunks of 450).
        Rate limited to 1 request per second.

        Args:
            request_data (List[Dict[str, str]]): List of dictionaries, each containing
                instrument identifiers like 'exchange_token', 'exchange', 'instrument_type'.
            interval (str): Interval for OHLC data. Possible values: '1d', 'I1', 'I30'.
                            Defaults to '1d'.

        Returns:
            Dict[str, Any]: Dictionary containing OHLC quote data for each requested instrument,
                            keyed by their original instrument_key (after conversion).

        Raises:
            ValueError: If any instrument identifiers are invalid or not found, or if interval is invalid.
            Exception: If quote retrieval fails for any chunk.
        """
        try:
            valid_intervals = ["1d", "I1", "I30"]
            if interval not in valid_intervals:
                error_msg = f"Invalid interval: {interval}. Valid intervals are: {valid_intervals}"
                self.logger.error(error_msg)
                raise ValueError(error_msg)

            instrument_key_list = []
            for data in request_data:
                exchange_token = data.get("exchange_token", "")
                exchange = data.get("exchange", "NSE") 
                instrument_type = data.get("instrument_type", "")
                
                instrument_rows = self.master_df.filter(
                    (pl.col('exchange_token') == int(exchange_token)),
                    (pl.col('exchange') == f"{exchange}_{instrument_type}"),
                )
                if instrument_rows.is_empty():
                    error_msg = f'exchange_token: {exchange_token} not found in the upstox master file.'
                    self.logger.error(error_msg)
                    raise ValueError(error_msg)
                instrument_key = instrument_rows['instrument_key'][0]
                instrument_key_list.append(instrument_key)

            CHUNK_SIZE = 450
            chunks = [
                instrument_key_list[i:i + CHUNK_SIZE]
                for i in range(0, len(instrument_key_list), CHUNK_SIZE)]

            combined_response = {}
            
            for i, chunk in enumerate(chunks, 1): 
                if not chunk: 
                    continue

                main_instrument_key = ",".join(chunk)

                url = f'{self.BASE_URL}/market-quote/ohlc'
                headers = {
                    'Authorization': f'Bearer {self.access_token}',
                    'Accept': 'application/json'
                }
                params = {
                    'instrument_key': main_instrument_key,
                    'interval': interval
                }
                self.logger.debug(f"Requesting OHLC for chunk {i}/{len(chunks)} with interval {interval}")
                async with aiohttp.ClientSession() as session:
                    async with session.get(url=url, headers=headers, params=params) as response:
                        if response.status == 200:
                            ohlc_api_response = await response.json()
                            if ohlc_api_response.get('status') == 'success':
                                if 'data' in ohlc_api_response:
                                    chunk_data = await self.convert_quote(response_data=ohlc_api_response['data']) 
                                    combined_response.update(chunk_data)
                                else:
                                    error_msg = f"OHLC response data missing for chunk {i}: {params}"
                                    self.logger.error(error_msg)
                                    raise ValueError(error_msg)
                            else:
                                error_msg = f"OHLC response retrieval unsuccessful for chunk {i}. Details: {ohlc_api_response}"
                                self.logger.error(error_msg)
                                raise Exception(error_msg)                                
                        elif response.status == 429: # Rate limit
                            self.logger.warning(f"Rate limit hit on ohlc_quote chunk {i}. Waiting 60s.")
                            await asyncio.sleep(60)
                            error_msg = f"Rate limit hit on ohlc_quote chunk {i} (not retried)."
                            self.logger.error(error_msg)
                            raise Exception(error_msg)
                        else:
                            error_text = await response.text()
                            error_msg = f"Failed to retrieve OHLC response for chunk {i}: HTTP {response.status} - {error_text}. Params: {params}"
                            self.logger.error(error_msg)
                            raise Exception(error_msg)            

                # Rate limiting - wait 1 second between chunks
                if i < len(chunks):  # Use 'i' from enumerate
                    self.logger.debug(f"Waiting 1 second before next OHLC chunk...")
                    await asyncio.sleep(1)

            self.logger.info(f"Successfully retrieved OHLC quotes for {len(combined_response)} instruments.")
            return combined_response

        except ValueError as ve:
            self.logger.error(f"ValueError in ohlc_quote: {ve}")
            raise
        except Exception as e:
            self.logger.error(f"Exception during ohlc quote retrieval: {e}")
            raise

    async def full_market_quote(
        self,
        request_data: List[Dict[str, str]]
    ) -> Dict[str, Any]:
        """
        Get full market quotes for multiple instruments.
        Handles chunking of requests to respect API limits (max 500 instruments per request, using chunks of 450).
        Rate limited to 1 request per second.

        Args:
            quote_request_data (List[Dict[str, str]]): List of dictionaries, each containing
                instrument identifiers like 'exchange_token', 'exchange', 'instrument_type'.

        Returns:
            Dict[str, Any]: Dictionary containing full market quote data for each requested instrument,
                            keyed by their original instrument_key.

        Raises:
            ValueError: If any instrument identifiers are invalid or not found.
            Exception: If quote retrieval fails for any chunk.
        """
        try:
            instrument_key_list = []
            for data in request_data:
                exchange_token = data.get("exchange_token", "")
                exchange = data.get("exchange", "NSE") 
                instrument_type = data.get("instrument_type", "")

                instrument_rows = self.master_df.filter(
                    (pl.col('exchange_token') == int(exchange_token)),
                    (pl.col('exchange') == f"{exchange}_{instrument_type}"),
                )
                if instrument_rows.is_empty():
                    error_msg = f'exchange_token: {exchange_token} not found in the upstox master file.'
                    self.logger.error(error_msg)
                    raise ValueError(error_msg)
                instrument_key = instrument_rows['instrument_key'][0]
                instrument_key_list.append(instrument_key)

            CHUNK_SIZE = 450
            chunks = [
                instrument_key_list[i:i + CHUNK_SIZE]
                for i in range(0, len(instrument_key_list), CHUNK_SIZE)]

            combined_response = {}
            
            for chunk in chunks:
                main_instrument_key = ",".join(chunk)

                url = f'{self.BASE_URL}/market-quote/quotes'
                headers = {
                    'Authorization': f'Bearer {self.access_token}',
                    'Accept': 'application/json'
                }
                params = {'instrument_key': main_instrument_key}

                async with aiohttp.ClientSession() as session:
                    async with session.get(url=url, headers=headers, params=params) as response:
                        if response.status == 200:
                            quote_api_response = await response.json()
                            if quote_api_response.get('status') == 'success':
                                if 'data' in quote_api_response:
                                    chunk_data = await self.convert_quote(response_data=quote_api_response['data'])
                                    combined_response.update(chunk_data)
                                else:
                                    error_msg = f"Full market quote response data missing for: {params}"
                                    self.logger.error(error_msg)
                                    raise ValueError(error_msg)
                            else:
                                error_msg = f"Full market quote response retrieval unsuccessful. Details: {quote_api_response}"
                                self.logger.error(error_msg)
                                raise Exception(error_msg)                                
                        else:
                            error_text = await response.text()
                            error_msg = f"Failed to retrieve full market quote response: {response.status} - {error_text}, Headers: {headers}, Params: {params}"
                            self.logger.error(error_msg)
                            raise Exception(error_msg)
                
                # Rate limiting - wait 1 second between chunks
                if chunks.index(chunk) < len(chunks) - 1:  # Don't wait after the last chunk
                    await asyncio.sleep(1)

            return combined_response

        except ValueError as ve:
            self.logger.error(f"ValueError in full_market_quote: {ve}")
            raise
        except Exception as e:
            self.logger.error(f"Exception during full market quote retrieval: {e}")
            raise

    async def convert_quote(self, response_data: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """
        Converts instrument tokens in the quote data to exchange tokens.

        Args:
            ltp_response_data (dict): A dictionary where each value contains quote data with an 'instrument_token' key.

        Returns:
            dict: A dictionary with trading symbols as keys and quote data as values.

        Raises:
            Exception: If an error occurs during the conversion process.
        """
        output_dict = {}
        for ltp_data, value in response_data.items():
            instrument_key = value['instrument_token']
            if instrument_key is None:
                self.logger.error(f"Missing 'instrument_token' in quote data for key {ltp_data}")
                continue
            try:
                temp_df = self.master_df.filter(
                    pl.col('instrument_key') == instrument_key
                )
                value["trading_symbol"] = temp_df["tradingsymbol"][0]
                value["instrument_type"] = temp_df["exchange"][0].split("_")[1] 
                if temp_df.is_empty():
                    self.logger.error(f"No matching instrument for token {instrument_key}")
                    continue
                new_key = temp_df['exchange_token'][0]
                output_dict[new_key] = value

            except Exception as e:
                self.logger.error(f"Error converting instrument token {instrument_key}: {e}")
                raise
        
        return output_dict

    async def historical_data(
            self,
            exchange: str,
            exchange_token: str,
            instrument_type: str,
            interval: str,
            from_date: str,
            to_date: str
            ) -> List[Dict[str, Any]]:
        """
        Get historical candle data for a specified instrument.
        Handles chunking of requests to respect API limits (max 1000 days per request).
        Rate limited to 1 request per second.
        
        Args:
            exchange (str): Exchange name (e.g., 'NSE', 'BSE').
            exchange_token (str): Exchange token for the instrument.
            instrument_type (str): Type of instrument (e.g., 'EQ', 'FUT').
            interval (str): Time interval for candles (e.g., '1minute', '1day').
            from_date (str): Start date in 'YYYY-MM-DD' format.
            to_date (str): End date in 'YYYY-MM-DD' format.
            
        Returns:
            List[Dict[str, Any]]: List of dictionaries containing historical candle data
            with datetime, open, high, low, close, volume, oi fields.
        """
        try:
            # Validate instrument exists
            instrument_rows = self.master_df.filter(
                (pl.col('exchange_token') == int(exchange_token)),
                (pl.col('exchange') == f"{exchange}_{instrument_type}"),
            )
            if instrument_rows.is_empty():
                error_msg = f"exchange_token: {exchange_token} not found in upstox master file."
                self.logger.error(error_msg)
                raise ValueError(error_msg)
            instrument_key = instrument_rows['instrument_key'][0]

            # Split date range into chunks of 1000 days
            from_dt = datetime.strptime(from_date, "%Y-%m-%d")
            to_dt = datetime.strptime(to_date, "%Y-%m-%d")
            date_chunks = []
            chunk_start = from_dt
            
            while chunk_start < to_dt:
                chunk_end = min(chunk_start + timedelta(days=999), to_dt)
                date_chunks.append((
                    chunk_start.strftime("%Y-%m-%d"),
                    chunk_end.strftime("%Y-%m-%d")
                ))
                chunk_start = chunk_end + timedelta(days=1)

            self.logger.info(f'Processing {len(date_chunks)} chunks for historical data')
            combined_df = None

            for i, (chunk_from, chunk_to) in enumerate(date_chunks, 1):
                url = f'{self.BASE_URL}/historical-candle/{instrument_key}/{interval}/{chunk_to}/{chunk_from}'
                headers = {
                    'Accept': 'application/json'
                }
                params = {
                    'instrument_key': instrument_key,
                    'interval': interval,
                    'from_date': chunk_from,
                    'to_date': chunk_to
                }

                self.logger.debug(f'Processing chunk {i} of {len(date_chunks)} ({chunk_from} to {chunk_to})')
                async with aiohttp.ClientSession() as session:
                    async with session.get(url=url, headers=headers, params=params) as response:
                        if response.status == 200:
                            hist_response = await response.json()
                            if hist_response.get('status') == 'success':
                                if 'data' in hist_response:
                                    chunk_df = await self._convert_to_polars_df(
                                        data=hist_response['data'],
                                        exchange=exchange,
                                        exchange_token=exchange_token,
                                        instrument_type=instrument_type,
                                        interval=interval,
                                        from_date=chunk_from,
                                        to_date=chunk_to
                                    )
                                    if not chunk_df.is_empty():
                                        if combined_df is None:
                                            combined_df = chunk_df
                                        else:
                                            combined_df = pl.concat([combined_df, chunk_df])
                                else:
                                    self.logger.warning(f'No data for chunk {i} ({chunk_from} to {chunk_to})')
                            else:
                                self.logger.warning(f'Unsuccessful response for chunk {i}: {hist_response}')
                        else:
                            error_text = await response.text()
                            self.logger.warning(f'Failed to retrieve chunk {i}: {response.status} - {error_text}')

                # Rate limiting - wait 1 second between chunks
                if i < len(date_chunks):  # Don't wait after the last chunk
                    await asyncio.sleep(1)

            # Return sorted results if we have data
            if combined_df is not None and not combined_df.is_empty():
                self.logger.info(f'Successfully processed {len(date_chunks)} chunks')
                # Ensure only required columns in correct order
                combined_df = combined_df.select([
                    "datetime", "open", "high", "low", "close", "volume", "oi"
                ])
                combined_df = combined_df.with_columns([
                    pl.col("open").cast(pl.Float64),
                    pl.col("high").cast(pl.Float64),
                    pl.col("low").cast(pl.Float64),
                    pl.col("close").cast(pl.Float64),
                    pl.col("volume").cast(pl.Int64),
                    pl.col("oi").cast(pl.Int64)
                ]).sort('datetime')
                
                return combined_df.to_dicts()
            else:
                self.logger.warning(f'No historical data found for any chunk')
                return []

        except Exception as e:
            self.logger.error(f'Exception while retrieving historical data: {e}')  
            raise

    async def _convert_to_polars_df(
            self,
            data: dict,
            exchange_token: str,
            instrument_type: str,
            exchange: str,
            interval: str,
            from_date: str,
            to_date: str
            ) -> pl.DataFrame:
        """
        Converts provided candle data to a Polars DataFrame with a timezone-adjusted datetime column.
        
        Args:
            data (dict): Dictionary containing candle data with datetime and OHLC values.
            exchange_token (str): The exchange token of the instrument.
            instrument_type (str): Type of instrument (e.g., 'EQ', 'FUT').
            exchange (str): Exchange name (e.g., 'NSE', 'BSE').
            interval (str): The interval for the historical data (e.g., '1minute', '5minute', '1day').
            from_date (str): The start date for the historical data in 'YYYY-MM-DD' format.
            to_date (str): The end date for the historical data in 'YYYY-MM-DD' format.

        Returns:
            pl.DataFrame: Polars DataFrame with columns: datetime, open, high, low, close, volume, oi
        """
        candles = data.get('candles', [])
        if candles:
            data_dict = {
                "datetime": [item[0] for item in candles],
                "open": [item[1] for item in candles],
                "high": [item[2] for item in candles],
                "low": [item[3] for item in candles],
                "close": [item[4] for item in candles],
                "volume": [item[5] for item in candles],
                "oi": [item[6] for item in candles],
            }
            df = pl.DataFrame(data_dict)
            df = df.with_columns([
                pl.col("datetime")
                .str.strptime(pl.Datetime)
                .dt.convert_time_zone("Asia/Kolkata")
                .dt.strftime("%Y-%m-%d %H:%M:%S"),
                pl.col("open").cast(pl.Float64),
                pl.col("high").cast(pl.Float64),
                pl.col("low").cast(pl.Float64),
                pl.col("close").cast(pl.Float64),
                pl.col("volume").cast(pl.Int64),
                pl.col("oi").cast(pl.Int64)
            ])

            # Check if we need today's data
            if to_date == datetime.now().strftime("%Y-%m-%d"):
                today_date = datetime.now().strftime("%Y-%m-%d")
                has_todays_data = df.filter(
                    pl.col("datetime").str.contains(today_date)
                ).height > 0

                if not has_todays_data:
                    todays_mkt_quote = await self.full_market_quote(
                        exchange_token=exchange_token, 
                        exchange=exchange, 
                        instrument_type=instrument_type
                    )
                    if todays_mkt_quote.get('ohlc'):
                        if todays_mkt_quote.get('timestamp'):  # Validate timestamp exists
                            row = {
                                'datetime': todays_mkt_quote['timestamp'],
                                'open': todays_mkt_quote['ohlc']['open'],
                                'high': todays_mkt_quote['ohlc']['high'],
                                'low': todays_mkt_quote['ohlc']['low'],
                                'close': todays_mkt_quote['ohlc']['close'],
                                'volume': todays_mkt_quote['volume'],
                                'oi': todays_mkt_quote['oi'],
                            }

                            todays_df = pl.DataFrame(row)
                            todays_df = todays_df.with_columns([
                                pl.col('datetime')
                                .str.strptime(pl.Datetime)
                                .dt.convert_time_zone("Asia/Kolkata")
                                .dt.strftime("%Y-%m-%d %H:%M:%S"),
                                pl.col("open").cast(pl.Float64),
                                pl.col("high").cast(pl.Float64),
                                pl.col("low").cast(pl.Float64),
                                pl.col("close").cast(pl.Float64),
                                pl.col("volume").cast(pl.Int64),
                                pl.col("oi").cast(pl.Int64)
                            ])
                            
                            df = pl.concat([df, todays_df])
                        else:
                            self.logger.warning(f"Current day's timestamp missing for exchange token: {exchange_token}")
                    else:
                        self.logger.warning(f"Current day's full market quote not available for exchange token: {exchange_token}")
                else:
                    self.logger.debug(f"Current day's data already exists in historical data for exchange token: {exchange_token}")

            # Ensure only required columns in correct order
            df = df.select([
                "datetime", "open", "high", "low", "close", "volume", "oi"
            ]).sort('datetime')
            
            return df
        else:
            self.logger.warning(f"Historical data for exchange token: {exchange_token} from: {from_date} to: {to_date} at interval: {interval} not found.")
            return pl.DataFrame(schema={
                "datetime": str,
                "open": float,
                "high": float,
                "low": float,
                "close": float,
                "volume": int,
                "oi": int
            })

    async def fetch_access_token(self) -> str:
        """
        Fetch a new access token for the Upstox API.
        
        Returns:
            str: The new access token.
        
        Raises:
            Exception: If token fetching fails.
        """
        token_rotator = UpstoxTokenRotator(
            config=self.config,
            logger=self.logger
        )
        return token_rotator.get_current_token()
