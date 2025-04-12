import json
import boto3
import gzip
import logging
import aiohttp
import polars as pl
from io import BytesIO
from datetime import datetime, timedelta
from brokers.basebroker import BaseBroker
from typing import List, Dict, Any


class UpstoxBroker(BaseBroker):
    
    BASE_URL = "https://api.upstox.com/v2"
    BASE_ORDER_URL = "https://api-hft.upstox.com/v2"

    def __init__(self, account_name: str, logger: logging.Logger):
        self.account_name = account_name
        self.broker_name = 'Upstox'
        self.logger = logger
        self.access_token = None

    async def initialize(self):
        try:
            self.logger.info(f'Initializing UpstoxBroker for {self.account_name}')
            self.access_token = self.fetch_upstox_access_token()
            self.upstox_master_data = await self._get_upstox_master_data()
            self.upstox_master_df = pl.DataFrame(data=self.upstox_master_data)
            if self.upstox_master_df is None:
                raise Exception("Instrument data could not be loaded.")
        except Exception as e:
            self.logger.error(f"Initialization failed: {e}")
            raise

    async def _get_upstox_master_data(self):
        return await super()._get_upstox_master_data()

    async def ltp_quote(self, ltp_request_data: List[Dict[str, str]]) -> float:
        """
        Asynchronously retrieves the Last Traded Price (LTP) for a specified exchange token.

        This function fetches the LTP for a given exchange token from Upstox's market quote API. 
        It verifies that the token exists in the local instrument data, then retrieves the price data.

        Args:
            exchange_token (str): The unique identifier for the instrument on the exchange.
            exchange (str): Name of the exchange.

        Returns:
            float: The last traded price of the instrument associated with the given exchange token.

        Raises:
            ValueError: If the exchange token is not found in the instrument data.
            Exception: If the API request fails or if LTP retrieval is unsuccessful due to server issues or invalid responses.

        Example:
            ltp_request_data = [
                    {
                        "exchange_token": "55555",
                        "exchange": "NSE"
                    },
                    {
                        "exchange_token": "44444",
                        "exchange": "BSE"
                    }
                ]

        Notes:
            - Requires `instrument_df` to contain the exchange token and `instrument_key` mappings.
            - Requires a valid Upstox API access token and active session to function.
        """
        try:
            instrument_key_list = []
            for data in ltp_request_data:
                exchange_token = data.get("exchange_token", "")
                exchange = data.get("exchange", "NSE") 
                instrument_type = data.get("instrument_type", "")

                instrument_rows = self.upstox_master_df.filter(
                    (pl.col('exchange_token') == exchange_token),
                    (pl.col('exchange') == exchange),
                    (pl.col('instrument_type') == instrument_type)   
                )
                if instrument_rows.is_empty():
                    error_msg = f'exchange_token: {exchange_token} not found in the upstox master file.'
                    self.logger.error(error_msg)
                    raise ValueError(error_msg)
                instrument_key = instrument_rows['instrument_key'][0]
                instrument_key_list.append(instrument_key)
            
            main_instrument_key = ",".join(instrument_key_list)

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
                                data = await self.convert_quote(ltp_response_data=ltp_response['data'])
                                return data
                            else:
                                error_msg = f'LTP response data missing for: {params}'
                                self.logger.error(error_msg)
                                raise ValueError
                        else:
                            error_msg = f'LTP response retrieval unsuccessful. Details: {ltp_response}'
                            self.logger.error(error_msg)
                            raise Exception(error_msg)
                    else:
                        error_text = await response.text()
                        error_msg = f'Failed to retrieve LTP response: {response.status} - {error_text}'
                        self.logger.error(error_msg)
                        raise Exception(error_msg)
        except Exception as e:
            self.logger.error(f'Exception during LTP response retrieval: {e}')

    async def convert_quote(self, ltp_response_data: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """
        Converts instrument tokens in the quote data to exchange tokens.

        This function takes a dictionary of quote data where the keys are instrument tokens,
        and converts the instrument tokens to their corresponding exchange tokens using the
        instrument DataFrame (`self.instrument_df`). It returns a new dictionary with exchange
        tokens as keys and the original quote data as values.

        Args:
            ltp_response_data (dict): A dictionary where each value contains quote data with an 'instrument_token' key.

        Returns:
            dict: A dictionary with trading symbols as keys and quote data as values.

        Raises:
            Exception: If an error occurs during the conversion process.
        """
        output_dict = {}
        for ltp_data, value in ltp_response_data.items():
            instrument_key = value['instrument_token']
            if instrument_key is None:
                self.logger.error(f"Missing 'instrument_token' in quote data for key {ltp_data}")
                continue
            try:
                temp_df = self.upstox_master_df.filter(
                    pl.col('instrument_key') == instrument_key
                )
                value["trading_symbol"] = temp_df["trading_symbol"][0]
                value["instrument_type"] = temp_df["instrument_type"][0]
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
            ) -> Dict:
        """
        Retrieves historical candle data for a specified exchange token from Upstox.

        Args:
            exchange_token (str): The unique identifier for the instrument within the Upstox API.
            interval (str): The time interval for the data (e.g., '1minute', '5minute', '15minute', '1day').
            from_date (str): The starting date for the historical data in 'YYYY-MM-DD' format.
            to_date (str): The ending date for the historical data in 'YYYY-MM-DD' format.

        Returns:
            Dict: A dictionary containing historical candle data with keys such as:
                - `datetime`: The timestamp for each data point.
                - `open`: The opening price of the instrument.
                - `high`: The highest price of the instrument.
                - `low`: The lowest price of the instrument.
                - `close`: The closing price of the instrument.
                - `volume`: The trading volume.
                - `oi`: The open interest.

        Raises:
            ValueError: If the `exchange_token` is not found in the Upstox master data.
            Exception: For any errors related to data retrieval or API response issues.

        Example:
            historical_df = await upstox_broker.historical_data(
                exchange="NSE"
                exchange_token="12345", 
                interval="1day", 
                from_date="2023-01-01", 
                to_date="2023-06-30"
            )

            This example retrieves daily OHLC data for the instrument with `exchange_token` "12345" from 
            January 1, 2023, to June 30, 2023, and prints the resulting DataFrame.
        """
        try:
            instrument_rows = self.upstox_master_df.filter(
                (pl.col('exchange_token') == exchange_token),
                (pl.col('exchange') == exchange),
                (pl.col('instrument_type') == instrument_type)
            )
            if instrument_rows.is_empty():
                error_msg = f"exchange_token: {exchange_token} not found in upstox master file."
                self.logger.error(error_msg)
                raise ValueError(error_msg)
            instrument_key = instrument_rows['instrument_key'][0]  

            url = f'{self.BASE_URL}/historical-candle/{instrument_key}/{interval}/{to_date}/{from_date}'
            headers = {
                'Accept': 'application/json'
            }
            params = {
                'instrument_key': instrument_key,
                'interval': interval,
                'from_date': from_date,
                'to_date': to_date
            }
            async with aiohttp.ClientSession() as session:
                async with session.get(url=url, headers=headers, params=params) as response:
                    if response.status == 200:
                        hist_response = await response.json()
                        if hist_response.get('status') == 'success':
                            if 'data' in hist_response:
                                data = await self._convert_to_polars_df(data=hist_response['data'],
                                                                        exchange=exchange,
                                                                        exchange_token=exchange_token,
                                                                        instrument_type=instrument_type,
                                                                        interval=interval,
                                                                        from_date=from_date,
                                                                        to_date=to_date)
                                self.logger.info(f'Historical data for: {exchange_token} from: {from_date} to: {to_date} retrieved.')
                                return data.to_dicts()
                            else:
                                error_msg = f'Response data missing for historical data: {params}'
                                self.logger.error(error_msg)
                                raise ValueError(error_msg)

                        else:
                            error_msg = f'Retrieving historical data unsuccessful. Details: {hist_response}'
                            self.logger.error(error_msg)
                            raise Exception(error_msg)
                    else:
                        error_text = await response.text()
                        error_msg = f'Failed to retrieve historical data: {response.status} - {error_text}'
                        self.logger.error(error_msg)
                        return {}
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
            exchange_token (str): The exchange token of the instrument.
            data (dict): Dictionary containing candle data with datetime and OHLC values.
            interval (str): The interval for the historical data (e.g., '1minute', '5minute', '1day').
            from_date (str): The start date for the historical data in 'YYYY-MM-DD' format.
            to_date (str): The end date for the historical data in 'YYYY-MM-DD' format.

        Returns:
            pl.DataFrame: Polars DataFrame with adjusted datetime column.
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
            df = pl.DataFrame(data_dict, strict=False)
            df = df.with_columns(
                pl.col("datetime")
                .str.strptime(pl.Datetime)
                .dt.convert_time_zone("Asia/Kolkata")
                .dt.strftime("%Y-%m-%d %H:%M:%S")
            )
            todays_mkt_quote = await self.full_market_quote(exchange_token=exchange_token, exchange=exchange, instrument_type=instrument_type)
            if todays_mkt_quote['ohlc']:
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
                todays_df = todays_df.with_columns(
                    pl.col('datetime')
                    .str.strptime(pl.Datetime)
                    .dt.convert_time_zone("Asia/Kolkata")
                    .dt.strftime("%Y-%m-%d %H:%M:%S")
                )

                todays_df = todays_df.with_columns([
                    pl.col("open").cast(pl.Float64),
                    pl.col("high").cast(pl.Float64),
                    pl.col("low").cast(pl.Float64),
                    pl.col("close").cast(pl.Float64),
                    pl.col("volume").cast(pl.Int64),
                    pl.col("oi").cast(pl.Int64)
                ])

                combined_df = pl.concat([df, todays_df])
                combined_df = combined_df.sort('datetime')
                return combined_df
            else:
                self.logger.warning(f"Current day's full market quote not added for exchange token: {exchange_token}.")
                return df
        else:
            self.logger.warning(f"Historical data for exchange token: {exchange_token} from: {from_date} to: {to_date} at interval: {interval} not found.")
            return pl.DataFrame(data={}, strict=False)

    async def ohlc_quote(
            self,
            exchange: str,
            exchange_token: str,
            instrument_type: str,
            interval: str
            ) -> Dict[str, float]:
        """
        Asynchronously retrieves Open-High-Low-Close (OHLC) data for a specified exchange token and time interval.

        This function fetches the OHLC data for a given exchange token from Upstox's market quote API,
        for a specified time interval such as '1day' or '5min'. It verifies that the token exists in 
        the local instrument data before making the request.

        Args:
            exchange_token (str): The unique identifier for the instrument on the exchange.
            interval (str): The time interval for OHLC data (e.g., '1d' (1 day), '1I' (1 minute), '5I' (5 minutes)).
            exchange (str): Name of the exchange.

        Returns:
            Dict[str, float]: A dictionary containing the OHLC data, with keys 'open', 'high', 'low', and 'close' 
                              representing respective prices.

        Raises:
            ValueError: If the exchange token is not found in the instrument data.
            Exception: If the API request fails or if OHLC data retrieval is unsuccessful due to server issues or invalid responses.
        
        Example:
            exchange_token = '74989'
            interval = '1day'
            ohlc_data = await broker.ohlc_quote(exchange_token, interval)
            print(f"OHLC data: {ohlc_data}")
        """
        try:
            instrument_rows = self.upstox_master_df.filter(
                (pl.col('exchange_token') == exchange_token),
                (pl.col('exchange') == exchange),
                (pl.col('instrument_type') == instrument_type)
            )
            if instrument_rows.is_empty():
                error_msg = f'exchange_token: {exchange_token} not found in the upstox master file.'
                self.logger.error(error_msg)
                raise ValueError(error_msg)
            instrument_key = instrument_rows['instrument_key'][0]

            url = f'{self.BASE_URL}/market-quote/ohlc'
            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Accept': 'application/json'
            }
            params = {'instrument_key': instrument_key, 'interval': interval}

            async with aiohttp.ClientSession() as session:
                async with session.get(url=url, headers=headers, params=params) as response:
                    if response.status == 200:
                        ohlc_response = await response.json()
                        if ohlc_response.get('status') == 'success':
                            if 'data' in ohlc_response:
                                data = await self.convert_quote(exchange_tokens=ohlc_response['data'])
                                return data[exchange_token]['ohlc']
                            else:
                                error_msg = f'OHLC response data missing for: {params}'
                                self.logger.error(error_msg)
                                raise ValueError(error_msg)
                        else:
                            error_msg = f'OHLC response retrieval unsuccessful. Details: {ohlc_response}'
                            self.logger.error(error_msg)
                            raise Exception(error_msg)
                    else:
                        error_text = await response.text()
                        error_msg = f'Failed to retrieve OHLC response: {response.status} - {error_text}'
                        self.logger.error(error_msg)
                        raise Exception(error_msg)         
        except Exception as e:
            self.logger.error(f'Exception during OHLC response retrieval: {e}')      

    async def full_market_quote(self, exchange: str, exchange_token: str, instrument_type: str) -> Dict[str, Any]:
        """
        Asynchronously retrieves the full market quote for a specified exchange token.

        This function fetches comprehensive market data for a given exchange token from Upstox's market 
        quote API. It verifies the existence of the exchange token in the instrument data before making 
        the request, ensuring that only valid tokens are processed.

        Args:
            exchange_token (str): The unique identifier for the instrument on the exchange.
            exchange (str): Name of the exchange.

        Returns:
            Dict[str, Any]: A dictionary containing the full market quote data for the specified 
                            exchange token, including details such as bid-ask prices, 
                            last traded price, volume, and other key metrics.

        Raises:
            ValueError: If the exchange token is not found in the instrument data.
            Exception: If the API request fails or if full market quote retrieval is unsuccessful 
                       due to server issues or invalid responses.

        Example:
            exchange_token = '74989'
        """
        try:
            instrument_rows = self.upstox_master_df.filter(
                (pl.col('exchange_token') == exchange_token),
                (pl.col('exchange') == exchange),
                (pl.col('instrument_type') == instrument_type)
            )
            if instrument_rows.is_empty():
                error_msg = f'exchange_token: {exchange_token} not found in the upstox master file.'
                self.logger.error(error_msg)
                raise ValueError(error_msg)
            instrument_key = instrument_rows['instrument_key'][0]

            url = f'{self.BASE_URL}/market-quote/quotes'
            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Accept': 'application/json'
            }
            params = {'instrument_key': instrument_key}

            async with aiohttp.ClientSession() as session:
                async with session.get(url=url, headers=headers, params=params) as response:
                    if response.status == 200:
                        quote_response = await response.json()
                        if quote_response.get('status') == 'success':
                            if 'data' in quote_response:
                                data = await self.convert_quote(ltp_response_data=quote_response['data'])
                                return data[exchange_token]
                            else:
                                error_msg = f'Full market quote data missing for: {params}'
                                self.logger.error(error_msg)
                                raise ValueError
                        else:
                            error_msg = f'Full market quote retrieval unsuccessful. Details: {quote_response}'
                            self.logger.error(error_msg)
                            raise Exception(error_msg)
                    else:
                        error_text = await response.text()
                        error_msg = f'Failed to retrieve full market quote: {response.status} - {error_text}'
                        self.logger.error(error_msg)
                        raise Exception(error_msg)         
        except Exception as e:
            self.logger.error(f'Exception during full market quote retrieval: {e}')      

    def fetch_upstox_access_token(self, region_name: str = "ap-south-1") -> str:
        """
        Fetches the Upstox access token from AWS Secrets Manager.

        Args:
            region_name (str): AWS region where the secret is stored. Default is 'ap-south-1'.

        Returns:
            str: The Upstox access token.

        Raises:
            Exception: If unable to fetch or parse the secret.
        """
        secret_name = "my_upstox_access_token"

        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )

        try:
            get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        except Exception as e:
            raise Exception(f"Error fetching secret {secret_name}: {e}")

        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            secret = get_secret_value_response['SecretBinary'].decode('utf-8')

        try:
            secret_data = json.loads(secret)
            access_token = secret_data.get("access_token")
            if not access_token:
                raise Exception("Key 'access_token' not found in secret JSON.")
            return access_token
        except json.JSONDecodeError:
            return secret
