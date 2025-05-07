"""
Base broker module defining the abstract interface for all broker implementations.

This module contains the BaseBroker abstract base class that defines the common
interface that all broker implementations must adhere to.
"""

import abc
import json
import gzip
import aiohttp
import logging
from io import BytesIO
import polars as pl
from typing import Dict, List, Any, Optional


class BaseBroker(abc.ABC):
    """
    Abstract base class for all broker implementations.
    
    This class defines the common interface that all broker implementations
    must implement to ensure consistency across different brokers.
    
    Attributes:
        broker_name (str): The name of the broker (e.g., 'Upstox', 'Zerodha').
        logger (logging.Logger): Logger instance for the broker.
        config (Dict[str, Any]): Configuration dictionary for the broker.
    """
    
    def __init__(self, config: Dict[str, Any], logger: logging.Logger):
        """
        Initialize the broker with account information and logger.
        
        Args:
            config (Dict[str, Any]): Configuration dictionary for the broker.
            logger (logging.Logger): Logger instance for the broker.
        """
        self.broker_name = self._get_broker_name()
        self.logger = logger
        self.config = config
        self.access_token = None
        
    @abc.abstractmethod
    def _get_broker_name(self) -> str:
        """
        Get the name of the broker.
        
        Returns:
            str: The name of the broker.
        """
        pass
    
    @abc.abstractmethod
    async def initialize(self) -> None:
        """
        Initialize the broker with necessary configurations and data.
        
        This method should be called before using any other methods of the broker.
        It should set up any required connections, fetch initial data, and
        ensure the broker is ready for use.
        
        Raises:
            Exception: If initialization fails.
        """
        pass
    
    @abc.abstractmethod
    async def fetch_access_token(self) -> str:
        """
        Fetch a new access token for the broker API.
        
        Returns:
            str: The new access token.
            
        Raises:
            Exception: If token fetching fails.
        """
        pass
    
    @abc.abstractmethod
    async def ltp_quote(self, ltp_request_data: List[Dict[str, str]]) -> Dict[str, Any]:
        """
        Get last traded price quotes for specified instruments.
        
        Args:
            ltp_request_data (List[Dict[str, str]]): List of dictionaries containing
                instrument identifiers like exchange_token, exchange, etc.
                
        Returns:
            Dict[str, Any]: Dictionary containing LTP data for requested instruments.
            
        Raises:
            ValueError: If instrument identifiers are invalid.
            Exception: If quote retrieval fails.
        """
        pass
    
    @abc.abstractmethod
    async def historical_data(
        self,
        exchange: str,
        exchange_token: str,
        instrument_type: str,
        interval: str,
        from_date: str,
        to_date: str
    ) -> Dict[str, Any]:
        """
        Get historical candle data for a specified instrument.
        
        Args:
            exchange (str): Exchange name (e.g., 'NSE', 'BSE').
            exchange_token (str): Exchange token for the instrument.
            instrument_type (str): Type of instrument (e.g., 'EQ', 'FUT').
            interval (str): Time interval for candles (e.g., '1minute', '1day').
            from_date (str): Start date in 'YYYY-MM-DD' format.
            to_date (str): End date in 'YYYY-MM-DD' format.
            
        Returns:
            Dict[str, Any]: Dictionary containing historical candle data.
            
        Raises:
            ValueError: If parameters are invalid.
            Exception: If data retrieval fails.
        """
        pass
    
    @abc.abstractmethod
    async def full_market_quote(
        self,
        exchange_token: str,
        exchange: str,
        instrument_type: str
    ) -> Dict[str, Any]:
        """
        Get full market quote for a specified instrument.
        
        Args:
            exchange_token (str): Exchange token for the instrument.
            exchange (str): Exchange name (e.g., 'NSE', 'BSE').
            instrument_type (str): Type of instrument (e.g., 'EQ', 'FUT').
            
        Returns:
            Dict[str, Any]: Dictionary containing full market quote data.
            
        Raises:
            ValueError: If parameters are invalid.
            Exception: If quote retrieval fails.
        """
        pass

    async def _get_upstox_master_data(self) -> Dict:
        '''
        Mapping Index tradingsymbols into the upstox master file.
        Add the 'instrument_key' with it's respective 'tradingsymbol'

        '''
        instrument_link = 'https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz'

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(instrument_link) as response:
                    response.raise_for_status()
                    compressed_data = BytesIO(await response.read())
                    if response.status == 200:
                        with gzip.GzipFile(fileobj=compressed_data) as f:
                            instrument_df = pl.read_csv(f)
                            return instrument_df.to_dict()
                    else:
                        error_msg = f"Failed to retrieve Upstox instrument data. Status code: {response.status}"
                        raise Exception(error_msg)
        except Exception as e:
            raise Exception(f"Error fetching instrument data: {e}")