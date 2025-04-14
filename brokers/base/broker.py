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
        instrument_keys = [
            "NSE_INDEX|Nifty 50", "NSE_INDEX|NIFTY100 EQL Wgt", "NSE_INDEX|NIFTY50 EQL Wgt", "NSE_INDEX|NiftyM150Momntm50",
            "NSE_INDEX|Nifty Auto", "NSE_INDEX|Nifty Commodities", "NSE_INDEX|Nifty Mid Liq 15", "NSE_INDEX|Nifty GS 10Yr Cln",
            "NSE_INDEX|NIFTY TOTAL MKT", "NSE_INDEX|Nifty Serv Sector", "NSE_INDEX|Nifty100 Liq 15", "NSE_INDEX|Nifty Bank",
            "NSE_INDEX|Nifty Next 50", "NSE_INDEX|NIFTY AlphaLowVol", "NSE_INDEX|Nifty Energy", "NSE_INDEX|Nifty Div Opps 50",
            "NSE_INDEX|NIFTY SMLCAP 50", "NSE_INDEX|Nifty PSE", "NSE_INDEX|NIFTY M150 QLTY50", "NSE_INDEX|NIFTY100 LowVol30",
            "NSE_INDEX|Nifty200Momentm30", "NSE_INDEX|Nifty100ESGSecLdr", "NSE_INDEX|Nifty GS 10Yr", "NSE_INDEX|NIFTY Alpha 50",
            "NSE_INDEX|Nifty 500", "NSE_INDEX|Nifty Realty", "NSE_INDEX|NIFTY INDIA MFG", "NSE_INDEX|NIFTY200 QUALTY30",
            "NSE_INDEX|Nifty GrowSect 15", "NSE_INDEX|NIFTY100 ESG", "NSE_INDEX|Nifty GS 8 13Yr", "NSE_INDEX|Nifty Infra",
            "NSE_INDEX|NIFTY SMLCAP 250", "NSE_INDEX|Nifty50 PR 1x Inv", "NSE_INDEX|NIFTY MIDCAP 100", "NSE_INDEX|Nifty FinSrv25 50",
            "NSE_INDEX|NIFTY CONSR DURBL", "NSE_INDEX|India VIX", "NSE_INDEX|Nifty Pharma", "NSE_INDEX|NIFTY MIDCAP 150",
            "NSE_INDEX|Nifty50 TR 1x Inv", "NSE_INDEX|Nifty PSU Bank", "NSE_INDEX|NIFTY HEALTHCARE", "NSE_INDEX|NIFTY500 MULTICAP",
            "NSE_INDEX|Nifty IT", "NSE_INDEX|NIFTY MIDSML 400", "NSE_INDEX|Nifty Media", "NSE_INDEX|Nifty 100",
            "NSE_INDEX|NIFTY100 Qualty30", "NSE_INDEX|NIFTY LARGEMID250", "NSE_INDEX|NIFTY SMLCAP 100", "NSE_INDEX|Nifty Midcap 50",
            "NSE_INDEX|NIFTY MICROCAP250", "NSE_INDEX|Nifty50 PR 2x Lev", "NSE_INDEX|Nifty200 Alpha 30", "NSE_INDEX|Nifty Fin Service",
            "NSE_INDEX|Nifty FMCG", "NSE_INDEX|Nifty50 Value 20", "NSE_INDEX|Nifty50 TR 2x Lev", "NSE_INDEX|Nifty50 Div Point",
            "NSE_INDEX|Nifty MNC", "NSE_INDEX|Nifty Consumption", "NSE_INDEX|Nifty Pvt Bank", "NSE_INDEX|Nifty CPSE",
            "NSE_INDEX|Nifty GS 11 15Yr", "NSE_INDEX|Nifty Metal", "NSE_INDEX|Nifty GS 15YrPlus", "NSE_INDEX|Nifty 200",
            "NSE_INDEX|NIFTY MID SELECT", "NSE_INDEX|Nifty GS Compsite", "NSE_INDEX|Nifty GS 4 8Yr", "NSE_INDEX|NIFTY IND DIGITAL",
            "NSE_INDEX|Nifty Tata 25 Cap", "NSE_INDEX|Nifty Multi Mfg", "NSE_INDEX|NIFTY OIL AND GAS", "NSE_INDEX|Nifty MidSml Hlth",
            "NSE_INDEX|Nifty Multi Infra", "MCX_INDEX|MCXBULLDEX", "BSE_INDEX|INDSTR", "BSE_INDEX|ALLCAP", "BSE_INDEX|SMLCAP",
            "BSE_INDEX|BSEFMC", "BSE_INDEX|REALTY", "BSE_INDEX|SMEIPO", "BSE_INDEX|LRGCAP", "BSE_INDEX|BSE500", "BSE_INDEX|FINSER",
            "BSE_INDEX|AUTO", "BSE_INDEX|BSECD", "BSE_INDEX|MFG", "BSE_INDEX|OILGAS", "BSE_INDEX|UTILS", "BSE_INDEX|GREENX",
            "BSE_INDEX|MIDSEL", "BSE_INDEX|BSEHC", "BSE_INDEX|ENERGY", "BSE_INDEX|SMLSEL", "BSE_INDEX|SENSEX", "BSE_INDEX|MIDCAP",
            "BSE_INDEX|CONDIS", "BSE_INDEX|COMDTY", "BSE_INDEX|BANKEX", "BSE_INDEX|CPSE", "BSE_INDEX|TECK", "BSE_INDEX|POWER",
            "BSE_INDEX|BSECG", "BSE_INDEX|CARBON", "BSE_INDEX|BSE200", "BSE_INDEX|BSE100", "BSE_INDEX|TELCOM", "BSE_INDEX|BSEIPO",
            "BSE_INDEX|BSEIT", "BSE_INDEX|INFRA", "BSE_INDEX|METAL", "BSE_INDEX|BSEPSU", "BSE_INDEX|SENSEX50"
        ]

        tradingsymbol_mappings = {
            'instrument_key': [],
            'trading_symbol': []
        }
        for key in instrument_keys:
            tradingsymbol_mappings['instrument_key'].append(key)
            tradingsymbol_mappings['trading_symbol'].append(key.split('|')[1].strip().upper())
        tradingsymbol_mappings_df = pl.DataFrame(tradingsymbol_mappings)

        instrument_link = 'https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz'

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(instrument_link) as response:
                    if response.status == 200:
                        compressed_data = BytesIO(await response.read())
                        with gzip.GzipFile(fileobj=compressed_data) as f:
                            data = json.load(f)
                            instrument_df = pl.DataFrame(data)
                            instrument_df = instrument_df.join(tradingsymbol_mappings_df, on='instrument_key', how='left')
                            instrument_df = instrument_df.with_columns(
                                pl.when(pl.col("trading_symbol") == '')
                                .then(pl.col("trading_symbol_right"))
                                .otherwise(pl.col("trading_symbol"))
                                .alias("trading_symbol")
                            ).drop("trading_symbol_right")
                            return instrument_df.to_dict()
                    else:
                        error_msg = f"Failed to retrieve Upstox instrument data. Status code: {response.status}"
                        raise Exception(error_msg)
        except Exception as e:
            raise Exception(f"Error fetching instrument data: {e}")  