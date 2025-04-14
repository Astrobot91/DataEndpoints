import json
import gzip
import aiohttp
import polars as pl
from io import BytesIO
from typing import Dict, Any, List
from logger import get_logger
from abc import ABC, abstractmethod


class BaseBroker(ABC):
    """
    BaseBroker: Abstract base class for all brokers.
    Shared functionality and common interface for broker implementations.
    """
    def __init__(self, account_name: str) -> None:
        self.account_name = account_name
        self.logger = get_logger(
            name="BaseBroker",
            log_group="DataPipeline",
            log_stream="broker"
        )

    @abstractmethod
    async def initialize(self):
        """Initialize the broker-specific configuration or connection."""
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

    # @abstractmethod
    # async def place_order(self, **kwargs) -> Dict[str, Any]:
    #     """Place an order."""
    #     pass

    # @abstractmethod
    # async def cancel_order(self, order_id: str) -> Dict[str, Any]:
    #     """Cancel an existing order."""
    #     pass

    # @abstractmethod
    # async def modify_order(self, **kwargs) -> Dict[str, Any]:
    #     """Modify an existing order."""
    #     pass

    # @abstractmethod
    # async def get_order_details(self, order_id: str) -> Dict[str, Any]:
    #     """Get details of an order."""
    #     pass

    # @abstractmethod
    # async def get_orderbook(self) -> List[Dict[str, Any]]:
    #     """Get orderbook"""
    #     pass

    # @abstractmethod
    # async def historical_data(self, **kwargs) -> Dict:
    #     """Fetch historical data."""
    #     pass

    # @abstractmethod
    # async def get_funds_and_margin(self, segment: str = None) -> float:
    #     """Fetch available balance in the account"""
    #     pass

    @abstractmethod
    async def ltp_quote(self, exchange: str, exchange_token: str) -> float:
        """Fetch LTP quote."""
        pass

    # @abstractmethod
    # async def ohlc_quote(self, exchange: str, exchange_token: str, interval: str) -> Dict:
    #     """Fetch OHLC quote."""
    #     pass

    # @abstractmethod
    # async def full_market_quote(self, exchange: str, exchange_token: str) -> Dict:
    #     """Fetch full market quote."""
    #     pass
    
    # @abstractmethod
    # async def calculate_margin(self, instrument_dict: Dict[str, Any]) -> float:
    #     """Fetch required margin for the trade"""
    #     pass

    # @abstractmethod
    # async def calculate_brokerage(self, instrument_dict: Dict[str, Any]) -> float:
    #     """Fetch required brokerage for the trade"""
    #     pass

    # @abstractmethod
    # async def market_holidays(self) -> Dict:
    #     """Fetch market holidays"""
    #     pass
