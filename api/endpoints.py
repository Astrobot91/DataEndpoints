"""
API endpoints module for broker-agnostic data access.

This module defines FastAPI endpoints that provide a unified interface
for accessing data from different brokers.
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Dict, Any, Optional

from brokers.factory import BrokerFactory
from brokers.base.broker import BaseBroker
from logger import get_logger

router = APIRouter()


async def get_broker(broker_type: str = Query(..., description="Broker type (e.g., 'upstox', 'zerodha')")):
    """
    Dependency to get initialized broker instance based on type.
    
    Args:
        broker_type (str): The type of broker to use.
        account_name (str): The account name or ID for the broker.
        
    Returns:
        BaseBroker: An initialized broker instance.
        
    Raises:
        HTTPException: If broker initialization fails.
    """
    try:
        logger = get_logger(
            name=f"{broker_type.capitalize()}Broker",
            log_group="DataPipeline",
            log_stream="broker",
        )
        broker = BrokerFactory.create_broker(
            broker_type=broker_type,
            logger=logger
        )
        await broker.initialize()
        return broker
    except Exception as err:
        raise HTTPException(
            status_code=500,
            detail=f"Error initializing {broker_type} broker: {str(err)}"
        )


@router.get("/master-data")
async def get_master_data(broker=Depends(get_broker)):
    """
    Get the broker's master data.
    
    Args:
        broker: The broker instance from the dependency.
        
    Returns:
        Dict: Response containing master data.
        
    Raises:
        HTTPException: If data retrieval fails.
    """
    try:
        # This assumes all brokers have a similar master_df attribute
        # In a real implementation, you might need broker-specific handling
        master_data = broker.master_df.to_dicts()
        return {"status": "success", "data": master_data}
    except Exception as err:
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving master data: {str(err)}"
        )


@router.post("/ltp-quote")
async def get_ltp_quote(
    instruments: List[Dict[str, str]],
    broker=Depends(get_broker)
):
    """
    Get Last Traded Price for multiple instruments.
    
    Args:
        instruments: List of instrument identifiers.
        broker: The broker instance from the dependency.
        
    Returns:
        Dict: Response containing LTP data.
        
    Raises:
        HTTPException: If data retrieval fails.
    
    Example Request Body:
    ```json
    [
        {"exchange_token": "21195", "exchange": "NSE", "instrument_type": "EQ"},
        {"exchange_token": "9305", "exchange": "NSE", "instrument_type": "FUTIDX"},
    ]
    ```
    """
    try:
        ltp_data = await broker.ltp_quote(ltp_request_data=instruments)
        if not ltp_data:
            raise HTTPException(
                status_code=404,
                detail="No data found for the provided instruments."
            )
        return {"status": "success", "data": ltp_data}
    except ValueError as err:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid request: {str(err)}"
        )
    except Exception as err:
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching LTP data: {str(err)}"
        )


@router.post("/historical-data")
async def historical_data(
    instrument: Dict[str, str],
    broker=Depends(get_broker)
):
    """
    Get historical data for an instrument.
    
    Args:
        instrument: Instrument identifier and parameters.
        broker: The broker instance from the dependency.
        
    Returns:
        Dict: Response containing historical data.
        
    Raises:
        HTTPException: If data retrieval fails.
    
    Example Request Body:
    ```json
    {
        "exchange": "NSE",
        "exchange_token": "21195",
        "instrument_type": "EQ",
        "interval": "1day",
        "from_date": "2023-01-01",
        "to_date": "2023-01-31"
    }
    ```
    """
    try:
        exchange = instrument.get("exchange", "NSE")
        exchange_token = instrument.get("exchange_token")
        interval = instrument.get("interval")
        from_date = instrument.get("from_date")
        to_date = instrument.get("to_date")
        instrument_type = instrument.get("instrument_type")

        if not all([exchange_token, interval, from_date, to_date, instrument_type]):
            raise ValueError("Missing required parameters")

        hist_data = await broker.historical_data(
            exchange=exchange,
            exchange_token=exchange_token,
            interval=interval,
            from_date=from_date,
            to_date=to_date,
            instrument_type=instrument_type,
        )
        if not hist_data:
            raise HTTPException(
                status_code=404,
                detail="No historical data found for the provided instrument."
            )
        return {"status": "success", "data": hist_data}
    except ValueError as err:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid request: {str(err)}"
        )
    except Exception as err:
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching historical data: {str(err)}"
        )
