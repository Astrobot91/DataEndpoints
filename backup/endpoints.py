from fastapi import APIRouter, Depends, HTTPException
from typing import List

from brokers.upstox.broker import UpstoxBroker
from logger import get_logger

router = APIRouter()


async def get_upstox_broker(account_name: str = "2HCB67"):
    """Dependency to get initialized UpstoxBroker instance."""
    logger = get_logger(
        name="UpstoxBroker",
        log_group="DataPipeline",
        log_stream="broker",
    )
    broker = UpstoxBroker(account_name=account_name, logger=logger)
    await broker.initialize()
    return broker


@router.get("/master-data")
async def get_master_data(broker: UpstoxBroker = Depends(get_upstox_broker)):
    """Get the Upstox master data."""
    try:
        master_data = broker.upstox_master_df.to_dicts()
        return {"status": "success", "data": master_data}
    except Exception as err:
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving master data: {str(err)}"
        )


@router.post("/ltp-quote")
async def get_ltp_quote(
    instruments: List[dict],
    broker: UpstoxBroker = Depends(get_upstox_broker)
):
    """
    Get Last Traded Price for multiple instruments.

    Request Body Example:
    [
        {"exchange_token": "21195", "exchange": "NSE"},
        {"exchange_token": "9305", "exchange": "NSE"}
    ]
    """
    try:
        ltp_data = await broker.ltp_quote(ltp_request_data=instruments)
        if not ltp_data:
            raise HTTPException(
                status_code=404,
                detail="No data found for the provided instruments."
            )
        return {"status": "success", "data": ltp_data}
    except Exception as err:
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching LTP data: {str(err)}"
        )


@router.post("/historical-data")
async def historical_data(
    instrument: dict, broker: UpstoxBroker = Depends(get_upstox_broker)
):
    """
    Get historical data for an instrument.
    """
    exchange = instrument.get("exchange", "NSE")
    exchange_token = instrument.get("exchange_token")
    interval = instrument.get("interval")
    from_date = instrument.get("from_date")
    to_date = instrument.get("to_date")
    instrument_type = instrument.get("instrument_type")

    try:
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
    except Exception as err:
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching historical data: {str(err)}"
        )
