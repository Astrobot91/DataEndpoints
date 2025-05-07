"""
Main application module for the broker-agnostic data endpoints system.

This module sets up the FastAPI application with all necessary routes
and middleware for the broker-agnostic data endpoints system.
"""

import os
import json
import boto3
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.endpoints import router as api_router
from services.token_rotation_service import TokenRotationService
from logger import get_logger


# Create logger
logger = get_logger(
    name="DataEndpointsApp",
    log_group="DataPipeline",
    log_stream="app"
)

# Create FastAPI app
app = FastAPI(
    title="Broker-Agnostic Data Endpoints",
    description="A unified API for accessing market data from multiple brokers",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API router
app.include_router(api_router, prefix="/api/v1", tags=["data"])

# Broker configuration
UPSTOX_CONFIG_SECRET_NAME = os.getenv("UPSTOX_CONFIG_SECRET_NAME", "my_upstox_config")

secrets_client = boto3.client("secretsmanager")

upstox_config_secret = secrets_client.get_secret_value(SecretId=UPSTOX_CONFIG_SECRET_NAME)
upstox_config_json = json.loads(upstox_config_secret["SecretString"])

broker_config = {
    "upstox": upstox_config_json
    # Add other brokers as needed
    # "zerodha": zerodha_config_json,
    # "dhan": dhan_config_json
}

# Token rotation service
token_rotation_service = None

@app.on_event("startup")
async def startup_event():
    """
    Initialize services on application startup.
    
    This function is called when the FastAPI application starts up.
    It initializes the token rotation service and starts it in a background task.
    """
    global token_rotation_service
    
    logger.info("Starting application")
    
    # Initialize token rotation service
    token_rotation_service = TokenRotationService(
        brokers=broker_config,
        health_check_interval=300  # 5 seconds
    )
    
    # Start token rotation service in background
    import asyncio
    asyncio.create_task(token_rotation_service.start())
    
    logger.info("Application startup complete")

@app.on_event("shutdown")
async def shutdown_event():
    """
    Clean up resources on application shutdown.
    
    This function is called when the FastAPI application shuts down.
    It performs any necessary cleanup operations.
    """
    logger.info("Shutting down application")
    
    # Add any cleanup operations here
    
    logger.info("Application shutdown complete")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
    logger.info("Running application with Uvicorn")
