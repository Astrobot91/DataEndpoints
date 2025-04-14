"""
Token rotation service module.

This module contains the TokenRotationService class that handles token rotation
for all brokers in a unified way.
"""

import time
import json
import logging
import asyncio
import aiohttp
from typing import Dict, Any, Optional, List

from brokers.factory import BrokerFactory
from brokers.base.broker import BaseBroker
from logger import get_logger


class TokenRotationService:
    """
    Service for managing token rotation across different brokers.
    
    This class provides a unified interface for monitoring token health
    and triggering token rotation when needed.
    
    Attributes:
        logger (logging.Logger): Logger instance for the service.
        brokers (Dict[str, Dict[str, Any]]): Configuration for different brokers.
        health_check_interval (int): Interval in seconds between health checks.
    """
    
    def __init__(self, brokers: Dict[str, Dict[str, Any]], health_check_interval: int = 5):
        """
        Initialize the token rotation service.
        
        Args:
            brokers (Dict[str, Dict[str, Any]]): Configuration for different brokers.
                Format: {broker_type: {account_name: account_config, ...}, ...}
            health_check_interval (int): Interval in seconds between health checks.
        """
        self.logger = get_logger(
            name="TokenRotationService",
            log_group="DataPipeline",
            log_stream="token_rotation"
        )
        self.brokers = brokers
        self.health_check_interval = health_check_interval
        self.broker_instances = {}
    
    async def initialize(self):
        """
        Initialize broker instances for token rotation.
        
        This method creates and initializes broker instances for all configured brokers.
        """
        self.logger.info("Initializing token rotation service")
        for broker_type, config in self.brokers.items():
            try:
                logger = get_logger(
                    name=f"{broker_type.capitalize()}Broker",
                    log_group="DataPipeline",
                    log_stream=f"broker"
                )
                broker = BrokerFactory.create_broker(
                    broker_type=broker_type,
                    config=config,
                    logger=logger
                )
                await broker.initialize()
                self.broker_instances[broker_type] = {}
                self.broker_instances[broker_type]["object"] = broker
                self.broker_instances[broker_type]["config"] = config
                self.logger.info(f"Initialized {broker_type} broker.")
            except Exception as e:
                self.logger.error(f"Failed to initialize {broker_type} broker: {e}")

    async def start(self):
        """
        Start the token rotation service.
        
        This method starts the health check loop that monitors token health
        and triggers rotation when needed.
        """
        self.logger.info("Starting token rotation service")
        
        await self.initialize()
        
        while True:
            try:
                await self.check_all_tokens()
                await asyncio.sleep(self.health_check_interval)
            except Exception as e:
                self.logger.error(f"Error in token rotation service: {e}")
                await asyncio.sleep(60)
    
    async def check_all_tokens(self):
        """
        Check the health of all tokens and rotate if needed.
        
        This method checks the health of tokens for all configured brokers
        and triggers rotation for any that are unhealthy.
        """
        self.logger.info("Checking health of all tokens")
        
        for broker_type, broker_dict in self.broker_instances.items():
            try:
                is_healthy = await self.check_token_health(broker_dict.get("object"))
                if not is_healthy:
                    self.logger.warning(f"Token for {broker_type} broker is unhealthy, rotating")
                    await self.rotate_token(broker_type)
                else:
                    self.logger.info(f"Token for {broker_type} broker is healthy")
            except Exception as e:
                self.logger.error(f"Error checking token health for {broker_type} broker: {e}")

    async def check_token_health(self, broker: BaseBroker) -> bool:
        """
        Check the health of a token by making a test API call.
        
        Args:
            broker: The broker instance to check.
            
        Returns:
            bool: True if token is healthy, False otherwise.
        """
        try:
            # Make a lightweight API call to check token health
            # This is a simplified implementation
            # In a real environment, you would use a specific endpoint for health checks
            test_instruments = [{"exchange_token": "26000", "exchange": "NSE", "instrument_type": "INDEX"}]
            await broker.ltp_quote(test_instruments)
            return True
        except Exception as e:
            self.logger.error(f"Token health check failed: {e}")
            return False
    
    async def rotate_token(self, broker_type: str) -> bool:
        """
        Rotate the token for a specific broker account.
        
        Args:
            broker_type (str): The type of broker.
            
        Returns:
            bool: True if rotation was successful, False otherwise.
        """
        self.logger.info(f"Rotating token for {broker_type} broker.")
        
        try:
            # Get the broker instance
            broker_dict = self.broker_instances[broker_type]
            broker_config = broker_dict.get("config")
            broker_object = broker_dict.get("object")
            
            # Create a token rotator for the broker
            logger = get_logger(
                name=f"{broker_type.capitalize()}TokenRotator",
                log_group="DataPipeline",
                log_stream=f"token_rotator"
            )
            
            # This assumes each broker module has a TokenRotator class
            # In a real implementation, you would use a factory pattern here as well
            token_rotator_class = __import__(f"brokers.{broker_type}.token_rotator", fromlist=[f"{broker_type.capitalize()}TokenRotator"])
            token_rotator = getattr(token_rotator_class, f"{broker_type.capitalize()}TokenRotator")(
                config=broker_config,
                logger=logger
            )
            
            # Rotate the token
            result = token_rotator.rotate()
            
            if result.get("statusCode") == 200:
                self.logger.info(f"Token rotation successful for {broker_type} broker.")
                
                # Reinitialize the broker with the new token
                await broker_object.initialize()
                return True
            else:
                self.logger.error(f"Token rotation failed for {broker_type} broker: {result.get('body')}")
                return False
        except Exception as e:
            self.logger.error(f"Error rotating token for {broker_type} broker: {e}")
            return False
