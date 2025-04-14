"""
Factory module for creating broker instances.

This module contains the BrokerFactory class that creates appropriate broker
instances based on configuration.
"""

import logging
from typing import Dict, Any, Optional, Type

from .base import BaseBroker


class BrokerFactory:
    """
    Factory class for creating broker instances.
    
    This class is responsible for creating the appropriate broker instance
    based on the broker type specified in the configuration.
    """
    
    _broker_registry = {}
    
    @classmethod
    def register_broker(cls, broker_type: str, broker_class: Type[BaseBroker]) -> None:
        """
        Register a broker class with the factory.
        
        Args:
            broker_type (str): The type identifier for the broker.
            broker_class (Type[BaseBroker]): The broker class to register.
        """
        cls._broker_registry[broker_type.lower()] = broker_class
    
    @classmethod
    def create_broker(cls, broker_type: str, config: Dict[str, Any], logger: logging.Logger) -> BaseBroker:
        """
        Create a broker instance of the specified type.
        
        Args:
            broker_type (str): The type of broker to create (e.g., 'upstox', 'zerodha').
            account_name (str): The account name or ID for the broker.
            logger (logging.Logger): Logger instance for the broker.
            
        Returns:
            BaseBroker: An instance of the specified broker type.
            
        Raises:
            ValueError: If the specified broker type is not registered.
        """
        broker_type = broker_type.lower()
        if broker_type not in cls._broker_registry:
            raise ValueError(f"Broker type '{broker_type}' is not registered")
        
        broker_class = cls._broker_registry[broker_type]
        return broker_class(logger=logger, config=config)
