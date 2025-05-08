"""
Main module for broker implementations.

This module imports and registers all available broker implementations.
"""

from .factory import BrokerFactory
from .base import BaseBroker, BaseAuthenticator, BaseTokenRotator

# Import broker implementations
from .upstox.broker import UpstoxBroker
from .zerodha.broker import ZerodhaBroker

# Register broker implementations with the factory
BrokerFactory.register_broker('upstox', UpstoxBroker)
BrokerFactory.register_broker('Zerodha', ZerodhaBroker)

__all__ = [
    'BrokerFactory',
    'BaseBroker',
    'BaseAuthenticator',
    'BaseTokenRotator'
]
