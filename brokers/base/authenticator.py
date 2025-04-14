"""
Base authenticator module defining the abstract interface for all broker authentication implementations.

This module contains the BaseAuthenticator abstract base class that defines the common
interface that all broker authenticator implementations must adhere to.
"""

import abc
import logging
from typing import Dict, Any, Optional


class BaseAuthenticator(abc.ABC):
    """
    Abstract base class for all broker authenticator implementations.
    
    This class defines the common interface that all broker authenticator implementations
    must implement to ensure consistency across different brokers.
    
    Attributes:
        config (Dict[str, Any]): Configuration parameters for authentication.
        logger (logging.Logger): Logger instance for the authenticator.
    """
    
    def __init__(self, config: Dict[str, Any], logger: logging.Logger):
        """
        Initialize the authenticator with configuration and logger.
        
        Args:
            config (Dict[str, Any]): Configuration parameters for authentication.
            logger (logging.Logger): Logger instance for the authenticator.
        """
        self.config = config
        self.logger = logger
    
    @abc.abstractmethod
    def fetch_access_token(self) -> str:
        """
        Fetch a new access token from the broker's authentication service.
        
        This method should handle the entire authentication flow, including
        any necessary API calls, credential validation, and token retrieval.
        
        Returns:
            str: The new access token.
            
        Raises:
            Exception: If authentication fails for any reason.
        """
        pass
