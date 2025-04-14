"""
Base token rotator module defining the abstract interface for all broker token rotation implementations.

This module contains the BaseTokenRotator abstract base class that defines the common
interface that all broker token rotation implementations must adhere to.
"""

import abc
import logging
from typing import Dict, Any, Optional


class BaseTokenRotator(abc.ABC):
    """
    Abstract base class for all broker token rotation implementations.
    
    This class defines the common interface that all broker token rotation implementations
    must implement to ensure consistency across different brokers.
    
    Attributes:
        config (Dict[str, Any]): Configuration parameters for token rotation.
        logger (logging.Logger): Logger instance for the token rotator.
    """
    
    def __init__(self, config, logger: logging.Logger):
        """
        Initialize the token rotator with configuration and logger.
        
        Args:
            config (Dict[str, Any]): Configuration parameters for token rotation.
            logger (logging.Logger): Logger instance for the token rotator.
        """
        self.config = config
        self.logger = logger
    
    @abc.abstractmethod
    def rotate(self) -> Dict[str, Any]:
        """
        Rotate the access token for the broker.
        
        This method should handle the entire token rotation flow, including
        fetching a new token via the authenticator, storing it securely,
        and returning the result.
        
        Returns:
            Dict[str, Any]: Dictionary containing the rotation result with at least
                a 'statusCode' and 'body' field.
            
        Raises:
            Exception: If token rotation fails for any reason.
        """
        pass
    
    @abc.abstractmethod
    def get_current_token(self) -> str:
        """
        Get the current access token.
        
        Returns:
            str: The current access token.
            
        Raises:
            Exception: If token retrieval fails.
        """
        pass
    
    @abc.abstractmethod
    def store_token(self, token: str) -> bool:
        """
        Store a new access token securely.
        
        Args:
            token (str): The new access token to store.
            
        Returns:
            bool: True if token was stored successfully, False otherwise.
            
        Raises:
            Exception: If token storage fails.
        """
        pass
    
    @abc.abstractmethod
    def is_token_valid(self, token: str) -> bool:
        """
        Check if a token is valid.
        
        Args:
            token (str): The token to check.
            
        Returns:
            bool: True if token is valid, False otherwise.
        """
        pass
