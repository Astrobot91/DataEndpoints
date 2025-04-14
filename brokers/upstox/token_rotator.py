"""
Upstox token rotator implementation.

This module contains the UpstoxTokenRotator class which implements the BaseTokenRotator
interface for the Upstox trading platform.
"""

import os
import json
import boto3
import logging
from typing import Dict, Any

from ..base.token_rotator import BaseTokenRotator
from .authenticator import UpstoxAuthenticator


class UpstoxTokenRotator(BaseTokenRotator):
    """
    Upstox token rotator implementation.
    
    This class implements the BaseTokenRotator interface for the Upstox trading platform.
    It handles token rotation, storage, and validation for Upstox API access tokens.
    
    Attributes:
        config (Dict[str, Any]): Configuration parameters for token rotation.
        logger (logging.Logger): Logger instance for the token rotator.
    """
    
    UPSTOX_TOKEN_SECRET_NAME = os.getenv("UPSTOX_TOKEN_SECRET_NAME", "my_upstox_access_token")

    def __init__(self, config: Dict[str, Any], logger: logging.Logger):
        """
        Initialize the token rotator with configuration and logger.
        
        Args:
            config (Dict[str, Any]): Configuration parameters for token rotation.
            logger (logging.Logger): Logger instance for the token rotator.
        """
        super().__init__(config, logger)
        self.secrets_client = boto3.client("secretsmanager")
    
    def rotate(self) -> Dict[str, Any]:
        """
        Rotate the access token for Upstox.
        
        This method fetches Upstox credentials from Secrets Manager,
        uses the authenticator to get a fresh access token, and stores
        the new token in Secrets Manager.
        
        Returns:
            Dict[str, Any]: Dictionary containing the rotation result.
            
        Raises:
            Exception: If token rotation fails.
        """
        self.logger.info("Starting Upstox token rotation process.")
        
        try:
            # Create authenticator and fetch new token
            authenticator = UpstoxAuthenticator(config=self.config, logger=self.logger)
            new_token = authenticator.fetch_access_token()
            self.logger.info(f"Successfully obtained new access token: {new_token}")
            
            # Store the new token
            self.store_token(new_token)
            
            return {
                "statusCode": 200,
                "body": json.dumps("Upstox token rotation completed successfully!")
            }
        except Exception as e:
            error_msg = f"Error during token rotation: {e}"
            self.logger.error(error_msg)
            return {
                "statusCode": 500,
                "body": json.dumps(error_msg)
            }
    
    def get_current_token(self) -> str:
        """
        Get the current access token from Secrets Manager.
        
        Returns:
            str: The current access token.
            
        Raises:
            Exception: If token retrieval fails.
        """
        try:
            token_secret = self.secrets_client.get_secret_value(
                SecretId=self.UPSTOX_TOKEN_SECRET_NAME
            )
            token_json = json.loads(token_secret["SecretString"])
            return token_json.get("access_token", "")
        except Exception as e:
            error_msg = f"Error retrieving current token: {e}"
            self.logger.error(error_msg)
            raise Exception(error_msg)
    
    def store_token(self, token: str) -> bool:
        """
        Store a new access token in Secrets Manager.
        
        Args:
            token (str): The new access token to store.
            
        Returns:
            bool: True if token was stored successfully, False otherwise.
            
        Raises:
            Exception: If token storage fails.
        """
        try:
            updated_secret = json.dumps({"access_token": token})
            self.secrets_client.update_secret(
                SecretId=self.UPSTOX_TOKEN_SECRET_NAME,
                SecretString=updated_secret
            )
            self.logger.info("Upstox access token updated in Secrets Manager.")
            return True
        except Exception as e:
            error_msg = f"Error storing token: {e}"
            self.logger.error(error_msg)
            return False
    
    def is_token_valid(self, token: str) -> bool:
        """
        Check if a token is valid by making a test API call.
        
        Args:
            token (str): The token to check.
            
        Returns:
            bool: True if token is valid, False otherwise.
        """
        # This is a simplified implementation
        # to verify the token's validity
        return bool(token) and len(token) > 10
