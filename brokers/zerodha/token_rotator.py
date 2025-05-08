import os
import json
import boto3
import logging
from typing import Dict, Any

from ..base.token_rotator import BaseTokenRotator
from .authenticator import ZerodhaAuthenticator


class ZerodhaTokenRotator(BaseTokenRotator):

    ZERODHA_TOKEN_SECRET_NAME = os.getenv("ZERODHA_TOKEN_SECRET_NAME", "my_zerodha_access_token")

    def __init__(self, config: Dict[str, Any], logger: logging.Logger):

        super().__init__(config, logger)
        self.secrets_client = boto3.client("secretsmanager")
    

    def rotate(self) -> Dict[str, Any]:

        self.logger.info("Starting Zerodha token rotation process.")
        
        try:
        
            authenticator = ZerodhaAuthenticator(config=self.config, logger=self.logger)
            new_token = authenticator.fetch_access_token()
            self.logger.info(f"Successfully obtained new access token: {new_token}")
            
            self.store_token(new_token)
            
            return {
                "statusCode": 200,
                "body": json.dumps("Zerodha token rotation completed successfully!")
            }
        except Exception as e:
            error_msg = f"Error during token rotation: {e}"
            self.logger.error(error_msg)
            return {
                "statusCode": 500,
                "body": json.dumps(error_msg)
            }
    

    def get_current_token(self) -> str:
        try:
            token_secret = self.secrets_client.get_secret_value(
                SecretId=self.ZERODHA_TOKEN_SECRET_NAME
            )
            token_json = json.loads(token_secret["SecretString"])
            return token_json.get("access_token", "")
        except Exception as e:
            error_msg = f"Error retrieving current token: {e}"
            self.logger.error(error_msg)
            raise Exception(error_msg)
    

    def store_token(self, token: str) -> bool:
        
        try:
            updated_secret = json.dumps({"access_token": token})
            self.secrets_client.update_secret(
                SecretId=self.ZERODHA_TOKEN_SECRET_NAME,
                SecretString=updated_secret
            )
            self.logger.info("Zerodha access token updated in Secrets Manager.")
            return True
        except Exception as e:
            error_msg = f"Error storing token: {e}"
            self.logger.error(error_msg)
            return False
    

    def is_token_valid(self, token: str) -> bool:

        return bool(token) and len(token) > 10
