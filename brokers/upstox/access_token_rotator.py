import os
import json
import boto3
import logging
from logger import get_logger
from upstox_authenticator import UpstoxAuthenticator

UPSTOX_CONFIG_SECRET_NAME = os.getenv("UPSTOX_CONFIG_SECRET_NAME", "my_upstox_config")
UPSTOX_TOKEN_SECRET_NAME = os.getenv("UPSTOX_TOKEN_SECRET_NAME", "my_upstox_access_token")

logger = get_logger(
    name="UpstoxTokenRotator",
    log_group="DataPipeline",
    log_stream="upstox_token_rotator"
)

def rotate():
    """
    Rotator that:
      1. Fetches Upstox credentials from Secrets Manager.
      2. Uses Selenium to log in and retrieve a fresh access token.
      3. Stores that token in Secrets Manager.
      4. Logs steps to CloudWatch.
    """
    logger.info("Starting Upstox token rotation process.")
    secrets_client = boto3.client("secretsmanager")
    
    upstox_config_secret = secrets_client.get_secret_value(SecretId=UPSTOX_CONFIG_SECRET_NAME)
    config_json = json.loads(upstox_config_secret["SecretString"])
    
    authenticator = UpstoxAuthenticator(config=config_json, logger=logger)
    new_token = authenticator.fetch_access_token()
    logger.info(f"Successfully obtained new access token: {new_token}")
    
    updated_secret = json.dumps({"access_token": new_token})
    secrets_client.update_secret(
        SecretId=UPSTOX_TOKEN_SECRET_NAME,
        SecretString=updated_secret
    )
    
    logger.info("Upstox access token updated in Secrets Manager.")
    
    return {
        "statusCode": 200,
        "body": json.dumps("Upstox token rotation completed successfully!")
    }


if __name__ == "__main__":
    rotate()