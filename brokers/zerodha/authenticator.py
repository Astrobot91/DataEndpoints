import time
import pyotp
import requests
import logging
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from kiteconnect import KiteConnect
from urllib.parse import urlparse, parse_qs
from typing import Dict, Any
from ..base.authenticator import BaseAuthenticator


class ZerodhaAuthenticator(BaseAuthenticator):
    """
    Authenticator for Zerodha (Kite Connect) that handles login via Selenium,
    TOTP generation, and session token retrieval.
    """
    def __init__(self, config: Dict[str, Any], logger: logging.Logger):
        """
        Initialize the ZerodhaAuthenticator with API credentials and logger.

        Args:
            config (Dict[str, Any]): Configuration dictionary containing:
                - API_KEY: Kite API key
                - API_SECRET: Kite API secret
                - TOTP_KEY: Base32 TOTP secret for 2FA
                - USERID: Zerodha user ID
                - PASSWORD: Zerodha login password
            logger (logging.Logger): Logger for debug and error messages.
        """
        super().__init__(config, logger)
        self.api_key = config.get("API_KEY")
        self.api_secret = config.get("API_SECRET")
        self.totp_key = config.get("TOTP_KEY")
        self.userid = config.get("USERID")
        self.password = config.get("PASSWORD")

        self.access_token = None
        self.driver = None

    def _create_webdriver(self) -> webdriver.Chrome:
        """
        Create and configure a Selenium Chrome WebDriver instance.

        Tries up to 5 times to initialize the driver in headless mode.

        Returns:
            webdriver.Chrome: Configured Chrome WebDriver instance.

        Raises:
            Exception: If WebDriver creation fails after multiple attempts.
        """
        chrome_options = Options()
        chromium_location = '/usr/bin/chromium'
        chromedriver_location = '/usr/local/bin/chromedriver'
        chrome_options.add_argument("--headless")
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--incognito')
        chrome_options.add_argument('--disable-infobars')
        chrome_options.add_argument('--disable-extensions')
        chrome_options.add_argument('--disable-software-rasterizer')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.binary_location = chromium_location

        for attempt in range(5):
            try:
                driver = webdriver.Chrome(
                    service=ChromeService(executable_path=chromedriver_location),
                    options=chrome_options
                )
                self.logger.info("WebDriver created successfully")
                return driver
            except Exception as e:
                self.logger.error(f"WebDriver creation attempt {attempt+1} failed: {e}")
                time.sleep(5)

        self.logger.error("Failed to create WebDriver after multiple attempts")
        raise Exception("Failed to create WebDriver after multiple attempts")

    def fetch_access_token(self) -> str:
        """
        Perform login and exchange the request token for an access token.

        Returns:
            str: The Zerodha API access token.

        Raises:
            Exception: Propagates errors from login or session generation.
        """
        try:
            request_token = self._perform_login()
            kite = KiteConnect(api_key=self.api_key)
            self.logger.info(kite)

            data = kite.generate_session(request_token, api_secret=self.api_secret)
            self.logger.info(data)
            kite.set_access_token(data["access_token"])

            access_token = data["access_token"]
            return access_token
        except Exception as e:
            self.logger.error(f"Failed to fetch access token: {e}")
            time.sleep(2)
            raise

    def _perform_login(self) -> str:
        """
        Automate the Zerodha login flow:
        1. Navigate to login page
        2. Enter user ID and password
        3. Generate and enter TOTP
        4. Retrieve the request_token from redirect URL

        Returns:
            str: The request token used to generate an access token.

        Raises:
            Exception: On any failure during the login process.
        """
        try:
            driver = self._create_webdriver()

            driver.get(f"https://kite.zerodha.com/connect/login?v=3&api_key={self.api_key}")
            self.logger.info("Opened Kite login page")

            time.sleep(1)

            username = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "input#userid"))
            )
            password = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "input#password"))
            )

            username.clear()
            password.clear()
            username.send_keys(self.userid)
            password.send_keys(self.password)
            self.logger.info("Entered username and password")

            WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button.button-orange"))
            ).click()
            self.logger.info("Clicked login button")

            pin = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "#userid"))
            )
            pin.clear()
            pin.click()
            authkey = pyotp.TOTP(self.totp_key)
            pin.send_keys(authkey.now())
            self.logger.info("Entered TOTP code")

            time.sleep(2)
            current_url = driver.current_url
            self.logger.info(f"Redirect URL: {current_url}")

            parsed = urlparse(current_url)
            params = parse_qs(parsed.query)
            request_token = params.get("request_token", [None])[0]

            if request_token is None:
                self.logger.error("Request token not found in URL")
                raise Exception("No request_token in redirect URL")

            self.logger.info(f"Request token retrieved: {request_token}")
            return request_token

        except Exception as e:
            self.logger.error(f"Error during Zerodha login: {e}")
            raise

        finally:
            try:
                driver.quit()
            except Exception:
                pass
