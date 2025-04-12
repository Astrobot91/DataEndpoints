import time
import pyotp
import requests
import logging
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from urllib.parse import urlparse, parse_qs

class UpstoxAuthenticator:
    """
    Automates the Upstox login flow via Selenium to obtain a new access token.
    """
    def __init__(self, config: dict, logger: logging.Logger):
        """
        Args:
            config (dict): Dictionary containing upstox credentials and settings.
            logger (logging.Logger): The logger instance.
        """
        self.logger = logger
        self.api_key = config["API_KEY"]
        self.api_secret = config["API_SECRET"]
        self.redirect_uri = config["REDIRECT_URL"]
        self.phone_no = config["PHONE_NO"]
        self.totp_key = config["TOTP_KEY"]
        self.pin_code = config["PIN_CODE"]
        
        self.access_token = None
        self.driver = None

    def fetch_access_token(self) -> str:
        """
        Main method to automate login and return new Upstox access token.
        """
        auth_code = self._perform_login()
        if not auth_code:
            raise Exception("Failed to obtain authorization code from Upstox.")
        
        new_token = self._get_access_token(auth_code)
        return new_token

    def _perform_login(self):
        """
        Automates the login process via Selenium, returning the authorization code.
        """
        auth_url = (
            f"https://api.upstox.com/v2/login/authorization/dialog?"
            f"response_type=code&client_id={self.api_key}&redirect_uri={self.redirect_uri}"
        )
        self.logger.info("Creating WebDriver for Upstox login")
        
        try:
            self.driver = self._create_webdriver()
            self.driver.get(auth_url)
            self.logger.info("Opened Upstox login page")

            self._enter_phone_number()
            self._enter_totp()
            self._enter_pin_code()

            time.sleep(10)  # Wait for the final redirect
            current_url = self.driver.current_url
            self.logger.info(f"Current URL after login: {current_url}")
            auth_code = self._get_code_from_url(current_url)
            return auth_code
        except Exception as e:
            self.logger.error(f"Error during Upstox login: {e}")
            raise
        finally:
            if self.driver:
                self.driver.quit()

    def _create_webdriver(self):
        """
        Creates and configures a Selenium WebDriver instance.

        Returns:
            WebDriver: Configured Selenium WebDriver instance.

        Raises:
            Exception: If WebDriver creation fails after multiple attempts.
        """
        chrome_options = Options()
        chromium_location = '/usr/bin/chromium'
        chromedriver_location = '/usr/bin/chromedriver'
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
                driver = webdriver.Chrome(service=ChromeService(executable_path=chromedriver_location), options=chrome_options)
                self.logger.info("WebDriver created successfully")
                return driver
            except Exception as e:
                self.logger.error(f"WebDriver creation attempt {attempt+1} failed: {e}")
                time.sleep(5)
        self.logger.error("Failed to create WebDriver after multiple attempts")
        raise Exception("Failed to create WebDriver after multiple attempts")

    def _enter_phone_number(self):
        """
        Enters phone number on the login page, requests OTP.
        """
        time.sleep(5)  # give page time to render
        self.logger.info("Entering phone number")
        mobilenum = WebDriverWait(self.driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "#mobileNum"))
        )
        mobilenum.clear()
        mobilenum.send_keys(self.phone_no)
        
        get_otp_button = WebDriverWait(self.driver, 20).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "#getOtp"))
        )
        get_otp_button.click()
        self.logger.info("Phone number entered and OTP requested")

    def _enter_totp(self):
        """
        Generates and enters the TOTP code.
        """
        self.logger.info("Entering TOTP")
        otp_input = WebDriverWait(self.driver, 20).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "#otpNum"))
        )
        otp_input.clear()
        totp = pyotp.TOTP(self.totp_key)
        otp_input.send_keys(totp.now())
        
        continue_button = WebDriverWait(self.driver, 20).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "#continueBtn"))
        )
        continue_button.click()
        self.logger.info("TOTP entered and continued")

    def _enter_pin_code(self):
        """
        Enters the 6-digit PIN to complete the login.
        """
        self.logger.info("Entering 6-digit PIN code")
        pin_input = WebDriverWait(self.driver, 40).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "#pinCode"))
        )
        pin_input.send_keys(self.pin_code)
        
        pin_continue_button = WebDriverWait(self.driver, 20).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "#pinContinueBtn"))
        )
        pin_continue_button.click()
        self.logger.info("PIN code entered and continued")

    def _get_code_from_url(self, url: str) -> str:
        """
        Extracts authorization code from the redirected URL.
        """
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
        code = query_params.get("code", [None])[0]
        self.logger.info(f"Authorization code obtained: {code}")
        return code

    def _get_access_token(self, code: str) -> str:
        """
        Exchanges authorization code for an Upstox access token.
        """
        self.logger.info("Exchanging authorization code for access token")
        
        url = "https://api.upstox.com/v2/login/authorization/token"
        headers = {
            "accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        data = {
            "code": code,
            "client_id": self.api_key,
            "client_secret": self.api_secret,
            "redirect_uri": self.redirect_uri,
            "grant_type": "authorization_code",
        }
        try:
            response = requests.post(url, headers=headers, data=data)
            response.raise_for_status()
            token_data = response.json()
            access_token = token_data["access_token"]
            self.logger.info("Access token obtained successfully")
            return access_token
        except requests.exceptions.HTTPError as http_err:
            self.logger.error(f"HTTP error occurred: {http_err} - Response: {response.text}")
            raise
        except Exception as err:
            self.logger.error(f"An error occurred: {err}")
            raise
