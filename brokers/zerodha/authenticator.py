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
from typing import Dict, Any
from urllib.parse import urlparse, parse_qs

from ..base.authenticator import BaseAuthenticator

class ZerodhaAuthenticator(BaseAuthenticator):
    def __init__(self, config: Dict[str, Any], logger: logging.Logger):
        """
        Initialize the authenticator with configuration and logger.
        
        Args:
            config (Dict[str, Any]): Configuration parameters for authentication.
            logger (logging.Logger): Logger instance for the authenticator.
        """
        super().__init__(config, logger)
        self.api_key = config.get("API_KEY")
        self.api_secret = config.get("API_SECRET")
        self.totp_key = config.get("TOTP_KEY")
        self.userid = config.get("USERID")
        self.password = config.get("PASSWORD")
        
        self.access_token = None
        self.driver = None

    
    def _create_webdriver(self):
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

    def _perform_login(self):
        while True:
            try:
                driver = self.create_webdriver()
                
                driver.get(f"https://kite.zerodha.com/connect/login?v=3&api_key={api_config.zerodha_api_key}")
                logging.info("Opened Kite login page")

                time.sleep(1)   

                logging.info(driver.page_source)

                username = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "input#userid"))
                )

            
                password = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "input#password"))
                )
                
                time.sleep(1)
                
                username.clear()
                password.clear()
                
                time.sleep(1)
                
                username.send_keys(api_config.zerodha_email)
                password.send_keys(api_config.zerodha_alpha_password)
                logging.info("Entered username and password")
                
                time.sleep(1)
                
                WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR,"button.button-orange" ))).click() 
                logging.info("Clicked login button")
                
                time.sleep(1)
                

                pin = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "#userid"))
                )
                
                pin.clear()
                pin.click()
                logging.info("Entered TOTP")
                totp_key = api_config.zerodha_totp_key
                
                authkey = pyotp.TOTP(totp_key)
                
                pin.send_keys(authkey.now())
                
                time.sleep(1)
                time.sleep(15)
                current_url = driver.current_url

                logging.info(current_url)
                
                if current_url:
                    sp = current_url.split("&")
                    cut_dict = {}
                    for x in sp:
                        cut = x.split("=")
                        logging.info(cut)
                        cut_dict[cut[0]] = cut[1] 
                    request_token = cut_dict.get('sess_id', None)

                    if not request_token:
                        raise Exception("Request token not found in URL")
                else:
                    raise Exception("Failed to retrieve current URL")

            except Exception as e:
                self.logger.exception(e)