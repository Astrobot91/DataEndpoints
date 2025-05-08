import json
import requests
import pandas as pd
from typing import List, Dict

class BrokerData:
    """Class containing static methods to interact with the Upstox API."""
    
    BASE_URL = "http://0.0.0.0:8001/api/v1/"
    
    @staticmethod
    def get_master_data(broker_type: str = "upstox") -> Dict:
        """
        Fetches the master data from the broker API.
        
        Args:
            broker_type (str): The broker type (e.g., 'upstox').
        
        Returns:
            The JSON response from the API as a Python dictionary.
        
        Raises:
            Exception: If the API call fails, with the status code and error message.
        """
        url = f"{BrokerData.BASE_URL}master-data"
        headers = {"Content-Type": "application/json"}
        response = requests.get(url, params={"broker_type": broker_type}, headers=headers)
        if response.status_code != 200:
            raise Exception(f"Failed to get master data: {response.status_code} - {response.text}")
        return response.json()
    
    @staticmethod
    def get_ltp_quote(instruments: List[Dict[str, str]], broker_type: str = "upstox") -> Dict:
        """
        Fetches the Last Traded Price (LTP) quote for the given instruments.
        
        Args:
            broker_type (str): The broker type (e.g., 'upstox').
            instruments: A list of dictionaries, each with 'exchange_token' and 'exchange' keys.
        
        Returns:
            The JSON response from the API as a Python dictionary.
        
        Raises:
            Exception: If the API call fails, with the status code and error message.
        """
        url = f"{BrokerData.BASE_URL}ltp-quote"
        headers = {"Content-Type": "application/json"}
        response = requests.post(
            url, 
            json=instruments, 
            headers=headers,
            params={"broker_type": broker_type}
        )
        if response.status_code != 200:
            raise Exception(f"Failed to get LTP quote: {response.status_code} - {response.text}")
        return response.json()
    
    @staticmethod
    def get_ohlc_quote(instruments: List[Dict[str, str]], broker_type: str = "upstox") -> Dict:
        """
        Fetches the OLHC quote for the given instruments.
        
        Args:
            broker_type (str): The broker type (e.g., 'upstox').
            instruments: A list of dictionaries, each with 'exchange_token' and 'exchange' keys.
        
        Returns:
            The JSON response from the API as a Python dictionary.
        
        Raises:
            Exception: If the API call fails, with the status code and error message.
        """
        url = f"{BrokerData.BASE_URL}ohlc-quote"
        headers = {"Content-Type": "application/json"}
        response = requests.post(
            url, 
            json=instruments,
            headers=headers,
            params={"broker_type": broker_type}
        )
        if response.status_code != 200:
            raise Exception(f"Failed to get ohlc quote: {response.status_code} - {response.text}")
        return response.json()
    

    @staticmethod
    def get_full_mkt_quote(instruments: List[Dict[str, str]], broker_type: str = "upstox") -> Dict:
        """
        Fetches the Last Traded Price (LTP) quote for the given instruments.
        
        Args:
            broker_type (str): The broker type (e.g., 'upstox').
            instruments: A list of dictionaries, each with 'exchange_token' and 'exchange' keys.
        
        Returns:
            The JSON response from the API as a Python dictionary.
        
        Raises:
            Exception: If the API call fails, with the status code and error message.
        """
        url = f"{BrokerData.BASE_URL}full-mkt-quote"
        headers = {"Content-Type": "application/json"}
        response = requests.post(
            url, 
            json=instruments, 
            headers=headers,
            params={"broker_type": broker_type}
        )
        if response.status_code != 200:
            raise Exception(f"Failed to get full market quote: {response.status_code} - {response.text}")
        return response.json()
    

    @staticmethod
    def historical_data(instrument: Dict, broker_type: str = "upstox") -> Dict:
        """
        Hits the /historical-data endpoint and returns the JSON response.

        Args:
            broker_type (str): The broker type (e.g., 'upstox').
            instrument (Dict): A dictionary with the instrument parameters. For example:
                {
                    "exchange": "NSE",
                    "exchange_token": "21195",
                    "instrument_type": "EQ",
                    "interval": "day",
                    "from_date": "2023-01-01",
                    "to_date": "2023-01-31"
                }

        Returns:
            Dict: The JSON response from the API.

        Raises:
            Exception: If the API call fails.
        """
        url = f"{BrokerData.BASE_URL}historical-data?broker_type={broker_type}"
        headers = {"Content-Type": "application/json"}
        payload = {
            "instrument": json.dumps(instrument)
        }
        response = requests.post(url, json=instrument, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to get historical data: {response.status_code} - {response.text}")


if __name__ == "__main__":
    # Example usage
    try:
        master_data = BrokerData.get_master_data()
        
        instruments = [
            {"exchange_token": "21195", "exchange": "NSE", "instrument_type": "EQ"},
            {"exchange_token": "521054", "exchange": "BSE", "instrument_type": "EQ"}
        ]
        ltp_quote = BrokerData.get_ohlc_quote(instruments)
        print("LTP Quote:", pd.DataFrame(ltp_quote.get('data')))

        
        # instrument = {
        #     "exchange": "NSE",
        #     "exchange_token": "21195",
        #     "instrument_type": "EQ",
        #     "interval": "day",
        #     "from_date": "2023-01-01",
        #     "to_date": "2023-01-31"
        # }
        # historical_data = BrokerData.historical_data(instrument)
        # print("Historical Data:", historical_data)
    except Exception as e:
        print(e)
