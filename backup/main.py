import time
import sys
import os
import subprocess
import requests
import json
from datetime import datetime, timedelta


yesterday = datetime.now() - timedelta(days=1)
from_date = (yesterday - timedelta(days=10)).strftime('%Y-%m-%d')
to_date = yesterday.strftime('%Y-%m-%d')

ENDPOINT_URL = "http://localhost:8001/api/v1/upstox/historical-data"

PAYLOAD = {
    "exchange": "NSE",
    "exchange_token": "2885",
    "interval": "day",
    "from_date": from_date,
    "to_date": to_date,
    "instrument_type": "EQ"
}

def rotate_token():
    print("Rotating access token...")
    try:
        subprocess.run(["python3", "brokers/upstox/access_token_rotator.py"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Token rotator failed: {e}")
        sys.exit(1)
    print("Token rotation completed.")

def restart_program():
    print("Restarting program...")
    python = sys.executable
    os.execv(python, [python] + sys.argv)

def main_loop():
    while True:
        try:
            response = requests.post(ENDPOINT_URL, json=PAYLOAD, timeout=10)
            if response.status_code == 500: 
                print("No historical data returned. Initiating token rotation.")
                rotate_token()
                restart_program()
        except Exception as ex:
            print(f"Error occurred: {ex}")
        time.sleep(5)

if __name__ == "__main__":
    main_loop()