import os
import json
import requests
import pytz
import logging
import pandas as pd
from datetime import datetime
from warnings import warn
from termcolor import colored

from common import  OutputPathSingleton, convert_timestamp_columns, top_shareholders_by_symbol, parse_formatted_file_date
from common import RETRIEVED_JSON_PATH, DEBUG



# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

if DEBUG:
    # warning with colored output
    logging.warning(colored("Running in DEBUG mode!!!", "yellow"))

# Constants
SISTER_7_SLUGS = ['googl', 'amzn', 'meta', 'nvda', 'aapl', 'msft', 'tsla', 'intc', 'qcom', 'mu']

class StockOwnershipAPI:
    """
    Encapsulates API interactions for stock ownership retrieval.
    """

    #BASE_URL = "https://api.fintel.io/data/v/0.0/so/us/"

    def __init__(self, api_key, base_url='https://api.fintel.io/data/v/0.0/so/us/'):
        self.api_key = api_key
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            "accept": "application/json",
            "X-API-KEY": api_key
        })

    def fetch_data(self, slug):
        """
        Fetches stock ownership data from the API.
        """
        try:
            url = f"{self.base_url}{slug}"
            response = self.session.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to fetch data for {slug}: {e}")
            return {}

    def retrieve_sisters_data(self, slug_list):
        """
        Retrieves and stores data for the 7 sisters stocks.
        """
        results_dir = OutputPathSingleton.get_path()
        if not results_dir:
            return False

        for slug in slug_list:
            file_path = os.path.join(results_dir, f"{slug}.json")

            if os.path.exists(file_path):
                logging.info(f"Skipping {slug}, file already exists.")
                continue

            data = self.fetch_data(slug)
            if data:
                with open(file_path, "w") as f:
                    json.dump(data, f)
                    logging.info(f"Saved {file_path}")
        
        logging.info("Data retrieval completed.")
        return True