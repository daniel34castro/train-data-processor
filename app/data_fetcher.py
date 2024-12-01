import json
import time
import requests
from requests.exceptions import RequestException


class DataFetcher:
    """Handles fetching data from an external API."""

    
    def __init__(self, api_url):
        self.api_url = api_url

    def fetch_data(self):
        """Fetch data from the API."""
        retry_attempts = 2
        retry_delay = 5
        
        for attempt in range(retry_attempts):
            print(f"Attempt {attempt + 1} to retrieve and send train data...")
            try:
                data = self.__make_request()
                return data
            except RequestException as e:
                if attempt < retry_attempts - 1:
                    print(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    print("Max retry attempts reached. Exiting...")
                    raise Exception("Could not fetch data")
            
    def __make_request(self):
            response = requests.get(self.api_url)
            response.raise_for_status()
            print("Data fetched from Digitraffic API")
            return response.json()

    def fetch_data_local(self):
        with open('live_trains.json', 'r') as f:
            data = json.load(f)
            print(f"Number of records fetched: {len(data)}")
            return data
