
import logging
import time
import os
import uuid
import socket
from data_processor import DataProcessor
from row_processor import RowProcessor
from record_processor import RecordProcessor
from data_fetcher import DataFetcher
from kafka_producer_client import KafkaProducerClient


CONF = {'bootstrap.servers': 'pkc-7xoy1.eu-central-1.aws.confluent.cloud:9092',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': '5QQUUN7SPZLDSXQO',
        'sasl.password': 'hj4Cq0CEWY69HHuxwJA4oeB522B6wpdm6O5hizlXk7Ys/2EQDQ7ac0MdOcfrmTlx',
        'linger.ms': 3000,               # Adds small delay to batch messages
        # 'batch.size': 32768,          # Batches messages up to 32 KB
        'compression.type': 'snappy', # Compress messages using snappy
        # 'acks': '1',                  # Acknowledgment level for performance
        'client.id': socket.gethostname()}

# Define the Digitraffic API URL
LIVE_API = 'https://rata.digitraffic.fi/api/v1/live-trains/'

RAIL_TOPIC = 'rail-data'
RAW_TOPIC = 'rail-raw'
ERROR_TOPIC= 'rail-errors'
# Set up logging
logging.basicConfig(level=logging.DEBUG)  # Set the logging level to DEBUG
logger = logging.getLogger("confluent_kafka")

if __name__ == "__main__":
    print("Starting..")
    processor = DataProcessor(CONF, LIVE_API, RAW_TOPIC, RAIL_TOPIC, ERROR_TOPIC, max_threads=10)
    start_time = time.perf_counter()
    processor.process_data()
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    print(f"Finishing... Took {elapsed_time:.2f} seconds.")
    
    # Run multiple times, prints average time
    # elapsed_times = []
    # for i in range(5):
    #     print(f"Run {i + 1}...")
    #     processor = DataProcessor(CONF, LIVE_API, RAW_TOPIC, RAIL_TOPIC, ERROR_TOPIC, max_threads=10)
    #     start_time = time.perf_counter()
    #     processor.process_data()
    #     end_time = time.perf_counter()
    #     elapsed_time = end_time - start_time
    #     elapsed_times.append(elapsed_time)
    #     print(f"Run {i + 1} finished. Took {elapsed_time:.2f} seconds.")
    # average_time = sum(elapsed_times) / len(elapsed_times)
    # print(f'Processing times = {elapsed_times}')
    # print(f"\nAverage time: {average_time:.2f} seconds.")

