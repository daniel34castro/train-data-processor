
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
        # 'linger.ms': 5,               # Adds small delay to batch messages
        # 'batch.size': 32768,          # Batches messages up to 32 KB
        'compression.type': 'snappy', # Compress messages using snappy
        # 'acks': '1',                  # Acknowledgment level for performance
        'client.id': socket.gethostname()}

# Define the Digitraffic API URL
LIVE_API = 'https://rata.digitraffic.fi/api/v1/live-trains/'

RAIL_TOPIC = 'rail-data'
RAW_TOPIC = 'rail-raw'
ERROR_TOPIC= 'rail-errors'

# Main function to orchestrate the data retrieval and sending process
def main():
    fetcher = DataFetcher(LIVE_API)
    data = fetcher.fetch_data_local()
    if data is None:
        raise Exception("There isn't any data")
    
    kafka_client = KafkaProducerClient(CONF)
    record_processor = RecordProcessor()
    row_processor = RowProcessor()
    for record in data:
        recordUUID = str(uuid.uuid4())
        # Send raw data to Kafka topic
        kafka_client.send_message(recordUUID, record, RAW_TOPIC)
                
        corrupted_record = record_processor.corrupt_record(record)
        
        try:
            timeTableRows = record_processor.process(corrupted_record)
        except Exception as e:
                print(f"Error processing record: {e}")
                error_record = {"record": corrupted_record}
                error_record['exception'] = e.args[0]
                error_record['exception-detailed'] = e.args[1] if len(e.args) > 1 else None
                kafka_client.send_message(recordUUID, error_record, ERROR_TOPIC)
                continue
            
        for row in timeTableRows:
            try:
                if row.get('type') != 'DEPARTURE':
                    continue  # Skip rows where type is not 'DEPARTURE'
                row_processed = row_processor.process(row, corrupted_record)
                # Send processed data to Kafka topic if no exception is raised
                kafka_client.send_message(row['uniqueKey'], row_processed, RAIL_TOPIC)
            except Exception as e:
                print(f"Error processing or sending data: {e}")
                error_record = {"record": row}
                error_record['exception'] = e.args[0]
                error_record['exception-detailed'] = e.args[1] if len(e.args) > 1 else None
                kafka_client.send_message(str(uuid.uuid4()), error_record, ERROR_TOPIC)

if __name__ == "__main__":
    # print("Starting..")
    # start_time = time.time()
    # main()
    # end_time = time.time()  
    # elapsed_time = end_time - start_time
    # print(f"Finishing... Took {elapsed_time:.2f} seconds.")
    
    # Run the processor
    elapsed_times = []
    for i in range(5):
        print(f"Run {i + 1}...")
        processor = DataProcessor(CONF, LIVE_API, RAW_TOPIC, RAIL_TOPIC, ERROR_TOPIC, max_threads=10)
        start_time = time.time()
        processor.process_data()
        end_time = time.time()
        elapsed_time = end_time - start_time
        elapsed_times.append(elapsed_time)
        print(f"Run {i + 1} finished. Took {elapsed_time:.2f} seconds.")

    average_time = sum(elapsed_times) / len(elapsed_times)
    print(f'Processing times = {elapsed_time}')
    print(f"\nAverage time: {average_time:.2f} seconds.")

