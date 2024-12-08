import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from row_processor import RowProcessor
from record_processor import RecordProcessor
from data_fetcher import DataFetcher
from kafka_producer_client import KafkaProducerClient


class DataProcessor:
    """Class to handle data fetching, processing, and sending to Kafka."""
    def __init__(self, kafka_conf, api_url, raw_topic, rail_topic, error_topic, max_threads=5):
        self.fetcher = DataFetcher(api_url)
        self.kafka_client = KafkaProducerClient(kafka_conf)
        self.record_processor = RecordProcessor()
        self.row_processor = RowProcessor()
        self.raw_topic = raw_topic
        self.rail_topic = rail_topic
        self.error_topic = error_topic
        self.max_threads = max_threads  # Maximum threads for parallel processing

    def process_data(self):
        """Fetch data and process it using multithreading."""
        print("Fetching data...")
        data = self.fetcher.fetch_data()
        if not data:
            raise Exception("No data fetched from the source.")

        print(f"Fetched {len(data)} records.")

        # self._multi_thread_processing(data)
        for record in data:
            self._process_record(record) 
        
        self.kafka_client.flush()

        print("Finished processing all records.")


    def _multi_thread_processing(self, data):
        # Process records concurrently
        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            futures = [executor.submit(self._process_record, record) for record in data]
            # Collect and print results
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Error during record processing: {e}")
                    
                    
    def _process_record(self, record):
        """Process an individual record and send to Kafka."""
        record_uuid = str(uuid.uuid4())
        # Send raw data to Kafka
        self.kafka_client.send_message(record_uuid, record, self.raw_topic)

        corrupted_record = self.record_processor.corrupt_record(record)

        try:
            time_table_rows = self.record_processor.process(corrupted_record)
        except Exception as e:
            self._handle_error(record_uuid, corrupted_record, e)
            return

        for row in time_table_rows:
            if row.get('type') != 'DEPARTURE':
                continue  # Skip rows where type is not 'DEPARTURE'
            try:
                row_processed = self.row_processor.process(row, corrupted_record)
                self.kafka_client.send_message(row['uniqueKey'], row_processed, self.rail_topic)
            except Exception as e:
                self._handle_error(str(uuid.uuid4()), row, e)

    def _handle_error(self, record_uuid, record, exception):
        """Handle errors by sending them to the error topic."""
        # print(f"Error processing record: {exception}")
        error_record = {"record": record}
        error_record['exception'] = exception.args[0]
        error_record['exception-detailed'] = exception.args[1] if len(exception.args) > 1 else None
        self.kafka_client.send_message(record_uuid, error_record, self.error_topic)
        
        
        
