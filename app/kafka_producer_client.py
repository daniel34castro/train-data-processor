import json
import time
from confluent_kafka import Producer, KafkaException

class KafkaProducerClient:
    """Handles sending data to Kafka topics."""
    def __init__(self, conf, batch_size=500, flush_timeout=10):
        self.producer = Producer(conf)  # Kafka producer instance
        self.batch_size = batch_size    # Number of messages before auto-flush
        self.flush_timeout = flush_timeout  # Configurable flush timeout in seconds
        self.record_count = 0  # Counter for batched messages

    def delivery_report(self, err, msg):
        """
        Enhanced callback for delivery report.
        Logs the status and provides hooks for further handling if needed.
        """
        if err is not None:
            print(f"Delivery failed for record {msg.key()}: {err}")
            # Additional failure handling can go here
        # else:
        #     print(f"Message delivered to {msg.topic()} [partition: {msg.partition()}]")

    def send_message(self, key, record, topic, retries=3, retry_delay=2):
        """
        Sends a single message to a Kafka topic with a retry mechanism.
        
        Args:
            key (str): The message key for Kafka partitioning.
            record (dict): The message payload as a dictionary.
            topic (str): The target Kafka topic.
            retries (int): Number of retries for transient errors.
            retry_delay (int): Delay (in seconds) between retry attempts.
        """
        for attempt in range(1, retries + 1):  # Retry loop
            try:
                self.producer.produce(
                    topic=topic,
                    key=key,
                    value=json.dumps(record),
                    callback=self.delivery_report
                )
                self.__flush_by_batch() 
                return  # Message produced successfully; exit the loop
            except KafkaException as e:
                print(f"Attempt {attempt}/{retries} failed: Kafka error - {e}")
                if attempt < retries:
                    print(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    print(f"Failed to send message to {topic} after {retries} attempts.")
            except Exception as e:
                print(f"Unexpected error: {e}")
                break  # Stop on non-retryable errors

    def __flush_by_batch(self):
        """
        Flush producer buffer periodically based on batch size.
        Ensures all messages are sent to Kafka after reaching the configured batch size.
        """
        self.record_count += 1
        if self.record_count % self.batch_size == 0:
            # print(f"Flushing after {self.batch_size} messages.")
            unsent_count = self.producer.flush(self.flush_timeout)
            if unsent_count > 0:
                print(f"{unsent_count} messages were not sent after flush timeout.")

    def flush(self):
        """
        Explicit flush method to ensure all buffered messages are sent to Kafka.
        Useful for ensuring delivery at the end of a batch or program execution.
        """
        print("Performing final flush.")
        unsent_count = self.producer.flush(self.flush_timeout)
        if unsent_count > 0:
            print(f"{unsent_count} messages could not be delivered after the final flush.")

