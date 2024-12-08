import json
import time
from confluent_kafka import Producer, KafkaException

class KafkaProducerClient:
    """Handles sending data to Kafka topics with built-in batching support."""
    def __init__(self, conf):
        self.producer = Producer(conf)  # Kafka producer instance

    def delivery_report(self, err, msg):
        """Callback for message delivery status."""
        if err is not None:
            print(f"Delivery failed for record {msg.key()}: {err}")
        else:
            # Optional: Log success (can be commented out to reduce verbosity)
            print(f"Message delivered to {msg.topic()} [partition: {msg.partition()}]")
            pass

    def send_message(self, key, record, topic, retries=3, retry_delay=2):
        """
        Sends a single message to a Kafka topic with a retry mechanism.
        
        Args:
            key (str): The message key for Kafka partitioning.
            record (dict): The message payload.
            topic (str): The Kafka topic to send the message to.
        """
        for attempt in range(1, retries + 1):  # Retry loop
            try:
                self.producer.produce(
                    topic=topic,
                    key=key,
                    value=json.dumps(record),
                    callback=self.delivery_report
                )
                return 
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
            
    def flush(self):
        self.producer.flush()

