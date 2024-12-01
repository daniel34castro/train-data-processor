import json
from confluent_kafka import Producer, KafkaException

class KafkaProducerClient:
    """Handles sending data to Kafka topics."""
    def __init__(self, conf, batch_size=500):
        self.producer = Producer(conf)
        self.batch_size = batch_size
        self.record_count = 0

    def delivery_report(self, err, msg):
        """Reports the delivery status of a Kafka message."""
        if err is not None:
            print(f"Delivery failed for record {msg.key()}: {err}")
        # else:
            # print(f"Record successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

    def send_message(self, key, record, topic):
        """Sends a single message to a Kafka topic."""
        try:
            self.producer.produce(
                topic=topic,
                key=key,
                value=json.dumps(record),
                callback=self.delivery_report,
            )
            self.__flush_by_batch()
            
        # TODO implement retry mechanism     
        except KafkaException as e:
            print(f"Kafka error while producing to {topic}: {e}")
        except Exception as e:
            print(f"Unexpected error while producing to {topic}: {e}")
            
    def __flush_by_batch(self):
        # Flush periodically after reaching batch_size
        self.record_count += 1    
        if self.record_count % self.batch_size == 0:
            print(f"Flushing after {self.batch_size} messages.")
            unsent_count = self.producer.flush(0)
            if unsent_count > 0:
                print(f"{unsent_count} messages still in buffer. Flushing...")
            self.producer.flush()

