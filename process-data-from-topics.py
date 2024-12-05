import json
import time
import logging
from kafka import KafkaConsumer, KafkaProducer

class KafkaSensorDataProcessor:
    def __init__(self, topic_prefix):
        """Initializing a KafkaProcessor with a given topics prefix."""
        try:
            from configs import kafka_config
        except ImportError as err:
            raise ImportError(
                "Failed to import Kafka configuration. "
                "Make sure the configs.py file exists and contains the required kafka_config dictionary."
            ) from err

        self.kafka_config = kafka_config
        self.topic_prefix = topic_prefix
        self.topic_name_sensors = f'{topic_prefix}_building_sensors'
        self.topic_name_temperature_alerts = f'{topic_prefix}_temperature_alerts'
        self.topic_name_humidity_alerts = f'{topic_prefix}_humidity_alerts'
        
        self.consumer = self._configure_consumer()
        self.producer = self._configure_producer()
        self._configure_logging()

    def _configure_logging(self):
        """Set up logging with timestamp in ISO 8601 (UTC)."""
        logging.basicConfig(
            level=logging.INFO,
            format='{"timestamp": "%(asctime)s", "level": "%(levelname)s", "message": "%(message)s"}',
            datefmt="%Y-%m-%dT%H:%M:%SZ"
        )
        logging.Formatter.converter = time.gmtime  # Log times in UTC

    def _configure_consumer(self):
        """Setting up Kafka Consumer."""
        return KafkaConsumer(
            self.topic_name_sensors,
            bootstrap_servers=self.kafka_config['bootstrap_servers'],
            security_protocol=self.kafka_config['security_protocol'],
            sasl_mechanism=self.kafka_config['sasl_mechanism'],
            sasl_plain_username=self.kafka_config['username'],
            sasl_plain_password=self.kafka_config['password'],
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )

    def _configure_producer(self):
        """Setting up Kafka Producer."""
        return KafkaProducer(
            bootstrap_servers=self.kafka_config['bootstrap_servers'],
            security_protocol=self.kafka_config['security_protocol'],
            sasl_mechanism=self.kafka_config['sasl_mechanism'],
            sasl_plain_username=self.kafka_config['username'],
            sasl_plain_password=self.kafka_config['password'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def _process_message(self, data):
        """Processing a received message."""
        sensor_id = data['sensor_id']
        timestamp = data['timestamp']
        temperature = data['temperature']
        humidity = data['humidity']

        # Checking the temperature
        if temperature > 40:
            alert = {
                "sensor_id": sensor_id,
                "timestamp": timestamp,
                "value": temperature,
                "message": "Temperature exceeds threshold!"
            }
            self.producer.send(self.topic_name_temperature_alerts, value=alert)
            logging.warning(f"Temperature alert: {json.dumps(alert, indent=2)}")

        # Checking the humidity
        if humidity > 80 or humidity < 20:
            alert = {
                "sensor_id": sensor_id,
                "timestamp": timestamp,
                "value": humidity,
                "message": "Humidity out of range!"
            }
            self.producer.send(self.topic_name_humidity_alerts, value=alert)
            logging.warning(f"Humidity alert: {json.dumps(alert, indent=2)}")

    def run(self):
        """The main cycle of message processing."""
        logging.info(f"Start processing messages from the topic: {self.topic_name_sensors}")
        try:
            for message in self.consumer:
                data = message.value
                self._process_message(data)
        except KeyboardInterrupt:
            logging.info("The script is stopped by the user.")
        except Exception as e:
            logging.error(f"An error occurred: {e}")

def main():
    """Starting the KafkaSensorDataProcessor."""
    topic_prefix = "oleg"
    processor = KafkaSensorDataProcessor(topic_prefix)
    processor.run()

if __name__ == "__main__":
    main()
