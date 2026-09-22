import os
import sys
import pandas as pd
import time
import logging
import json
import joblib
from confluent_kafka import Consumer, Producer
from sklearn.preprocessing import TargetEncoder

sys.path.append(os.path.abspath('./src'))
from data_preprocessing import preprocess_data
from scoring import get_preds

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/service.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

KAFKA_CONFIG = {
    "transactions_topic": os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions"),
    "scoring_topic": os.getenv("KAFKA_SCORING_TOPIC", "scoring"),
    "bootstrap_servers": os.getenv("BOOTSTRAP_SERVERS", "kafka:9092"),
    "transactions_group": os.getenv("KAFKA_TRANSACTIONS_GROUP", 'ml-scorer')
}

class ProcessingService:
    def __init__(self):
        self.consumer = Consumer({
            'bootstrap.servers': KAFKA_CONFIG['bootstrap_servers'],
            'group.id': KAFKA_CONFIG['transactions_group'],
            'auto.offset.reset': 'earliest'
        })
        self.producer = Producer({
            'bootstrap.servers': KAFKA_CONFIG['bootstrap_servers']
        })
        self.consumer.subscribe([KAFKA_CONFIG['transactions_topic']])
        self.encoder = joblib.load("./models/target_encoder.pkl")
        self.model = joblib.load("./models/lgbm_model.pkl")
        logger.info("Pretrained models imported successfully...")

    def process_messages(self):
        while True:
            message = self.consumer.poll(1.0)
            if not message:
                time.sleep(0.2)
                continue
            if message.error():
                logger.error(f"Kafka error: {message.error()}")
                continue
            try:
                data = json.loads(message.value().decode('utf-8'))
                transaction_id = data['transaction_id']
                df = pd.DataFrame(data['data'])

                df = preprocess_data(df, self.encoder)
                prediction = get_preds(df, self.model)
                prediction['transaction_id'] = transaction_id
                logger.info(f"Prediction for transaction {transaction_id} completed")

                record = prediction.to_json(orient='records')
                self.producer.produce(
                    KAFKA_CONFIG['scoring_topic'],
                    value=record.encode('utf-8')
                )

            except Exception as e:
                logger.error(f"Error processing message: {e}")

    def close(self):
        self.consumer.close()
        self.producer.flush(timeout=10)

if __name__ == "__main__":
    logger.info('Starting Kafka ML scoring service...')
    service = ProcessingService()
    try:
        service.process_messages()
    except KeyboardInterrupt:
        logger.info('Service stopped by user')
    finally:
        service.close()

