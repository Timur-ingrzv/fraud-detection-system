import psycopg2
import os
import logging
import json
from confluent_kafka import Consumer, Producer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/service.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

DB_CONFIG = {
    "dbname": os.getenv("POSTGRES_DB", "fraud_detections"),
    "table": os.getenv("POSTGRES_TABLE", "scores"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "123456"),
    "host": os.getenv("POSTGRES_HOST", "database"),
    "port": os.getenv("POSTGRES_PORT", "5432")
}

KAFKA_CONFIG = {
    "scoring_topic": os.getenv("KAFKA_SCORING_TOPIC", "scoring"),
    "bootstrap_servers": os.getenv("BOOTSTRAP_SERVERS", "kafka:9092"),
    "scoring_group": os.getenv("KAFKA_SCORING_GROUP", 'ml-scorer')
}

class EventsWriter:
    def __init__(self):
        connection_config = {k:v for k, v in DB_CONFIG.items() if k != 'table'}
        self.conn = psycopg2.connect(**connection_config)
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()

        self.consumer = Consumer({
            'bootstrap.servers': KAFKA_CONFIG['bootstrap_servers'],
            'group.id': KAFKA_CONFIG['scoring_group'],
            'auto.offset.reset': 'earliest'
        })
        self.consumer.subscribe([KAFKA_CONFIG['scoring_topic']])

    def process_messages(self):
        while True:
            message = self.consumer.poll(1.0)
            if not message:
                continue
            if message.error():
                logger.error(f"Kafka error: {message.error()}")
                continue
            try:
                data = json.loads(message.value().decode('utf-8'))[0]
                query = f"""
                    INSERT INTO {DB_CONFIG['table']} (transaction_id, score, fraud_flag)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (transaction_id) DO UPDATE SET
                    score = EXCLUDED.score,
                    fraud_flag = EXCLUDED.fraud_flag
                """
                self.cursor.execute(query, (
                    data["transaction_id"],
                    float(data["score"]),
                    int(data["fraud_flag"]),
                ))
  
            except Exception as e:
                logger.error(f"Error processing message: {e}")


    def close(self):
        self.cursor.close()
        self.conn.close()

if __name__ == "__main__":
    logger.info('Starting writer service...')
    writer = EventsWriter()
    try:
        writer.process_messages()
    except KeyboardInterrupt:
        logger.info('Service stopped by user')
    finally:
        writer.close()

