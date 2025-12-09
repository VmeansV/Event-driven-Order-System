import json
import logging
import os
import uuid

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")


def get_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",  # Ждать подтверждения от всех реплик
            retries=3,
        )
        return producer
    except KafkaError as e:
        logger.error(f"Failed to create Kafka producer: {e}")
        return None


def get_consumer(*topics, group_id):
    try:
        consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=[KAFKA_BROKER],
            group_id=group_id,
            auto_offset_reset="earliest",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            enable_auto_commit=True,
        )
        return consumer
    except KafkaError as e:
        logger.error(f"Failed to create Kafka consumer: {e}")
        return None


def generate_message_id_header():
    return [("message_id", str(uuid.uuid4()).encode("utf-8"))]
