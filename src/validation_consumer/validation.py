from src.config import BOOTSTRAP_SERVERS, PROCESSED_TOPIC, RAW_TOPIC, DLQ_TOPIC ,CONTRACT
from src.SchemaDiagnoser.fn import check_schema
from kafka import KafkaConsumer, KafkaProducer
import datetime
import logging
import json

#TODO Implement the logger configuration in a separate file and import it here
logger = logging.getLogger(__name__)

def transform_message(message: dict) -> dict:
    """Transforms the incoming, already validated message into the desired format for further processing"""

    pass

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda m: json.dumps(m),
    key_serializer=lambda k: json.dumps(k) if k else None,
    acks="all",
    enable_idempotence=True,
    retries=5
)

consumer = KafkaConsumer(
    RAW_TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_deserializer=lambda m: json.loads(m),
    key_deserializer=lambda k: json.loads(k) if k else None,
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    group_id="validation-consumer-group"
)

logger.info(f"Validation consumer started and listening to the topic: {PROCESSED_TOPIC}")

#Define EXACTLY-ONCE, AT-LEAST-ONCE, and AT-MOST-ONCE processing semantics for the consumer
for message in consumer:
    logger.info(f"Message with key: {message.key} has been received from partition: {message.partition} and offset: {message.offset}")
    diagnosis = check_schema(schema_contract=CONTRACT, incoming=[message.value])

    if diagnosis['valid']:
        message = transform_message(message.value)
        logger.info(f"Message with key: {message.key} has been validated and transformed successfully.")
        producer.send(PROCESSED_TOPIC, value=message, key=message.key)

    else:
        metadata = {
            "source_topic": RAW_TOPIC,
            "source_partition": message.partition,
            "source_offset": message.offset,
            "failed_at": datetime.now().isoformat()
        }

        enriched_message = {
            "failure_metedata": metadata,
            "original_message": message.value}

        
        logger.error(f"Message with key: {message.key} has failed validation and been sent to the DLQ.")
        producer.send(DLQ_TOPIC, value=enriched_message, key=message.key)



