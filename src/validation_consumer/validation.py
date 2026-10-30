from src.config import BOOTSTRAP_SERVERS, PROCESSED_TOPIC, RAW_TOPIC, DLQ_TOPIC ,CONTRACT
from src.SchemaDiagnoser.fn import check_schema
from kafka import KafkaConsumer, KafkaProducer
import datetime
import logging
import json

logger = logging.getLogger(__name__)


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
n = 0

for message in consumer:
    logger.info(f"Message with key: {message.key} has been received from partition: {message.partition} and offset: {message.offset}")
    n += 1

    diagnosis = check_schema(schema_contract=CONTRACT, incoming=[message.value])

    if diagnosis['valid']:
        logger.info(f"Message with key: {message.key} has been validated successfully. Sending to the processed topic.")
        producer.send(PROCESSED_TOPIC, value=message, key=message.key)

    else:
        metadata = {
        "event_id": f"evt-{n}",
        "source_topic": RAW_TOPIC,
        "source_partition": message.partition,
        "source_offset": message.offset,
        "contract_version": CONTRACT.get("version"),
        "recovery_attempts": message.value.get("recovery_attempts", 0),
        "original_payload": message.value
        }

        enriched_message = {
            "failure_metedata": metadata,
            "original_message": message.value}

        
        logger.error(f"Message with key: {message.key} has failed validation and been sent to the DLQ.")
        producer.send(DLQ_TOPIC, value=enriched_message, key=message.key)

    message.commit()





