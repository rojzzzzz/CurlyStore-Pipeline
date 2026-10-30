from src.config import BOOTSTRAP_SERVERS, PROCESSED_TOPIC, RAW_TOPIC, DLQ_TOPIC, CONTRACT
from src.SchemaDiagnoser.fn import check_schema
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.structs import OffsetAndMetadata
import datetime
import logging
import json

logger = logging.getLogger(__name__)

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    key_serializer=lambda key: json.dumps(key).encode("utf-8") if key is not None else None,
    acks="all", enable_idempotence=True, retries=5,
    max_in_flight_requests_per_connection=1,
)
consumer = KafkaConsumer(
    RAW_TOPIC, bootstrap_servers=BOOTSTRAP_SERVERS,
    value_deserializer=json.loads,
    key_deserializer=lambda key: json.loads(key) if key else None,
    auto_offset_reset="earliest", enable_auto_commit=False,
    group_id="validation-consumer-group", max_poll_records=1,
)

try:
    for message in consumer:
        try:
            diagnosis = check_schema(CONTRACT, [message.value], ai_review=False, allow_extra=False)
        except (TypeError, ValueError) as exc:
            diagnosis = {"valid": False, "error": str(exc)}

        topic, payload = PROCESSED_TOPIC, message.value
        if not diagnosis["valid"]:
            topic = DLQ_TOPIC
            payload = {
                "original_message": message.value,
                "failure_metadata": {
                    "event_id": f"{message.topic}:{message.partition}:{message.offset}",
                    "source_topic": message.topic,
                    "source_partition": message.partition,
                    "source_offset": message.offset,
                    "contract_version": CONTRACT.get("version"),
                    "failed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    "diagnosis": diagnosis,
                },
            }

        producer.send(topic, value=payload, key=message.key,
                      headers=message.headers or []).get(timeout=30)
        consumer.commit({TopicPartition(message.topic, message.partition):
                         OffsetAndMetadata(message.offset + 1, "")})
        logger.info("Routed key=%s to %s", message.key, topic)
finally:
    consumer.close()
    producer.close()
