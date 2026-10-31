from src.config import BOOTSTRAP_SERVERS, VALIDATED_TOPIC, RAW_TOPIC, DLQ_TOPIC, CONTRACT
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from src.SchemaDiagnoser.fn import check_schema
from src.messages import decode_message, event_data, preserve_original
from kafka.structs import OffsetAndMetadata
from uuid import NAMESPACE_URL, uuid5
import datetime
import logging
import json

logger = logging.getLogger(__name__)

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    acks="all", enable_idempotence=True, retries=5,
    max_in_flight_requests_per_connection=1,
)
consumer = KafkaConsumer(
    RAW_TOPIC, bootstrap_servers=BOOTSTRAP_SERVERS,
    value_deserializer=decode_message,
    auto_offset_reset="earliest", enable_auto_commit=False,
    group_id="validation-consumer-group", max_poll_records=1,
)

try:
    #Strategy used for idempotency: AT-LEAST-ONCE commit with idempotent producer (Check event_id existence in downstream systems to detect duplicates)
    for message in consumer:

        try:
            event_type, data = event_data(message.value)
            diagnosis = check_schema(CONTRACT['contract'][event_type], [data], ai_review=False, allow_extra=False)
        except (TypeError, ValueError) as exc:
            diagnosis = {"valid": False, "error": str(exc)}

        topic = VALIDATED_TOPIC
        payload = message.value.copy() if isinstance(message.value, dict) else message.value

        if diagnosis["valid"]:
            payload.setdefault('event_id', str(uuid5(NAMESPACE_URL, f"{message.topic}:{message.partition}:{message.offset}")))
            payload.setdefault('event_type', event_type)

        if not diagnosis["valid"]:
            topic = DLQ_TOPIC
            payload = {
                "original_message": preserve_original(message.value),
                "failure_metadata": {
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
        logger.info(f"Routed key={message.key} to {topic}")
finally:
    consumer.close()
    producer.close()
