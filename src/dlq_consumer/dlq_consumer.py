from src.config import CONTRACT, DQL_TOPIC, BOOTSTRAP_SERVERS, RAW_TOPIC, SENDER_EMAIL, RECEIVER_EMAIL, SMTP_HOST, SMTP_PORT, SMTP_PASSWORD
from src.SchemaDiagnoser.fn import check_schema
from kafka import KafkaConsumer, KafkaProducer
from email.message import EmailMessage
import smtplib
import logging
import json

logger = logging.getLogger(__name__)
consumer = KafkaConsumer(
    DQL_TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_deserializer=lambda m: json.loads(m),
    key_deserializer=lambda k: json.loads(k) if k else None,
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    group_id="dlq-consumer-group"
)


def _resend_to_raw_topic(message: dict):
    """Resends the message to the RAW_TOPIC after temporal adaptation and validation"""

    producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda m: json.dumps(m),
    key_serializer=lambda k: json.dumps(k) if k else None,
    acks="all",
    enable_idempotence=True,
    retries=5
    )

    diagnosis = check_schema(schema_contract=CONTRACT, incoming=[message])

    if diagnosis['valid']:
        producer.send(RAW_TOPIC, value=message.value, key=message.key)
        logger.info(f"Message with key: {message.key} has been successfully adapted and resent to the RAW_TOPIC.")
    else:
        logger.error(f"Message with key: {message.key} failed validation after adaptation and will not be resent.")


def _temporal_adaptation(message: dict, diagnosis: dict) -> dict:
    #TODO: Check
 
    adapted = message.copy()
    review = diagnosis['review']
    explanation = diagnosis.get("explanation", {})
    category = explanation.get("category")

    if (review['status'] != "completed" or review.get("coverage") or 
        category not in ['schema_expansion', 'naming_change', 'mixed_changes']
        or not explanation.get("probability", 0) > 0.9):

        return None

    added = [c['column'] for c in diagnosis['changes'] if c['issues'] == 'added']
    original = set(message.keys()) - set(added)

    for column in added:
        if category == "schema_expansion":
            adapted.pop(column)
            continue

        matches = [name for name in original
                   if name.replace("_", "").casefold() == column.replace("_", "").casefold()]

        if len(matches) == 1:
            value, original_value = adapted.get(column), adapted[matches[0]]
            if ((value is None or isinstance(value, (str, int, float, bool)))
                    and type(value) is type(original_value) and value == original_value):
                adapted.pop(column)
    return adapted
    

def _diagnose_failed_event(message: dict):

    return check_schema(schema_contract=CONTRACT, incoming=[message.value],
                    ai_review=True, context="Failed order from a store of products of curly hair")

def _send_alert(body: str):
    try:
        with smtplib.SMTP(host=SMTP_HOST, port=SMTP_PORT) as server:
            server.starttls()
            server.login(SENDER_EMAIL, SMTP_PASSWORD)
            msg = EmailMessage()
            msg.set_content(body)
            msg['Subject'] = 'Message added to DLQ Alert'
            msg['From'] = SENDER_EMAIL
            msg['To'] = RECEIVER_EMAIL
            server.send_message(msg)
            logger.info("Alert email sent successfully.")
    except smtplib.SMTPException as e:
        logger.error(f"Failed to send alert email: {e}")

def dlq_consumer():
    for message in consumer:
        logger.info(f"Starting diagnosis for message with key: {message.key} from partition: {message.partition} and offset: {message.offset}, that has been sent to the DLQ.")
        diagnosis = _diagnose_failed_event(message.value['original_message'])

        body = f"Message with key: {message.key} has failed validation and been sent to the DLQ.\n\nOriginal Message: {message.value['original_message']}\n\nFailure Metadata: {message.value['failure_metedata']}\n\nDiagnosis: {diagnosis}"
        _send_alert(body)

        message_adapted = _temporal_adaptation(message.value, diagnosis)

        if message_adapted:
            _resend_to_raw_topic(message_adapted)

        else:
            pass #TODO: Decide what to do here. We can either leave it in the DLQ or upload it to a database




 
