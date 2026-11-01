from src.config import (
    CONTRACT, DLQ_TOPIC, BOOTSTRAP_SERVERS, RAW_TOPIC, SENDER_EMAIL,
    RECEIVER_EMAIL, SMTP_HOST, SMTP_PORT, SMTP_PASSWORD,
    PARKING_LOT_TOPIC, RECOVERY_ATTEMPTS_THRESHOLD,
)
from src.messages import EVENT_METADATA, decode_message, event_data, preserve_original
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from src.SchemaDiagnoser.fn import check_schema
from decimal import Decimal, InvalidOperation
from kafka.structs import OffsetAndMetadata
from email.message import EmailMessage
import smtplib
import logging
import json
import math
import re



logger = logging.getLogger(__name__)
ATTEMPTS_HEADER = "recovery_attempts"


def _temporal_adaptation(message: dict, diagnosis: dict) -> dict | None:
    """Repair only mechanically verifiable drift supported by a complete AI review."""

    review = diagnosis.get("review", {})
    explanation = review.get("diagnosis") or {}
    category = explanation.get("category")

    if (review.get("status") != "completed" or review.get("coverage")
            or explanation.get("probability", 0) < 0.9
            or category not in {"schema_expansion", "naming_change",
                                "representation_change", "mixed_changes"}):
        return None

    adapted = message.copy()
    changes = diagnosis["changes"]
    added = {c["column"] for c in changes if c["issues"] == ["added"]}
    missing = {c["column"] for c in changes if c["issues"] == ["missing"]}
    normalize = lambda name: name.replace("_", "").casefold()

    for column in sorted(added):
        matches = [name for name in (message.keys() - added) | missing
                   if normalize(name) == normalize(column)]

        if len(matches) == 1 and category in {"naming_change", "mixed_changes"}:
            target = matches[0]

            if sum(normalize(name) == normalize(column) for name in added) != 1:
                return None
            if target in adapted:
                value, existing = adapted[column], adapted[target]
                if (type(value) not in (str, int, float, bool, type(None))
                        or type(value) is not type(existing) or value != existing):
                    return None
            else:
                pair = [next(i for i, c in enumerate(changes) if c["column"] == name)
                        for name in (target, column)]
                if not any(r.get("category") == "possible_rename"
                           and r.get("change_indices") == pair
                           and r.get("probability", 0) >= 0.9
                           and not r.get("competing_changes")
                           for r in review.get("renames", [])):
                    return None
                adapted[target] = adapted[column]
            del adapted[column]
        elif category == "schema_expansion":
            del adapted[column]
        else:
            return None

    for change in changes:
        if change["issues"] != ["type_changed"]:
            continue
        if category not in {"representation_change", "mixed_changes"}:
            return None
        column, expected = change["column"], change["expected"]
        value = adapted[column]
        try:
            if expected == "int" and type(value) is str and re.fullmatch(r"-?(0|[1-9][0-9]*)", value):
                adapted[column] = int(value)
            elif expected == "float" and (type(value) is int or
                    (type(value) is str and re.fullmatch(r"-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?", value))):
                number = float(value)
                if not math.isfinite(number) or Decimal(str(number)) != Decimal(value):
                    return None
                adapted[column] = number
            else:
                return None
        except (ValueError, OverflowError, InvalidOperation):
            return None
    return adapted if added or any(c["issues"] == ["type_changed"] for c in changes) else None


def _send_alert(record, parked):
    try:
        metadata = parked["failure_metadata"]
        msg = EmailMessage()
        msg.set_content(
            f"A record could not be recovered and was selected for {PARKING_LOT_TOPIC}.\n\n"
            f"Record key: {record.key!r}\n"
            f"Validation failed at: {metadata.get('failed_at', 'Unavailable')}\n"
            f"Recovery reason: {metadata.get('recovery_reason', 'Unavailable')}\n\n"
            f"Diagnosis:\n{json.dumps(metadata.get('diagnosis'), indent=2, default=str)}"
        )
        reason = str(metadata.get("recovery_reason") or "unknown").replace("\r", " ").replace("\n", " ")[:100]
        key = repr(record.key).replace("\r", " ").replace("\n", " ")[:100]
        msg["Subject"] = f"Parking lot: {key} - {reason}"
        msg["From"], msg["To"] = SENDER_EMAIL, RECEIVER_EMAIL
        transport = smtplib.SMTP_SSL if SMTP_PORT == 465 else smtplib.SMTP
        with transport(SMTP_HOST, SMTP_PORT, timeout=10) as server:
            if SMTP_PORT != 465:
                server.starttls()
            if SMTP_PASSWORD:
                server.login(SENDER_EMAIL, SMTP_PASSWORD)
            server.send_message(msg)
    except (OSError, smtplib.SMTPException):
        logger.exception("Failed to send DLQ alert")


def _recover(record):
    """Return the destination, payload and headers; never publish a failed candidate."""

    envelope = record.value if isinstance(record.value, dict) else {"original_message": preserve_original(record.value)}

    metadata = envelope.get("failure_metadata", envelope.get("failure_metedata", {}))
    metadata = metadata if isinstance(metadata, dict) else {}
    headers = [(k, v) for k, v in (record.headers or []) if k != ATTEMPTS_HEADER]
    attempts, diagnosis, final, reason = 0, None, None, "no_safe_correction"

    try:
        counts = [metadata.get("recovery_attempts", 0)]

        counts.extend(v.decode("ascii") for k, v in (record.headers or []) if k == ATTEMPTS_HEADER)
        if any(type(n) not in (int, str) or not re.fullmatch(r"[0-9]+", str(n)) for n in counts):
            raise ValueError("Invalid recovery counter")
        attempts = max(map(int, counts))
        payload = envelope["original_message"]
        event_type, data = event_data(payload)
        contract = CONTRACT['contract'][event_type]
        diagnosis = check_schema(contract, [data], ai_review=attempts < RECOVERY_ATTEMPTS_THRESHOLD, allow_extra=False,
                                 context=f"Failed {event_type} event from a curly hair products store")

        if diagnosis["valid"]:
            headers.append((ATTEMPTS_HEADER, str(attempts).encode("ascii")))
            return RAW_TOPIC, payload, headers, diagnosis

        if attempts >= RECOVERY_ATTEMPTS_THRESHOLD:
            reason = "recovery_limit_reached"

        else:
            attempts += 1
            adapted = _temporal_adaptation(data, diagnosis)
            if adapted is not None:
                final = check_schema(contract, [adapted], ai_review=False, allow_extra=False)
                if final["valid"]:
                    headers.append((ATTEMPTS_HEADER, str(attempts).encode("ascii")))
                    logger.info("Recovered key=%s: %s", record.key, diagnosis)
                    recovered = {k: v for k, v in payload.items() if k in EVENT_METADATA}
                    return RAW_TOPIC, {**recovered, **adapted}, headers, diagnosis
                reason = "correction_failed_validation"

    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        reason = f"invalid_recovery_input: {exc}"

    parked = {**envelope, "failure_metadata": {**metadata, "recovery_attempts": attempts,
              "recovery_reason": reason, "diagnosis": diagnosis, "revalidation": final}}

    headers.append((ATTEMPTS_HEADER, str(attempts).encode("ascii")))
    return PARKING_LOT_TOPIC, parked, headers, diagnosis


def dlq_consumer():
    consumer = KafkaConsumer(
        DLQ_TOPIC, bootstrap_servers=BOOTSTRAP_SERVERS,
        value_deserializer=decode_message,
        auto_offset_reset="earliest", enable_auto_commit=False, group_id="dlq-consumer-group",
        max_poll_records=1,
    )
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all", enable_idempotence=True, retries=5, max_in_flight_requests_per_connection=1,
    )
    try:
        #AT-LEAST-ONCE
        for record in consumer:
            topic, payload, headers, diagnosis = _recover(record)

            producer.send(topic, value=payload, key=record.key, headers=headers).get(timeout=30)
            consumer.commit({TopicPartition(record.topic, record.partition):
                             OffsetAndMetadata(record.offset + 1, "")})
            logger.info(f"Routed key={record.key} to {topic}")
            if topic == PARKING_LOT_TOPIC:
                _send_alert(record, payload)
    finally:
        consumer.close()
        producer.close()



