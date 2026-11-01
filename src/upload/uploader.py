from src.config import (DATABASE_SERVER, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD, SCHEMA,
                        EVENTS_TABLE, VALIDATED_TOPIC, BOOTSTRAP_SERVERS, PARKING_LOT_TOPIC)
from mssql_python import connect, OperationalError, ConnectionStringParseError
from src.messages import decode_message, event_data
from decimal import Decimal
import datetime
import json
import logging


logger = logging.getLogger(__name__)
SQL_SCHEMA = "[" + SCHEMA.replace("]", "]]") + "]"
SQL_EVENTS_TABLE = "[" + EVENTS_TABLE.replace("]", "]]") + "]"


class RefundRejected(ValueError):
    """A refund amount that cannot be accepted by retrying the transaction."""


def _check_event_existance(cursor, event_id: str) -> bool:
    """Checks if the event has already been processed."""

    query = f"""SELECT event_id
               FROM {SQL_SCHEMA}.{SQL_EVENTS_TABLE}
               WHERE event_id = %(event_id)s"""

    cursor.execute(query, {"event_id": event_id})

    return cursor.fetchone()

def _insert_event(cursor, event_id: str, event_type: str, occurred_at: str | datetime.datetime):
    """Inserts the new event into the processed_events table"""

    query = f"""INSERT INTO {SQL_SCHEMA}.{SQL_EVENTS_TABLE} (event_id, event_type, occurred_at)
               VALUES (%(event_id)s, %(event_type)s, %(occurred_at)s)"""

    cursor.execute(query, {
        "event_id": event_id,
        "event_type": event_type,
        "occurred_at": occurred_at
    })
    logger.info(f"Inserted event_id {event_id} into processed_events table.")

def _insert_order(cursor, message: dict):
    """Inserts the new order into the orders table"""

    query = f"""INSERT INTO {SQL_SCHEMA}.orders (order_id, event_id, store_id, product_id, quantity,
               total_price, order_date, status) VALUES (%(order_id)s, %(event_id)s, %(store_id)s, %(product_id)s, %(quantity)s,
               %(total_price)s, %(order_date)s, %(status)s)"""

    try:
        cursor.execute(query, {
            "order_id": message.get("order_id"),
            "event_id": message.get("event_id"),
            "store_id": message.get("store_id"),
            "product_id": message.get("product_id"),
            "quantity": message.get("quantity"),
            "total_price": message.get("total_price"),
            "order_date": message.get("order_date"),
            "status": message.get("status")
        })
        logger.info(f"Inserted order_id {message.get('order_id')} into orders table.")
    except OperationalError as e:
        logger.error(f"Operational error while inserting order: {e}")
        raise

def _insert_refund(cursor, message: dict):
    """ Inserts the new refund into the refunds table"""

    cursor.execute(f"""SELECT total_price FROM {SQL_SCHEMA}.orders WITH (UPDLOCK, HOLDLOCK)
                       WHERE order_id = %(order_id)s""", {"order_id": message["order_id"]})
    order = cursor.fetchone()

    if order is None:
        raise ValueError("Refund order does not exist.")
    cursor.execute(f"""SELECT COALESCE(SUM(refund_amount), 0) FROM {SQL_SCHEMA}.refunds
                       WHERE order_id = %(order_id)s""", {"order_id": message["order_id"]})

    refunded = cursor.fetchone()[0]
    amount = Decimal(str(message["refund_amount"]))

    if not amount.is_finite() or amount <= 0 or amount + refunded > order[0]:
        raise RefundRejected("Refund amount must be positive and cannot exceed the remaining order total.")

    query = f"""INSERT INTO {SQL_SCHEMA}.refunds (refund_id, event_id, order_id, refund_amount, refunded_at,
            reason) VALUES (%(refund_id)s, %(event_id)s, %(order_id)s, %(refund_amount)s, %(refunded_at)s,
            %(reason)s)"""

    try:
        cursor.execute(query, {
            "refund_id": message.get("refund_id"),
            "event_id": message.get("event_id"),
            "order_id": message.get("order_id"),
            "refund_amount": message.get("refund_amount"),
            "refunded_at": message.get("refunded_at"),
            "reason": message.get("reason")
        })
        cursor.execute(f"""UPDATE {SQL_SCHEMA}.orders SET status = %(status)s
                           WHERE order_id = %(order_id)s""", {
            "order_id": message["order_id"],
            "status": "refunded" if amount + refunded == order[0] else "partially_refunded",
        })
        logger.info(f"Inserted refund_id {message.get('refund_id')} into refunds table.")
    except OperationalError as e:
        logger.error(f"Operational error while inserting refund: {e}")
        raise


def upload_event():
    """Uploads the events received from the validation consumer to the database."""

    from kafka import KafkaConsumer, KafkaProducer, TopicPartition
    from kafka.structs import OffsetAndMetadata


    consumer = KafkaConsumer(
        VALIDATED_TOPIC, bootstrap_servers=BOOTSTRAP_SERVERS,
        value_deserializer=decode_message,
        auto_offset_reset="earliest", enable_auto_commit=False,
        group_id="upload-consumer-group", max_poll_records=1)

    producer = None

    try:
        for message in consumer:
            payload = message.value
            event_type, _ = event_data(payload)
            event_id = payload["event_id"]

            try:
                with connect(server=DATABASE_SERVER, database=DATABASE_NAME, uid=DATABASE_USER,
                            pwd=DATABASE_PASSWORD, encrypt="yes", trust_server_certificate="no",
                            timeout=60) as conn:
                    with conn.cursor() as cursor:
                        if not _check_event_existance(cursor, event_id):
                            occurred_at = payload["order_date" if event_type == "order" else "refunded_at"]
                            _insert_event(cursor, event_id, event_type, occurred_at)
                            if event_type == "order":
                                _insert_order(cursor, payload)
                            else:
                                _insert_refund(cursor, payload)
                        else:
                            logger.warning(f"Event with event_id {event_id} already exists in the processed_events table. Skipping insertion.")

            except RefundRejected as exc:
                # The connection context has rolled back before we park the event.
                if producer is None:
                    producer = KafkaProducer(
                        bootstrap_servers=BOOTSTRAP_SERVERS,
                        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                        acks="all", enable_idempotence=True, retries=5,
                        max_in_flight_requests_per_connection=1,
                    )
                parked = {"original_message": payload, "failure_metadata": {
                    "source_topic": message.topic, "source_partition": message.partition,
                    "source_offset": message.offset, "recovery_reason": str(exc),
                    "failed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                }}
                producer.send(PARKING_LOT_TOPIC, value=parked, key=message.key,
                              headers=message.headers or []).get(timeout=30)

            consumer.commit({TopicPartition(message.topic, message.partition):
                             OffsetAndMetadata(message.offset + 1, "")})

    except ConnectionStringParseError as e:
        logger.error(f"Invalid connection string: {e}")
        raise

    except OperationalError as e:
        logger.error(f"Operational error: {e}")
        raise

    finally:
        consumer.close()
        if producer is not None:
            producer.close()
