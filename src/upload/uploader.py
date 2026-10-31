from src.config import (DATABASE_SERVER, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD, SCHEMA,
                        EVENTS_TABLE)
from mssql_python import connect, OperationalError, ConnectionStringParseError
import datetime
import logging


logger = logging.getLogger(__name__)



def _check_event_existance(cursor, event_id: str) -> bool:
    """Checks if the event has already been processed."""

    query = """SELECT event_id
               FROM processed_events
               WHERE event_id = %(event_id)s"""

    cursor.execute(query, {"event_id": event_id})

    return cursor.fetchone()

def _insert_event(cursor, event_id: str, event_type: str, ocurred_at: str | datetime.datetime):
    """Inserts the new event into the processed_events table"""

    query = """INSERT INTO %(schema)s.%(table)s (event_id, event_type, ocurred_at)
               VALUES (%(event_id)s, %(event_type)s, %(ocurred_at)s)"""

    cursor.execute(query, {
        "schema": SCHEMA,
        "table": EVENTS_TABLE,
        "event_id": event_id,
        "event_type": event_type,
        "ocurred_at": ocurred_at
    })
    logger.info(f"Inserted event_id {event_id} into processed_events table.")

def _insert_order(cursor, message: dict):
    """Inserts the new order into the orders table"""

    query = """INSERT INTO %(schema)s.orders (order_id, sale_event_id, store_id, product_id, quantity,
               order_date, status) VALUES (%(order_id)s, %(sale_event_id)s, %(store_id)s, %(product_id)s, %(quantity)s,
               %(order_date)s, %(status)s)"""

    try:
        cursor.execute(query, {
            "schema": SCHEMA,
            "order_id": message.get("order_id"),
            "sale_event_id": message.get("event_id"),
            "store_id": message.get("store_id"),
            "product_id": message.get("product_id"),
            "quantity": message.get("quantity"),
            "order_date": message.get("order_date"),
            "status": message.get("status")
        })
        logger.info(f"Inserted order_id {message.get('order_id')} into orders table.")
    except OperationalError as e:
        logger.error(f"Operational error while inserting order: {e}")
        raise

def upload_event(message: dict):
    try:
        with connect(server=DATABASE_SERVER, database=DATABASE_NAME, uid=DATABASE_USER,
                     pwd=DATABASE_PASSWORD, encrypt="yes", trust_server_certificate="no",
                     timeout=60) as conn:
            with conn.cursor() as cursor:
                event_id = message.get("event_id")
                if not _check_event_existance(cursor, event_id):
                    _insert_event(cursor, event_id, message.get("event_type"), message.get("ocurred_at"))

                else:
                    logger.warning(f"Event with event_id {event_id} already exists in the processed_events table. Skipping insertion.")
                    return
                

    except ConnectionStringParseError as e:
        logger.error(f"Invalid connection string: {e}")
        raise

    except OperationalError as e:
        logger.error(f"Operational error: {e}")
        raise
