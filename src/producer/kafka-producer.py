
from src.config import BOOTSTRAP_SERVERS, RAW_TOPIC
from kafka import KafkaProducer
import requests
import json
import time

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda m: json.dumps(m).encode("utf-8"),
    key_serializer=lambda k: json.dumps(k).encode("utf-8") if k is not None else None,
    acks="all",
    enable_idempotence=True,
    retries=5
)

def _request_api(url: str, session: requests.Session) -> dict:
    try:
        response = session.get(url)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error occurred while requesting API: {e}")
        return {}

def get_orders() -> list:
    """Requests the orders from the event generator and returns them so kafka can send them to the topic."""

    n = 0
    while True:
        with requests.Session() as session:

            if n >= 100000000:
                break

            response = _request_api("http://localhost:8000/", session)

            if response:
                producer.send(RAW_TOPIC, value=response, key={"order_id": response.get("order_id")})
                time.sleep(5)
                n += 1

            else:
                raise ValueError("No response received from the event generator API.")

    producer.flush()
    
