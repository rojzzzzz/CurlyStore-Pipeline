
from src.config import BOOTSTRAP_SERVERS, RAW_TOPIC, CONTRACT
from src.messages import event_data
from kafka import KafkaProducer
import requests
import json
import time
import uuid

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
        response = session.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error occurred while requesting API: {e}")
        return {}

def get_orders() -> None:
    """Requests the orders from the event generator and returns them so kafka can send them to the topic."""

    try:
        with requests.Session() as session:
            for _ in range(100000000):
                response = _request_api("http://localhost:8000/", session)
                if not response:
                    raise ValueError("No response received from the event generator API.")
                event_type, _ = event_data(response)
                response.setdefault('event_id', str(uuid.uuid4()))
                response.setdefault('event_version', str(CONTRACT['version']))
                response.setdefault('event_type', event_type)
                producer.send(RAW_TOPIC, value=response,
                              key={"order_id": response.get("order_id")}).get(timeout=30)
                time.sleep(5)
    finally:
        producer.close()
    
