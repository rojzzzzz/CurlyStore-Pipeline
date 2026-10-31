from base64 import b64encode
import json


EVENT_METADATA = {"event_id", "event_type", "event_version"}


def decode_message(value):
    """Keep malformed bytes available for DLQ routing instead of failing the poll."""
    try:
        return json.loads(value)
    except (TypeError, ValueError, UnicodeError):
        return value


def preserve_original(value):
    if isinstance(value, bytes):
        return {"encoding": "base64", "data": b64encode(value).decode("ascii")}
    return value


def event_data(message: dict) -> tuple[str, dict]:
    if not isinstance(message, dict):
        raise TypeError("Event must be a JSON object.")
    event_type = message.get("event_type")
    if "event_type" not in message:
        event_type = "refund" if "refund_id" in message else "order" if "order_id" in message else None
    if event_type not in ("order", "refund"):
        raise ValueError("Event type must be order or refund.")
    return event_type, {k: v for k, v in message.items() if k not in EVENT_METADATA}
