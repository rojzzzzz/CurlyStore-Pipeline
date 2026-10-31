from dotenv import load_dotenv
from pathlib import Path
import logging
import yaml
import os

load_dotenv()

logger = logging.getLogger(__name__)

def _get_env(value: str) -> str:
    env_val = os.getenv(value)
    if env_val is None:
        logger.error(f"Environment variable {value} is not set.")
        raise ValueError(f"Environment variable {value} is not set.")
    return env_val

def read_contract(contract_path: Path | str) -> dict:
    try:
        with open(contract_path, "r") as file:
            contract = yaml.safe_load(file)
            return contract
    except FileNotFoundError:
        logger.error(f"Contract file not found at {contract_path}.")
        raise
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML file: {e}")
        raise 


BOOTSTRAP_SERVERS = ["localhost:9092"]
RAW_TOPIC = "raw-orders"
VALIDATED_TOPIC = "processed-orders"
DLQ_TOPIC = "dlq-orders"
PARKING_LOT_TOPIC = "parking-lot-orders"
RECOVERY_ATTEMPTS_THRESHOLD = 3

CONTRACT_PATH = Path(__file__).parent / "data_contract.yml"
CONTRACT = read_contract(CONTRACT_PATH)

SENDER_EMAIL = _get_env("SENDER_EMAIL")
RECEIVER_EMAIL = _get_env("RECEIVER_EMAIL")
SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 465
SMTP_PASSWORD = _get_env("SMTP_PASSWORD")

DATABASE_SERVER = _get_env("DATABASE_SERVER")
DATABASE_NAME = _get_env("DATABASE_NAME")
DATABASE_USER = _get_env("DATABASE_USER")
DATABASE_PASSWORD = _get_env("DATABASE_PASSWORD")
SCHEMA = "curly_store"
EVENTS_TABLE = "processed_events"
