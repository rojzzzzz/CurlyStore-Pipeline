from pathlib import Path
import logging
import yaml
import os

logger = logging.getLogger(__name__)

def _get_env(value: str) -> str:
    value = os.getenv(value)
    if value is None:
        logger.error(f"Environment variable {value} is not set.")
        raise ValueError(f"Environment variable {value} is not set.")
    return value

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
PROCESSED_TOPIC = "processed-orders"
DLQ_TOPIC = "dlq-orders"

CONTRACT_PATH = Path(__file__).parent / "data_contract.yml"
CONTRACT = read_contract(CONTRACT_PATH)