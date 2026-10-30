"""Shared logging for the event API, Kafka producer, and pipeline consumers.

Call configure_logging() once per process at startup, then use
logging.getLogger(__name__) in each module. Importing this module has no side effects.
"""

import logging
import logging.config
import os
from pathlib import Path
import re
import time


DEFAULT_LOG_DIR = Path(__file__).resolve().parents[1] / "logs"
_SECRET_ENV_VARS = ("TYPESAFE_API_KEY", "SMTP_PASSWORD")
_CREDENTIAL_PATTERN = re.compile(
    r"""(?i)(\b(?:api[_-]?key|(?:smtp|sasl)[_-]?password|password|access[_-]?token)"""
    r"""\b["']?\s*[:=]\s*)(?:"[^"]*"|'[^']*'|[^\s&,;}]+)"""
)
_AUTHORIZATION_PATTERN = re.compile(
    r"""(?i)(\bauthorization\b["']?\s*[:=]\s*["']?(?:(?:bearer|basic)\s+)?)"""
    r"""[^"'\s,;}]+"""
)


class RedactingUTCFormatter(logging.Formatter):
    """Redact credentials from messages and tracebacks, using UTC timestamps."""

    converter = time.gmtime

    def format(self, record: logging.LogRecord) -> str:
        message = super().format(record)
        for name in _SECRET_ENV_VARS:
            secret = os.environ.get(name)
            if secret:
                message = message.replace(secret, "[REDACTED]")
        message = _CREDENTIAL_PATTERN.sub(r"\1[REDACTED]", message)
        return _AUTHORIZATION_PATTERN.sub(r"\1[REDACTED]", message)


def configure_logging(
    log_dir: Path | str | None = None,
    *,
    service_name: str | None = None,
) -> Path:
    """Send INFO+ to stderr, DEBUG+ to an all log, and ERROR+ to an errors log.

    CURLY_STORE_LOG_DIR overrides the default project logs directory.
    CURLY_STORE_SERVICE labels files unless service_name is supplied. Files use
    <service>.<pid>.all.log and <service>.<pid>.errors.log so separate pipeline
    processes do not rotate each other's files. Each rotates at 10 MB with five
    backups. Deployments should also expire files left by terminated processes.

    Repeated calls replace the configuration without duplicating output. Set this
    up after worker processes start; file handlers must not be shared across forks.
    Returns the resolved log directory.
    """
    log_dir = Path(
        log_dir or os.environ.get("CURLY_STORE_LOG_DIR") or DEFAULT_LOG_DIR
    ).expanduser().resolve()
    service = service_name or os.environ.get("CURLY_STORE_SERVICE") or "curly-store"
    service = re.sub(r"[^a-zA-Z0-9_-]+", "-", service).strip("-") or "curly-store"
    prefix = f"{service}.{os.getpid()}"
    log_dir.mkdir(parents=True, exist_ok=True)

    file_handler = {
        "class": "logging.handlers.RotatingFileHandler",
        "formatter": "project",
        "maxBytes": 10 * 1024 * 1024,
        "backupCount": 5,
        "encoding": "utf-8",
        "delay": True,
    }
    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "project": {
                    "()": RedactingUTCFormatter,
                    "format": "%(asctime)s %(levelname)-8s %(name)s [%(process)d]: %(message)s",
                    "datefmt": "%Y-%m-%dT%H:%M:%SZ",
                }
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "level": "INFO",
                    "formatter": "project",
                    "stream": "ext://sys.stderr",
                },
                "all_file": {
                    **file_handler,
                    "level": "DEBUG",
                    "filename": str(log_dir / f"{prefix}.all.log"),
                },
                "errors_file": {
                    **file_handler,
                    "level": "ERROR",
                    "filename": str(log_dir / f"{prefix}.errors.log"),
                },
            },
            "root": {
                "level": "DEBUG",
                "handlers": ["console", "all_file", "errors_file"],
            },
            "loggers": {
                "kafka": {"level": "INFO"},
                "urllib3": {"level": "WARNING"},
                "httpx": {"level": "WARNING"},
                "httpcore": {"level": "WARNING"},
                "uvicorn": {"level": "INFO", "handlers": [], "propagate": True},
                "uvicorn.error": {"level": "INFO", "handlers": [], "propagate": True},
                "uvicorn.access": {"level": "INFO", "handlers": [], "propagate": True},
            },
        }
    )
    return log_dir
