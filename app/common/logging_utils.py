import json
import logging
import os
from datetime import datetime
from typing import Any


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        extra_fields = getattr(record, "extra_fields", None)
        if isinstance(extra_fields, dict):
            payload.update(extra_fields)

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload, ensure_ascii=False)


def get_json_logger(name: str, log_dir: str, filename: str) -> logging.Logger:
    # Partition logs by month: logs/YYYY-MM/filename
    year_month = datetime.utcnow().strftime("%Y-%m")
    partitioned_dir = os.path.join(log_dir, year_month)
    os.makedirs(partitioned_dir, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if logger.handlers:
        return logger

    file_handler = logging.FileHandler(os.path.join(partitioned_dir, filename))
    file_handler.setFormatter(JsonFormatter())

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(JsonFormatter())

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    logger.propagate = False
    return logger


def log_json(logger: logging.Logger, level: str, message: str, **kwargs: Any) -> None:
    extra = {"extra_fields": kwargs}
    getattr(logger, level.lower())(message, extra=extra)