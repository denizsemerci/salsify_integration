from __future__ import annotations

import json
import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:  # type: ignore[override]
        log_record: Dict[str, Any] = {
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "time": self.formatTime(record, self.datefmt),
        }
        if record.exc_info:
            log_record["exc_info"] = self.formatException(record.exc_info)
        if hasattr(record, "extra") and isinstance(record.extra, dict):
            log_record.update(record.extra)
        return json.dumps(log_record, ensure_ascii=False)


def get_logger(name: str = "app", level: str | int = "INFO") -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    if isinstance(level, str):
        level_value = getattr(logging, level.upper(), logging.INFO)
    else:
        level_value = level

    logger.setLevel(level_value)

    logs_dir = Path(os.getenv("LOG_DIR", Path(__file__).resolve().parents[2] / "logs"))
    logs_dir.mkdir(parents=True, exist_ok=True)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(JsonFormatter())
    logger.addHandler(stream_handler)

    file_handler = RotatingFileHandler(logs_dir / "app.log", maxBytes=5_000_000, backupCount=3)
    file_handler.setFormatter(JsonFormatter())
    logger.addHandler(file_handler)

    logger.propagate = False
    return logger

