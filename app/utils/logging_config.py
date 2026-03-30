"""Application logging helpers."""

from __future__ import annotations

import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path

_LOGGING_INITIALIZED = False


def setup_logging() -> None:
    """Configure console and file logging once per process."""

    global _LOGGING_INITIALIZED
    if _LOGGING_INITIALIZED:
        return

    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    log_path = Path(os.getenv("LOG_FILE_PATH", "logs/review_analyzer.log"))
    log_path.parent.mkdir(parents=True, exist_ok=True)

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    root = logging.getLogger()
    root.setLevel(getattr(logging, log_level, logging.INFO))

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    file_handler = RotatingFileHandler(
        filename=log_path,
        maxBytes=1_000_000,
        backupCount=3,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    root.handlers.clear()
    root.addHandler(stream_handler)
    root.addHandler(file_handler)

    _LOGGING_INITIALIZED = True
