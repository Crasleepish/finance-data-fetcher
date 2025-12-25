from __future__ import annotations

import gzip
import logging
import shutil
import sys
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

from config.settings import LoggingConfig


def setup_logging(config: LoggingConfig) -> None:
    handlers = _build_handlers(config)
    formatter = logging.Formatter(config.log_format, datefmt=config.date_format)

    for handler in handlers:
        handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(config.level)
    root_logger.handlers = []
    for handler in handlers:
        root_logger.addHandler(handler)

    for logger_name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logger = logging.getLogger(logger_name)
        logger.handlers = list(handlers)
        logger.setLevel(config.level)
        logger.propagate = False


def _build_handlers(config: LoggingConfig) -> list[logging.Handler]:
    handlers: list[logging.Handler] = []

    log_path = Path(config.log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    file_handler = _build_file_handler(config)
    handlers.append(file_handler)

    if config.console:
        console_handler = logging.StreamHandler(sys.stdout)
        handlers.append(console_handler)

    return handlers


def _build_file_handler(config: LoggingConfig) -> TimedRotatingFileHandler:
    handler = TimedRotatingFileHandler(
        filename=config.log_file,
        when=config.rotation_when,
        interval=config.rotation_interval,
        backupCount=config.backup_count,
        encoding=config.encoding,
        delay=True,
    )
    handler.suffix = "%Y%m%d"
    handler.namer = _gzip_namer
    handler.rotator = _gzip_rotator
    return handler


def _gzip_namer(default_name: str) -> str:
    return f"{default_name}.gz"


def _gzip_rotator(source: str, dest: str) -> None:
    with open(source, "rb") as source_handle:
        with gzip.open(dest, "wb") as dest_handle:
            shutil.copyfileobj(source_handle, dest_handle)
    Path(source).unlink(missing_ok=True)
