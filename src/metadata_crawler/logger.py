"""Logging utilities."""

import logging
import logging.config
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Optional, cast

import appdirs
from rich.console import Console
from rich.logging import RichHandler

THIS_NAME = "metadata-crawler"

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s %(levelname)s: %(name)s - %(message)s",
)

logging.config.dictConfig(
    {
        "version": 1,
        # keep existing handlers
        "disable_existing_loggers": False,
        "root": {
            "level": "CRITICAL",
            "handlers": ["default"],
        },
        "formatters": {
            "standard": {
                "format": "%(asctime)s %(levelname)s: %(name)s - %(message)s",
            },
        },
        "handlers": {
            "default": {
                "class": "logging.StreamHandler",
                "formatter": "standard",
                "level": "CRITICAL",
            },
        },
    }
)


class Logger(logging.Logger):
    """Custom Logger defining the logging behaviour."""

    logfmt: str = "%(name)s: %(message)s"
    filelogfmt: str = "%(asctime)s %(levelname)s: %(name)s - %(message)s"
    datefmt: str = "%Y-%m-%dT%H:%M:%S"
    no_debug: list[str] = ["watchfiles", "httpcore", "pymongo", "pika"]

    def __init__(
        self,
        name: Optional[str] = None,
        level: Optional[int] = None,
        suffix: Optional[str] = None,
    ) -> None:
        """Instantiate this logger only once and for all."""
        self.level = level or int(
            cast(str, os.getenv("MDC_LOG_LEVEL", str(logging.CRITICAL)))
        )
        name = name or THIS_NAME
        logger_format = logging.Formatter(self.logfmt, self.datefmt)
        self.file_format = logging.Formatter(self.filelogfmt, self.datefmt)
        self._logger_file_handle: Optional[RotatingFileHandler] = None
        self._logger_stream_handle = RichHandler(
            rich_tracebacks=True,
            tracebacks_max_frames=10,
            tracebacks_extra_lines=5,
            show_path=True,
            console=Console(
                soft_wrap=False,
                force_jupyter=False,
                stderr=True,
            ),
        )
        self._logger_stream_handle.setFormatter(logger_format)
        self._logger_stream_handle.setLevel(self.level)
        super().__init__(name, self.level)

        self.propagate = False
        self.handlers = [self._logger_stream_handle]
        (
            self.add_file_handle(suffix=suffix)
            if os.getenv("MDC_LOG_INIT", "0") == "1"
            else None
        )

    def set_level(self, level: int) -> None:
        """Set the logger level to level."""
        for handler in self.handlers:
            log_level = level
            if isinstance(handler, RotatingFileHandler):
                log_level = min(level, logging.CRITICAL)
            handler.setLevel(log_level)
        self.setLevel(level)
        self.level = level

    def error(
        self,
        msg: object,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Log an error. When log level is smaller than INFO, log exceptions."""
        if self.level < logging.INFO:
            kwargs.setdefault("exc_info", True)
        self._log(logging.ERROR, msg, args, **kwargs)

    def add_file_handle(
        self,
        suffix: Optional[str] = None,
        level: int = logging.CRITICAL,
    ) -> None:
        """Add a file log handle to the logger."""
        suffix = suffix or os.getenv("MDC_LOG_SUFFIX", "")
        base_name = f"{THIS_NAME}-{suffix}" if suffix else THIS_NAME
        log_dir = Path(os.getenv("MDC_LOG_DIR", appdirs.user_log_dir(THIS_NAME)))
        log_dir.mkdir(exist_ok=True, parents=True)
        logger_file_handle = RotatingFileHandler(
            log_dir / f"{base_name}.log",
            mode="a",
            maxBytes=5 * 1024**2,
            backupCount=5,
            encoding="utf-8",
            delay=False,
        )
        logger_file_handle.setFormatter(self.file_format)
        logger_file_handle.setLevel(self.level)
        self.addHandler(logger_file_handle)


logger = Logger()


def get_level_from_verbosity(verbosity: int) -> int:
    """Calculate the log level from a verbosity."""
    return max(logging.CRITICAL - 10 * verbosity, -1)


def apply_verbosity(
    level: Optional[int] = None, suffix: Optional[str] = None
) -> int:
    """Set the logging level of the handlers to a certain level."""
    level = logger.level if level is None else level
    old_level = logger.level
    level = get_level_from_verbosity(level)
    logger.set_level(level)
    logger.add_file_handle(suffix, level)

    return old_level
