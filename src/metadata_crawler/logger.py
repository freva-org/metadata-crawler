import logging
import logging.config
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Optional

import appdirs
from rich.console import Console
from rich.logging import RichHandler

THIS_NAME = "data-crawler"

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
            "level": "WARNING",
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
                "level": "WARNING",
            },
        },
    }
)

logging.getLogger("sqlalchemy").setLevel(logging.WARNING)
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
logging.getLogger("sqlalchemy.pool").setLevel(logging.WARNING)


class Logger(logging.Logger):
    """Custom Logger defining the logging behaviour."""

    logfmt: str = "%(name)s: %(message)s"
    filelogfmt: str = "%(asctime)s %(levelname)s: %(name)s - %(message)s"
    datefmt: str = "%Y-%m-%dT%H:%M:%S"
    no_debug: list[str] = ["watchfiles", "httpcore", "pymongo", "pika"]

    def __init__(
        self, name: Optional[str] = None, level: Optional[int] = None
    ) -> None:
        """Instantiate this logger only once and for all."""
        level = level or logging.WARNING
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
        self._logger_stream_handle.setLevel(level)
        super().__init__(name, level)

        self.propagate = False
        self.handlers = [self._logger_stream_handle]

    def set_level(self, level: int) -> None:
        """Set the logger level to level."""
        for handler in self.handlers:
            log_level = level
            if isinstance(handler, RotatingFileHandler):
                log_level = min(level, logging.INFO)
            handler.setLevel(log_level)
        self.setLevel(level)

    def error(
        self,
        msg: object,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        if self.level < logging.INFO:
            kwargs.setdefault("exc_info", True)
        self._log(logging.ERROR, msg, args, **kwargs)


logger = Logger()


def add_file_handle(suffix: Optional[str], log_level: int = logging.INFO) -> None:
    """Add a file log handle to the logger."""

    base_name = f"{THIS_NAME}-{suffix}" if suffix else THIS_NAME
    log_dir = Path(appdirs.user_log_dir(THIS_NAME))
    log_dir.mkdir(exist_ok=True, parents=True)
    logger_file_handle = RotatingFileHandler(
        log_dir / f"{base_name}.log",
        mode="a",
        maxBytes=5 * 1024**2,
        backupCount=5,
        encoding="utf-8",
        delay=False,
    )
    logger_file_handle.setFormatter(logger.file_format)
    logger_file_handle.setLevel(min(log_level, logging.INFO))
    logger.addHandler(logger_file_handle)


def apply_verbosity(level: int) -> int:
    """Set the logging level of the handlers to a certain level."""
    old_level = logger.level
    level = max(logging.ERROR - 10 * level, -1)
    logger.set_level(level)
    return old_level
