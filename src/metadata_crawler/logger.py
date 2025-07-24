import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Iterator, Optional

import appdirs
from rich.console import Console
from rich.logging import RichHandler

THIS_NAME = "data-crawler"


class Logger(logging.Logger):
    """Custom Logger defining the logging behaviour."""

    logfmt: str = "%(name)s: %(message)s"
    filelogfmt: str = "%(asctime)s %(levelname)s: %(name)s - %(message)s"
    datefmt: str = "%Y-%m-%dT%H:%M:%S"
    no_debug: list[str] = ["watchfiles", "httpcore", "pymongo", "pika"]

    def _check_for_debug(self, name: str) -> bool:
        for service in self.no_debug:
            if name.startswith(service):
                return False
        return True

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
            handler.setLevel(level)
        self.setLevel(level)

    def error(
        self,
        msg: object,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        if self.level < logging.WARNING:
            kwargs.setdefault("exc_info", True)
        self._log(logging.ERROR, msg, args, **kwargs)

    def reset_loggers(self, level: Optional[int] = None) -> None:
        """Unify all loggers that we have currently aboard."""
        for name in logging.root.manager.loggerDict.keys():
            if not name.startswith(THIS_NAME):
                logging.getLogger(name).handlers = self.handlers
            logging.getLogger(name).propagate = False
            logging.getLogger(name).level = level or self.getEffectiveLevel()


logger = Logger()


def add_file_handle(suffix: Optional[str], log_level: int = logging.INFO) -> None:
    """Add a file log handle to the logger."""
    base_name = THIS_NAME
    if suffix:
        base_name += f"-{suffix}"
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


def set_log_level(level: int) -> None:
    """Set the logging level of the handlers to a certain level."""
    for handler in logger.handlers:
        log_level = level
        if isinstance(handler, RotatingFileHandler):
            log_level = min(level, logging.INFO)
        handler.setLevel(log_level)
    logger.setLevel(level)
