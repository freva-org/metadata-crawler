"""Random utility functions."""

import asyncio
import threading
from functools import wraps
from importlib.metadata import entry_points
from typing import Any, Callable, Dict, Optional, TypeVar, Union

import rich.console
import toml

from .logger import logger

T = TypeVar("T")


PrintLock = threading.Lock()
Console = rich.console.Console(force_terminal=True, stderr=True)


def load_plugins(group: str) -> list:
    """Load harverster plugins."""
    eps = entry_points().select(group=group)
    plugins = {}
    for ep in eps:
        plugins[ep.name] = ep.load()
    return plugins


async def call_sync_in_async(
    sync_func: Callable[..., T], *args: Any, **kwargs: Any
) -> T:
    """Wrapper for calling a synchronous function in an async context."""

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, sync_func, *args, **kwargs)


def exception_handler(exception: BaseException) -> None:
    """Handle raising exceptions appropriately."""

    trace_back = exception.__traceback__
    # Set only the last traceback of the exception
    while trace_back:
        exception.__traceback__ = trace_back
        trace_back = trace_back.tb_next
    msg = str(exception)
    if logger.level > 30:
        msg += " - increase verbosity for more information"
        exc_info = None
    else:
        exc_info = exception
    logger.error(msg, exc_info=exc_info)
    raise SystemExit


def daemon(
    func: Callable[..., Any],
) -> Callable[..., Optional[threading.Thread]]:
    """Threading decorator

    use @daemon above the function you want to run in the background
    """

    def background_func(*args: Any, **kwargs: Any) -> Optional[threading.Thread]:
        thread = threading.Thread(
            target=func, args=args, kwargs=kwargs, daemon=True
        )
        thread.start()
        return thread

    return background_func


def timedelta(seconds: Union[int, float]) -> str:
    """Convert seconds to a more human readable format."""
    hours = seconds // 60**2
    minutes = (seconds // 60) % 60
    sec = round(seconds - (hours * 60 + minutes) * 60, 2)
    out = []
    for num, letter in {sec: "Sec.", minutes: "Min.", hours: "Hour"}.items():
        if num > 0:
            out.append(f"{num} {letter}")
    return " ".join(out[::-1])


def deprecated_key(
    deprecated_keys: Dict[str, str],
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            config_file = args[0]
            for dataset, config in toml.loads(config_file.read_text()).items():
                for deprecated_key, new_key in deprecated_keys.items():
                    if config.get(deprecated_key) is not None:
                        logger.critical(
                            f"The '{deprecated_key}' key for '{config[deprecated_key]}' in 'drs_config.toml' is deprecated. "
                            f"Please update your config file to use '{new_key}' instead."
                        )
            return func(*args, **kwargs)

        return wrapper

    return decorator


def validate_bbox(func: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        config_file = args[0]
        for dataset, config in toml.loads(config_file.read_text()).items():
            drs_format = config.get("drs_format", "").lower()
            bbox = config.get("bbox")

            if drs_format != "cordex" and not bbox:
                logger.debug(
                    f"Dataset '{dataset}' uses {drs_format} format without bbox specification. "
                    "Will use global coordinates."
                )
            if drs_format == "cordex" and not bbox:
                if not bbox:
                    logger.debug(
                        f"Dataset '{dataset}' uses CORDEX format without bbox specification. "
                        "Will use CORDEX domain if matched, otherwise global coordinates."
                    )
        return func(*args, **kwargs)

    return wrapper
