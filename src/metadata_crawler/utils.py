"""Random utility functions."""

import difflib
import threading
from datetime import datetime, timedelta
from importlib.metadata import entry_points
from typing import (
    Any,
    AsyncIterator,
    Callable,
    Dict,
    Iterable,
    Optional,
    TypeVar,
    Union,
)

import rich.console

from .logger import logger

T = TypeVar("T")


PrintLock = threading.Lock()
Console = rich.console.Console(force_terminal=True, stderr=True)


async def create_async_iterator(itt: Iterable[Any]) -> AsyncIterator[Any]:
    """Create an async iterator from as sync iterable."""
    for item in itt:
        yield item


def convert_str_to_timestamp(
    time_str: str, alternative: str = "0001-01-01"
) -> datetime:
    """Convert a string representation of a time step to an iso timestamp.

    Parameters
    ----------
    time_str: str
        Representation of the time step in formats:
        - %Y%m%d%H%M%S%f (year, month, day, hour, minute, second, millisecond)
        - %Y%m%d%H%M (year, month, day, hour, minute)
        - %Y%m (year, month)
        - %Y%m%dT%H%M (year, month, day, hour, minute with T separator)
        - %Y%j (year and day of year, e.g. 2022203 for 22nd July 2022)
        - %Y (year only)
    alternative: str, default: 0
        If conversion fails, the alternative/default value the time step
        gets assign to

    Returns
    -------
    str: ISO time string representation of the input time step, such as
        %Y %Y-%m-%d or %Y-%m-%dT%H%M%S
    """
    has_t_separator = "T" in time_str
    position_t = time_str.find("T") if has_t_separator else -1
    # Strip anything that's not a number from the string
    if not time_str:
        return datetime.fromisoformat(alternative)
    # Not valid if time repr empty or starts with a letter, such as 'fx'
    digits = "".join(filter(str.isdigit, time_str))
    l_times = len(digits)

    if not l_times:
        return datetime.fromisoformat(alternative)
    try:
        if l_times <= 4:
            # Suppose this is a year only
            return datetime.fromisoformat(digits.zfill(4))
        if l_times <= 6:
            # Suppose this is %Y%m or %Y%e
            return datetime.fromisoformat(f"{digits[:4]}-{digits[4:].zfill(2)}")
        # Year and day of year
        if l_times == 7:
            # Suppose this is %Y%j
            year = int(digits[:4])
            day_of_year = int(digits[4:])
            date = datetime(year, 1, 1) + timedelta(days=day_of_year - 1)
            return date
        if l_times <= 8:
            # Suppose this is %Y%m%d
            return datetime.fromisoformat(
                f"{digits[:4]}-{digits[4:6]}-{digits[6:].zfill(2)}"
            )

        date_str = f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"
        time = digits[8:]
        if len(time) <= 2:
            time = time.zfill(2)
        else:
            # Alaways drop seconds
            time = time[:2] + ":" + time[2 : min(4, len(time))].zfill(2)
        return datetime.fromisoformat(f"{date_str}T{time}")

    except ValueError:
        if has_t_separator and position_t > 0:
            date_part = time_str[:position_t]
            time_part = time_str[position_t + 1 :]

            date_digits = "".join(filter(str.isdigit, date_part))
            if len(date_digits) >= 8:
                return datetime.fromisoformat(
                    f"{date_digits[:4]}-{date_digits[4:6]}"
                    f"-{date_digits[6:8]}T{time_part[:2].zfill(2)}"
                )

        return datetime.fromisoformat(alternative)


def find_closest(msg: str, target: str, options: Iterable[str]) -> str:
    """Find the closest match for a target within a collection of items.

    Parameters
    ----------
    target:   The string to match.
    options:  A list of candidate strings.


    Returns
    -------
        str: Message
    """
    matches = difflib.get_close_matches(target, options, n=1, cutoff=0.6)
    suffix = f", did you mean {matches[0]}?" if matches else ""
    return msg + suffix


def load_plugins(group: str) -> Dict[str, Any]:
    """Load harverster plugins."""
    eps = entry_points().select(group=group)
    plugins = {}
    for ep in eps:
        plugins[ep.name] = ep.load()
    return plugins


def exception_handler(exception: BaseException) -> None:
    """Handle raising exceptions appropriately."""
    msg = str(exception)
    if logger.level > 30:
        msg += " - increase verbosity for more information"
        exc_info = None
    else:
        exc_info = exception
    logger.error(msg, exc_info=exc_info)
    raise SystemExit(1)


def daemon(
    func: Callable[..., Any],
) -> Callable[..., Optional[threading.Thread]]:
    """Threading decorator.

    use @daemon above the function you want to run in the background
    """

    def background_func(*args: Any, **kwargs: Any) -> Optional[threading.Thread]:
        thread = threading.Thread(
            target=func, args=args, kwargs=kwargs, daemon=True
        )
        thread.start()
        return thread

    return background_func


def timedelta_to_str(seconds: Union[int, float]) -> str:
    """Convert seconds to a more human readable format."""
    hours = seconds // 60**2
    minutes = (seconds // 60) % 60
    sec = round(seconds - (hours * 60 + minutes) * 60, 2)
    out = []
    for num, letter in {sec: "Sec.", minutes: "Min.", hours: "Hour"}.items():
        if num > 0:
            out.append(f"{num} {letter}")
    return " ".join(out[::-1])
