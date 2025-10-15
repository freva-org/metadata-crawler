"""Random utility functions."""

import difflib
import logging
import multiprocessing as mp
import multiprocessing.context as mctx
import os
import sys
import time
from datetime import datetime, timedelta
from importlib.metadata import entry_points
from typing import (
    IO,
    Any,
    AsyncIterator,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Protocol,
    Set,
    Tuple,
    TypeAlias,
    TypeVar,
    Union,
)

import ciso8601
import orjson
import rich.console
import rich.spinner
from dateutil.parser import isoparse
from rich.live import Live
from rich.progress import Progress, TaskID

from .logger import logger

T = TypeVar("T")
U = TypeVar("U")


class SimpleQueueLike(Protocol[T]):
    """A simple queue like Type class."""

    def put(self, item: T) -> None:  # noqa
        ...

    def get(self) -> T:  # noqa
        ...


class QueueLike(Protocol[T]):
    """A queue like Type class."""

    def put(self, item: T) -> None:  # noqa
        ...

    def get(
        self, block: bool = True, timeout: Optional[float] = ...
    ) -> T:  # noqa
        ...

    def qsize(self) -> int:  # noqa
        ...


class EventLike(Protocol):
    """An event like Type class."""

    def set(self) -> None:  # noqa
        ...

    def clear(self) -> None:  # noqa
        ...

    def is_set(self) -> bool:  # noqa
        ...

    def wait(self) -> None:  # noqa
        ...


class LockLike(Protocol):
    """A lock like Type class."""

    def acquire(
        self, blocking: bool = ..., timeout: Optional[float] = ...
    ) -> bool:  # noqa
        ...

    def release(self) -> None:  # noqa
        ...

    def __enter__(self) -> "LockLike": ...
    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None: ...


class ValueLike(Protocol[U]):
    """A value like Type class."""

    value: U

    def get_lock(self) -> "Any":  # noqa
        ...


class FilesystemLike(Protocol):
    """File-like opener protocol (e.g., fsspec)."""

    def open(
        self,
        path: str,
        mode: str = "rt",
        compression: Optional[str] = None,
        encoding: Optional[str] = None,
        **kwargs: Any,
    ) -> IO[str]:  # noqa
        ...


Counter: TypeAlias = ValueLike[int]
PrintLock = mp.Lock()
Console = rich.console.Console(force_terminal=sys.stdout.isatty(), stderr=True)


class MetadataCrawlerException(Exception):
    """Custom Exception for the crawling."""


class EmptyCrawl(MetadataCrawlerException):
    """Cusotom Exceptoin for a crawl with no results."""


async def create_async_iterator(itt: Iterable[Any]) -> AsyncIterator[Any]:
    """Create an async iterator from as sync iterable."""
    for item in itt:
        yield item


def _parse_iso_datetime(s: str) -> datetime:
    return ciso8601.parse_datetime(s) or datetime.fromisoformat(s)


def parse_batch(
    lines: List[str],
    timestamp_keys: Set[str],
) -> List[Dict[str, Any]]:
    """Parse a batch of NDJSON lines and convert timestamp fields.

    Parameters
    ^^^^^^^^^^
    lines : list of str
        Raw NDJSON lines.
    timestamp_keys : set of str
        Keys that should be parsed as datetimes.

    Returns
    ^^^^^^^
    list of dict
        Parsed objects with timestamp fields converted to ``datetime``.
    """
    out: List[Dict[str, Any]] = []
    append = out.append
    loads = orjson.loads
    parse_dt = _parse_iso_datetime

    for line in lines:
        obj: Dict[str, Any] = loads(line)
        for k in timestamp_keys:
            v = obj.get(k, None)
            if v is None:
                continue
            if isinstance(v, str):
                obj[k] = parse_dt(v)
            elif isinstance(v, list):
                obj[k] = [parse_dt(x) if isinstance(x, str) else x for x in v]
        append(obj)
    return out


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
    _date = isoparse(alternative)
    _time = f"{_date.strftime('%H')}:{_date.strftime('%M')}"
    _day = _date.strftime("%d")
    _mon = _date.strftime("%m")
    has_t_separator = "T" in time_str
    position_t = time_str.find("T") if has_t_separator else -1
    # Strip anything that's not a number from the string
    if not time_str:
        return _date
    # Not valid if time repr empty or starts with a letter, such as 'fx'
    digits = "".join(filter(str.isdigit, time_str))
    l_times = len(digits)
    if not l_times:
        return _date
    try:
        if l_times <= 4:
            # Suppose this is a year only
            return isoparse(f"{digits.zfill(4)}-{_mon}-{_day}T{_time}")
        if l_times <= 6:
            # Suppose this is %Y%m or %Y%e
            return isoparse(f"{digits[:4]}-{digits[4:].zfill(2)}-{_day}T{_time}")
        # Year and day of year
        if l_times == 7:
            # Suppose this is %Y%j
            year = int(digits[:4])
            day_of_year = int(digits[4:])
            date = datetime(year, 1, 1, _date.hour, _date.minute) + timedelta(
                days=day_of_year - 1
            )
            return date
        if l_times <= 8:
            # Suppose this is %Y%m%d
            return isoparse(
                f"{digits[:4]}-{digits[4:6]}-{digits[6:].zfill(2)}T{_time}"
            )

        date_str = f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"
        time = digits[8:]
        if len(time) <= 2:
            time = time.zfill(2)
        else:
            # Alaways drop seconds
            time = time[:2] + ":" + time[2 : min(4, len(time))].zfill(2)
        return isoparse(f"{date_str}T{time}")

    except ValueError:
        if has_t_separator and position_t > 0:
            date_part = time_str[:position_t]
            time_part = time_str[position_t + 1 :]

            date_digits = "".join(filter(str.isdigit, date_part))
            if len(date_digits) >= 8:
                return isoparse(
                    f"{date_digits[:4]}-{date_digits[4:6]}"
                    f"-{date_digits[6:8]}T{time_part[:2].zfill(2)}"
                )

        return _date


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
    if logger.level >= logging.INFO:
        msg += " - increase verbosity for more information"
        exc_info = None
    else:
        exc_info = exception
    logger.critical(msg, exc_info=exc_info)
    raise SystemExit(1)


def daemon(
    func: Callable[..., Any],
) -> Callable[..., mctx.ForkProcess]:
    """Threading decorator.

    use @daemon above the function you want to run in the background
    """

    def background_func(*args: Any, **kwargs: Any) -> mctx.ForkProcess:
        ctx = mp.get_context("fork")
        proc = ctx.Process(target=func, args=args, kwargs=kwargs, daemon=True)
        proc.start()
        return proc

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


class IndexProgress:
    """A helper that displays the progress of index Tasks."""

    def __init__(
        self,
        total: int = 0,
        interactive: Optional[bool] = None,
        text: str = "Indexing: ",
    ) -> None:
        if interactive is None:
            self._interactive = bool(
                int(os.getenv("MDC_INTERACTIVE", str(int(Console.is_terminal))))
            )
        else:
            self._interactive = interactive
        self._log_interval = int(os.getenv("MDC_LOG_INTERVAL", "30"))
        self.text = text
        self._done = 0
        self._task: TaskID = TaskID(0)
        self._total = total
        self._start = self._last_log = time.time()
        self._progress = Progress()
        self._last_printed_percent: float = -1.0

    def start(self) -> None:
        """Start the progress bar."""
        self._start = self._last_log = time.time()

        if self._interactive:
            self._task = self._progress.add_task(
                f"[green] {self.text}", total=self._total or None
            )
            self._progress.start()

    def stop(self) -> None:
        """Stop the progress bar."""
        if self._interactive:
            self._progress.stop()
        else:
            self._text_update()

    def _text_update(self, bar_width: int = 40) -> None:
        elapsed = timedelta(seconds=int(time.time() - self._start))
        log_interval = timedelta(seconds=int(time.time() - self._last_log))
        if self._total > 0:
            filled = int((self._last_printed_percent / 100) * bar_width)
            bar = "#" * filled + "-" * (bar_width - filled)
            text = f"{self.text} [{bar}] {self._last_printed_percent:>6,.02f}%"
        else:
            text = f"{self.text} [{self._done:>12,}]"
        if log_interval.total_seconds() >= self._log_interval:
            print(f"{text} ({elapsed})", flush=True)
            self._last_log = time.time()

    def update(self, inc: int) -> None:
        """Update the status progress bar by an increment."""
        self._done += inc

        if self._interactive is True:
            desc = f"{self.text} [{self._done:>10d}]" if self._done == 0 else None
            self._progress.update(self._task, advance=inc, description=desc)
            return

        frac = self._done / max(self._total, 1)
        pct = frac * 100
        if pct > self._last_printed_percent or self._total == 0:
            self._last_printed_percent = pct
            self._text_update()


@daemon
def print_performance(
    print_status: EventLike,
    num_files: Counter,
    ingest_queue: QueueLike[Any],
    num_objects: Counter,
) -> None:
    """Display the progress of the crawler."""
    spinner = rich.spinner.Spinner(
        os.getenv("SPINNER", "earth"), text="[b]Preparing crawler ...[/]"
    )
    interactive = bool(
        int(os.getenv("MDC_INTERACTIVE", str(int(Console.is_terminal))))
    )
    log_interval = int(os.getenv("MDC_LOG_INTERVAL", "30"))
    sample_interval = 1.0 if interactive else 10.0

    def _snapshot() -> Tuple[float, int, int, int]:
        start = time.monotonic()
        n0 = num_files.value
        time.sleep(sample_interval)
        dn = num_files.value - n0
        dt = max(1e-6, time.monotonic() - start)
        perf_file = dn / dt
        queue_size = ingest_queue.qsize()
        return perf_file, n0, queue_size, num_objects.value

    def _build_msg(
        perf_file: float,
        discovered: int,
        queue_size: int,
        indexed: int,
        *,
        markup: bool,
    ) -> str:
        # Color thresholds only when markup=True (interactive)
        if markup:
            f_col = (
                "green"
                if perf_file > 500
                else "red" if perf_file < 100 else "blue"
            )
            q_col = (
                "red"
                if queue_size > 100_000
                else "green" if queue_size < 10_000 else "blue"
            )
            return (
                f"[bold]Discovering: [{f_col}]{perf_file:>6,.1f}[/{f_col}] files/s "
                f"#files: [blue]{discovered:>10,.0f}[/blue] "
                f"in queue: [{q_col}]{queue_size:>6,.0f}[/{q_col}] "
                f"#indexed: [blue]{indexed:>10,.0f}[/blue][/bold]"
            )
        else:
            return (
                f"Discovering: {perf_file:,.1f} files/s | "
                f"files={discovered:,} | queue={queue_size:,} | indexed={indexed:,}"
            )

    if interactive:
        with Live(
            spinner, console=Console, refresh_per_second=2.5, transient=True
        ):
            while print_status.is_set():
                perf, disc, qsz, idx = _snapshot()
                spinner.update(text=_build_msg(perf, disc, qsz, idx, markup=True))
        # Clear the last line when done
        Console.print(" " * Console.width, end="\r")
        Console.print(" ")
    else:
        # Non-TTY (e.g. systemd): emit a plain summary every log_interval secs
        next_log = time.monotonic()
        while print_status.is_set():
            perf, disc, qsz, idx = _snapshot()
            now = time.monotonic()
            if now >= next_log:
                # Print one clean line; journald/Cockpit will show one entry
                print(_build_msg(perf, disc, qsz, idx, markup=False), flush=True)
                next_log = now + log_interval
