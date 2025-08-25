"""API for adding commands to the cli."""

from functools import wraps
from typing import Any, Callable, Dict, Tuple, Union

from pydantic import BaseModel, ConfigDict


class Parameter(BaseModel):
    """CLI Parameter model."""

    model_config = ConfigDict(extra="allow")

    args: Union[str, Tuple[str, ...]]
    """Names for the arpargse.Namespace."""
    help: str
    """Help string that is going to be displayed."""


def cli_parameter(*args: str, **kwargs: Any) -> Dict[str, Any]:
    """Construct a ``argparse.Namespace``.

    Parameters
    ^^^^^^^^^^
    *args:
        Any arguments passed to ``argparse.ArgumentParser().add_argument``
    **kwargs:
        Any keyword arguments passed to ``argparse.ArgumentParser().add_arguent``

    """
    return Parameter(args=args, **kwargs).model_dump()


def cli_function(
    help: str = "",
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Wrap command line arguments around a method.

    Those arguments represent the arguments you would normally use to create
    a `argparse subcommand <https://docs.python.org/3/library/argparse.html>`_.

    Parameters
    ^^^^^^^^^^
    help:
        Help string for this sub command.
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        setattr(func, "_cli_help", help or func.__doc__)

        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return func(*args, **kwargs)

        return wrapper

    return decorator
