"""API for adding commands to the cli."""

from functools import wraps
from typing import Annotated, Any, Callable, Dict, Tuple, TypedDict, Union

from pydantic import BaseModel, ConfigDict


class Parameter(BaseModel):
    model_config = ConfigDict(extra="allow")

    args: Union[str, Tuple]
    """Names for the arpargse.Namespace"""
    help: str
    """Help string that is going to be displayed."""


def cli_parameter(*args: str, **kwargs: Any) -> Dict[str, Any]:
    """Parameters for the argparse.Namespace.
    *args:

    **kwargs:


    """

    return Parameter(args=args, **kwargs).model_dump()


def cli_function(
    help: str = "",
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Wrap command line arguments around a method.

    Those arguments represent the arguments you would normally use to create
    a argparse.Namespace"""

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        func._cli_help = help or func.__doc__

        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return func(*args, **kwargs)

        return wrapper

    return decorator
