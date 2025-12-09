from pathlib import Path
from typing import Any

import tomlkit

from ._version import __version__ as __version__
from .api.config import ConfigMerger
from .api.metadata_stores import CatalogueBackendType
from .data_collector import DataCollector as DataCollector
from .logger import logger as logger
from .run import async_add as async_add
from .run import async_delete as async_delete
from .run import async_index as async_index

__all__ = [
    "logger",
    "__version__",
    "DataCollector",
    "index",
    "add",
    "delete",
    "get_config",
    "async_index",
    "async_delete",
    "async_add",
    "get_config",
]

def get_config(config: Path | str | None = None) -> ConfigMerger: ...
def index(
    index_system: str,
    *catalogue_files: Path | str | list[str] | list[Path],
    batch_size: int = 2500,
    verbosity: int = 0,
    log_suffix: str | None = None,
    **kwargs: Any,
) -> None: ...
def delete(
    index_system: str,
    batch_size: int = 2500,
    verbosity: int = 0,
    log_suffix: str | None = None,
    **kwargs: Any,
) -> None: ...
def add(
    store: str | Path | None = None,
    config_file: Path | str | dict[str, Any] | tomlkit.TOMLDocument | None = None,
    data_object: str | list[str] | None = None,
    data_set: str | list[str] | None = None,
    data_store_prefix: str = "metadata",
    catalogue_backend: CatalogueBackendType = "jsonlines",
    batch_size: int = 25000,
    comp_level: int = 4,
    storage_options: dict[str, Any] | None = None,
    shadow: str | list[str] | None = None,
    latest_version: str = ...,
    all_versions: str = ...,
    n_procs: int | None = None,
    verbosity: int = 0,
    log_suffix: str | None = None,
    password: bool = False,
    fail_under: int = -1,
    **kwargs: Any,
) -> None: ...
