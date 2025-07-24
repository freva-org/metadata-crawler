from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import uvloop

from ._version import __version__
from .data_collector import DataCollector
from .logger import logger
from .run import async_add, async_delete, async_index

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
__all__ = [
    "logger",
    "__version__",
    "DataCollector",
    "index",
    "add",
    "delete",
    "async_index",
    "async_delete",
    "async_add",
]


def index(
    index_system: str,
    catalogue_file: Path | str,
    batch_size: int = 2500,
    **kwargs: Any,
) -> None:
    """Index metadata in the indexing system.

    Parameters
    ----------

    index_system:
        The index server where the metadata is indexed.
    catalogue_file:
        Path to the file where the metadata was stored.
    batch_size:
        If the index system supports batch-sizes, the size of the batches.
    **kwargs:
        Keyword arguments used to delete data from the index.

    """

    uvloop.call(
        async_index(
            index_system,
            catalogue_file=catalogue_file,
            batch_size=batch_size,
            **kwargs,
        )
    )


def delete(index_system: str, batch_size: int = 2500, **kwargs: Any) -> None:
    """Delete metadata from the indexing system.

    Parameters
    ----------

    index_system:
        The index server where the metadata is indexed.
    batch_size:
        If the index system supports batch-sizes, the size of the batches.
    **kwargs:
        Keyword arguments used to delete data from the index.

    """
    uvloop.call(async_delete(index_system, batch_size=batch_size, **kwargs))


def add(
    store: str | None = None,
    config_file: Path | None | str = None,
    data_dir: list[str] | None = None,
    data_set: list[str] | None = None,
    batch_size: int = 2500,
    comp_level: int = 4,
    password: bool = False,
) -> None:
    """Harvest metdata from sotrage systems and add them to an intake catalogue

    Parameters
    ----------

    store: str
        Path to the intake catalogue.
    config_file:
        Path to the drs-config file.
    data_set:
        Datasets that should be crawled. The datasets need to be defined
        in the drs-config file. By default all datasets are crawled.
    data_dir:
        Instead of defining datasets are are to be crawled you can crawl
        data based on their directories. The directories must be a root dirs
        given in the drs-config file. By default all root dirs are crawled.
    bach_size:
        Batch size that is used to collect the meta data. This can affect
        preformance.
    comp_level:
        Compression level used to write the meta data to csv.gz
    password:
        Display a password prompt and set password before beginning


    Example
    -------

        ::
            add(
                "my-data.yaml",
                "~/data/drs-config.toml",
                data_set=["cmip6", "cordex"],
            )
    """
    uvloop(
        async_add(
            store=store,
            config_file=config_file,
            data_dir=data_dir,
            data_set=data_set,
            batch_size=batch_size,
            comp_level=comp_level,
            password=password,
        )
    )
