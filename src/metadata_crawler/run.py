"""Apply the metadata collector."""

import asyncio
import os
import time
from pathlib import Path
from types import NoneType
from typing import Any, Dict, List, Optional, Sequence, Union, cast

import tomlkit
from rich import print as pprint
from rich.prompt import Prompt

from .api.config import CrawlerSettings, DRSConfig, strip_protocol
from .api.metadata_stores import IndexName
from .data_collector import DataCollector
from .logger import apply_verbosity, logger
from .utils import (
    find_closest,
    load_plugins,
    timedelta_to_str,
)

FilesArg = Optional[Union[str, Path, Sequence[Union[str, Path]]]]


def _norm_files(catalogue_files: FilesArg) -> List[str]:
    if catalogue_files is None:
        return [""]
    if isinstance(catalogue_files, (str, Path)):
        return [str(catalogue_files)]
    return [str(p) for p in catalogue_files]


def _get_search(
    config_file: Union[str, Path, Dict[str, Any], tomlkit.TOMLDocument],
    search_dirs: Optional[List[str]] = None,
    datasets: Optional[List[str]] = None,
) -> list[CrawlerSettings]:
    _search_items = []
    search_dirs = search_dirs or []
    datasets = datasets or []
    config = DRSConfig.load(config_file).datasets
    if not datasets and not search_dirs:
        return [
            CrawlerSettings(name=k, search_path=cfg.root_path)
            for (k, cfg) in config.items()
        ]
    for item in datasets or []:
        try:
            _search_items.append(
                CrawlerSettings(name=item, search_path=config[item].root_path)
            )
        except KeyError:
            msg = find_closest(f"No such dataset: {item}", item, config.keys())
            raise ValueError(msg) from None
    for num, _dir in enumerate(map(strip_protocol, search_dirs or [])):
        for name, cfg in config.items():
            if _dir.is_relative_to(strip_protocol(cfg.root_path)):
                _search_items.append(
                    CrawlerSettings(name=name, search_path=str(search_dirs[num]))
                )

    return _search_items


async def async_call(
    index_system: str,
    method: str,
    batch_size: int = 2500,
    catalogue_files: Optional[Sequence[Union[Path, str]]] = None,
    verbosity: int = 0,
    *args: Any,
    **kwargs: Any,
) -> None:
    """Index metadata."""
    old_level = apply_verbosity(verbosity)
    try:
        backends = load_plugins("metadata_crawler.ingester")
        try:
            cls = backends[index_system]
        except KeyError:
            msg = find_closest(
                f"No such backend: {index_system}", index_system, backends.keys()
            )
            raise ValueError(msg) from None
        flat_files = _norm_files(catalogue_files)
        _event_loop = asyncio.get_event_loop()
        flat_files = flat_files or [""]
        futures = []
        storage_options = kwargs.pop("storage_options", {})
        for cf in flat_files:
            obj = cls(
                batch_size=batch_size,
                catalogue_file=cf or None,
                storage_options=storage_options,
            )
            func = getattr(obj, method)
            future = _event_loop.create_task(func(**kwargs))
            futures.append(future)
        await asyncio.gather(*futures)
    finally:
        logger.set_level(old_level)


async def async_index(
    index_system: str,
    *catalogue_files: Union[Path, str, List[str], List[Path]],
    batch_size: int = 2500,
    verbosity: int = 0,
    **kwargs: Any,
) -> None:
    """Index metadata in the indexing system.

    Parameters
    ^^^^^^^^^^

    index_system:
        The index server where the metadata is indexed.
    catalogue_file:
        Path to the file where the metadata was stored.
    batch_size:
        If the index system supports batch-sizes, the size of the batches.
    verbosity:
        Set the verbosity of the system.

    Other Parameters
    ^^^^^^^^^^^^^^^^

    **kwargs:
        Keyword arguments used to delete data from the index.


    Example
    ^^^^^^^

    .. code-block:: python

        await async_index(
           "solr"
            "/tmp/catalog.yaml",
            server="localhost:8983",
            batch_size=1000,
        )
    """
    kwargs.setdefault("catalogue_files", catalogue_files)
    await async_call(
        index_system,
        "index",
        batch_size=batch_size,
        verbosity=verbosity,
        **kwargs,
    )


async def async_delete(
    index_system: str,
    batch_size: int = 2500,
    verbosity: int = 0,
    **kwargs: Any,
) -> None:
    """Delete metadata from the indexing system.

    Parameters
    ^^^^^^^^^^^
    index_system:
        The index server where the metadata is indexed.
    batch_size:
        If the index system supports batch-sizes, the size of the batches.
    verbosity:
        Set the verbosity of the system.

    Other Parameters
    ^^^^^^^^^^^^^^^^^

    **kwargs:
        Keyword arguments used to delete data from the index.

    Examples
    ^^^^^^^^

    .. code-block:: python

        await async_delete(
            "solr"
            server="localhost:8983",
            latest_version="latest",
            facets=[("file", "*.nc"), ("project", "OBS")],
        )
    """
    await async_call(
        index_system,
        "delete",
        batch_size=batch_size,
        verbosity=verbosity,
        **kwargs,
    )


async def async_add(
    store: Optional[
        Union[str, Path, Dict[str, Any], tomlkit.TOMLDocument]
    ] = None,
    config_file: Optional[
        Union[Path, str, Dict[str, Any], tomlkit.TOMLDocument]
    ] = None,
    data_object: Optional[Union[str, List[str]]] = None,
    data_set: Optional[Union[List[str], str]] = None,
    data_store_prefix: str = "metadata",
    batch_size: int = 2500,
    comp_level: int = 4,
    storage_options: Optional[Dict[str, Any]] = None,
    catalogue_backend: str = "duckdb",
    latest_version: str = IndexName().latest,
    all_versions: str = IndexName().all,
    password: bool = False,
    threads: Optional[int] = None,
    verbosity: int = 0,
) -> None:
    """Harvest metadata from storage systems and add them to an intake catalogue.

    Parameters
    ^^^^^^^^^^

    store:
        Path to the intake catalogue.
    config_file:
        Path to the drs-config file / loaded configuration.
    data_objects:
        Instead of defining datasets that are to be crawled you can crawl
        data based on their directories. The directories must be a root dirs
        given in the drs-config file. By default all root dirs are crawled.
    data_object:
        Objects (directories or catalogue files) that are processed.
    data_set:
        Dataset(s) that should be crawled. The datasets need to be defined
        in the drs-config file. By default all datasets are crawled.
    data_store_prefix: str
        Absolute path or relative path to intake catalogue source
    batch_size:
        Batch size that is used to collect the meta data. This can affect
        performance.
    comp_level:
        Compression level used to write the meta data to csv.gz
    storage_options:
        Set additional storage options for adding metadata to the metadata store
    catalogue_backend:
        Intake catalogue backend
    latest_version:
        Name of the core holding 'latest' metadata.
    all_versions:
        Name of the core holding 'all' metadata versions.
    password:
        Display a password prompt before beginning
    threads:
        Set the number of threads for collecting.
    verbosity:
        Set the verbosity of the system.


    Examples
    ^^^^^^^^

     .. code-block:: python

        await async_add(
            store="my-data.yaml",
             config_file="~/data/drs-config.toml",
             data_set=["cmip6", "cordex"],
        )

    """
    env = cast(os._Environ[str], os.environ.copy())
    old_level = apply_verbosity(verbosity)
    try:
        config_file = config_file or os.environ.get(
            "EVALUATION_SYSTEM_CONFIG_DIR"
        )
        if not config_file:
            raise ValueError("You must give a config file/directory")
        st = time.time()
        passwd = ""
        if password:  # pragma: no cover
            passwd = Prompt.ask(
                "[b]Enter the password", password=True
            )  # pragma: no cover

        if passwd:
            os.environ["DRS_STORAGE_PASSWD"] = passwd
        data_object = (
            data_object
            if isinstance(data_object, (NoneType, list))
            else [str(data_object)]
        )
        data_set = (
            data_set
            if isinstance(data_set, (NoneType, list))
            else [str(data_set)]
        )
        async with DataCollector(
            config_file,
            store,
            IndexName(latest=latest_version, all=all_versions),
            *_get_search(config_file, data_object, data_set),
            batch_size=batch_size,
            comp_level=comp_level,
            backend=catalogue_backend,
            data_store_prefix=data_store_prefix,
            threads=threads,
            storage_options=storage_options or {},
        ) as data_col:
            await data_col.ingest_data()
            num_files = data_col.ingested_objects
            files_discovered = data_col.crawled_files
            dt = timedelta_to_str(time.time() - st)
        logger.info("Discovered: %s files", f"{files_discovered:10,.0f}")
        logger.info("Ingested: %s files", f"{num_files:10,.0f}")
        logger.info("Spend: %s", dt)
        pprint(
            (
                f"[bold]Ingested [green]{num_files:10,.0f}[/green] "
                f"within [green]{dt}[/green][/bold]"
            )
        )
    finally:
        os.environ = env
        logger.set_level(old_level)
