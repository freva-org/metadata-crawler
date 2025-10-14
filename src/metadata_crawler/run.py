"""Apply the metadata collector."""

import asyncio
import os
import time
from fnmatch import fnmatch
from pathlib import Path
from types import NoneType
from typing import Any, Collection, Dict, List, Optional, Sequence, Union, cast

import tomlkit
import yaml
from rich.prompt import Prompt

from .api.config import CrawlerSettings, DRSConfig, strip_protocol
from .api.metadata_stores import (
    CatalogueBackendType,
    CatalogueReader,
    IndexName,
)
from .data_collector import DataCollector
from .logger import apply_verbosity, get_level_from_verbosity, logger
from .utils import (
    Console,
    EmptyCrawl,
    IndexProgress,
    MetadataCrawlerException,
    find_closest,
    load_plugins,
    timedelta_to_str,
)

FilesArg = Optional[Union[str, Path, Sequence[Union[str, Path]]]]


def _norm_files(catalogue_files: FilesArg) -> List[str]:
    if catalogue_files is None:
        return [""]
    return (
        [str(catalogue_files)]
        if isinstance(catalogue_files, (str, Path))
        else [str(p) for p in catalogue_files]
    )


def _match(match: str, items: Collection[str]) -> List[str]:
    out: List[str] = []
    for item in items:
        if fnmatch(item, match):
            out.append(item)

    if not out:
        msg = find_closest(f"No such dataset: {match}", match, items)
        raise MetadataCrawlerException(msg) from None
    return out


def _get_num_of_indexed_objects(
    catalogue_files: FilesArg, storage_options: Optional[Dict[str, Any]] = None
) -> int:
    num_objects = 0
    storage_options = storage_options or {}
    for cat_file in _norm_files(catalogue_files):
        try:
            cat = CatalogueReader.load_catalogue(cat_file, **storage_options)
            num_objects += cat.get("metadata", {}).get("indexed_objects", 0)
        except (FileNotFoundError, IsADirectoryError, yaml.parser.ParserError):
            pass
    return num_objects


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
        for ds in _match(item, config.keys()):
            logger.debug("Adding dataset %s", ds)
            _search_items.append(
                CrawlerSettings(name=ds, search_path=config[ds].root_path)
            )
    for num, _dir in enumerate(map(strip_protocol, search_dirs or [])):
        for name, cfg in config.items():
            if _dir.is_relative_to(strip_protocol(cfg.root_path)):
                logger.debug("Adding dataset %s", name)
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
    log_suffix: Optional[str] = None,
    num_objects: int = 0,
    *args: Any,
    **kwargs: Any,
) -> None:
    """Add / Delete metadata from index."""
    env = cast(os._Environ[str], os.environ.copy())
    old_level = apply_verbosity(verbosity, suffix=log_suffix)

    try:
        progress = IndexProgress(total=num_objects)
        os.environ["MDC_LOG_INIT"] = "1"
        os.environ["MDC_LOG_LEVEL"] = str(get_level_from_verbosity(verbosity))
        os.environ["MDC_LOG_SUFFIX"] = (
            log_suffix or os.getenv("MDC_LOG_SUFFIX") or ""
        )
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
        progress.start()
        for cf in flat_files:
            obj = cls(
                batch_size=batch_size,
                catalogue_file=cf or None,
                storage_options=storage_options,
                progress=progress,
            )
            func = getattr(obj, method)
            future = _event_loop.create_task(func(**kwargs))
            futures.append(future)
        await asyncio.gather(*futures)

    finally:
        os.environ = env
        progress.stop()
        logger.set_level(old_level)


async def async_index(
    index_system: str,
    *catalogue_files: Union[Path, str, List[str], List[Path]],
    batch_size: int = 2500,
    verbosity: int = 0,
    log_suffix: Optional[str] = None,
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
    log_suffix:
        Add a suffix to the log file output.

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
        log_suffix=log_suffix,
        num_objects=_get_num_of_indexed_objects(
            kwargs["catalogue_files"],
            storage_options=kwargs.get("storage_options"),
        ),
        **kwargs,
    )


async def async_delete(
    index_system: str,
    batch_size: int = 2500,
    verbosity: int = 0,
    log_suffix: Optional[str] = None,
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
    log_suffix:
        Add a suffix to the log file output.

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
        log_suffix=log_suffix,
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
    batch_size: int = 25_000,
    comp_level: int = 4,
    storage_options: Optional[Dict[str, Any]] = None,
    shadow: Optional[Union[str, List[str]]] = None,
    catalogue_backend: CatalogueBackendType = "jsonlines",
    latest_version: str = IndexName().latest,
    all_versions: str = IndexName().all,
    password: bool = False,
    n_procs: Optional[int] = None,
    verbosity: int = 0,
    log_suffix: Optional[str] = None,
    fail_under: int = -1,
    **kwargs: Any,
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
        Names can contain wildcards such as ``xces-*``.
    data_store_prefix: str
        Absolute path or relative path to intake catalogue source
    batch_size:
        Batch size that is used to collect the meta data. This can affect
        performance.
    comp_level:
        Compression level used to write the meta data to csv.gz
    storage_options:
        Set additional storage options for adding metadata to the metadata store
    shadow:
        'Shadow' this storage options. This is useful to hide secrets in public
        data catalogues.
    catalogue_backend:
        Intake catalogue backend
    latest_version:
        Name of the core holding 'latest' metadata.
    all_versions:
        Name of the core holding 'all' metadata versions.
    password:
        Display a password prompt before beginning
    n_procs:
        Set the number of parallel processes for collecting.
    verbosity:
        Set the verbosity of the system.
    log_suffix:
        Add a suffix to the log file output.
    fail_under:
        Fail if less than X of the discovered files could be indexed.

    Other Parameters
    ^^^^^^^^^^^^^^^^

    **kwargs:
        Additional keyword arguments.


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
    old_level = apply_verbosity(verbosity, suffix=log_suffix)
    try:
        os.environ["MDC_LOG_INIT"] = "1"
        os.environ["MDC_LOG_LEVEL"] = str(get_level_from_verbosity(verbosity))
        os.environ["MDC_LOG_SUFFIX"] = (
            log_suffix or os.getenv("MDC_LOG_SUFFIX") or ""
        )
        config_file = config_file or os.environ.get(
            "EVALUATION_SYSTEM_CONFIG_DIR"
        )
        if not config_file:
            raise MetadataCrawlerException(
                "You must give a config file/directory"
            )
        st = time.time()
        passwd: Optional[str] = None
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
            n_procs=n_procs,
            storage_options=storage_options or {},
            shadow=shadow,
            **kwargs,
        ) as data_col:
            await data_col.ingest_data()
            num_files = data_col.ingested_objects
            files_discovered = data_col.crawled_files
            dt = timedelta_to_str(time.time() - st)
        logger.info("Discovered: %s files", f"{files_discovered:10,.0f}")
        logger.info("Ingested: %s files", f"{num_files:10,.0f}")
        logger.info("Spend: %s", dt)
        Console.print(" " * Console.width, end="\r")
        Console.print(
            (
                f"[bold]Ingested [green]{num_files:10,.0f}[/green] "
                f"within [green]{dt}[/green][/bold]"
            )
        )

        if (
            files_discovered >= fail_under and num_files < fail_under
        ) or files_discovered == 0:
            await data_col.ingest_queue.delete()
            raise EmptyCrawl("Could not fulfill discovery threshold!") from None
    finally:
        os.environ = env
        logger.set_level(old_level)
