"""Apply the metadata collector."""

import os
import time
from fnmatch import fnmatch
from functools import lru_cache
from pathlib import Path
from types import NoneType
from typing import (
    Any,
    Collection,
    Dict,
    FrozenSet,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
)

import tomlkit
import yaml
from rich.prompt import Prompt

from .api.config import CrawlerSettings, Datasets, DRSConfig, strip_protocol
from .api.metadata_stores import (
    CatalogueBackendType,
    CatalogueReader,
)
from .api.stores import IndexName
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

FilesArg = Union[str, Path, Sequence[Union[str, Path]]]


@lru_cache(maxsize=None)
def _norm_files_cached(
    uris: Tuple[str, ...],
    backend: Optional[CatalogueBackendType],
    opts: FrozenSet[Tuple[str, str]],
) -> Tuple[str, ...]:
    storage_options = dict(opts)
    flat_uris: Tuple[str, ...] = ()
    for _uri in uris:
        flat_uris += tuple(
            CatalogueReader.rglob_stores(_uri, backend=backend, **storage_options)
        )
    return flat_uris


def _norm_files(
    metadata_stores: Optional[Union[FilesArg, Sequence[FilesArg]]],
    backend: Optional[CatalogueBackendType] = None,
    **storage_options: Any,
) -> Tuple[str, ...]:
    if metadata_stores is None:
        return ("",)
    raw: Tuple[str, ...] = (
        (str(metadata_stores),)
        if isinstance(metadata_stores, (str, Path))
        else tuple(str(p) for p in metadata_stores)
    )
    return _norm_files_cached(raw, backend, frozenset(storage_options.items()))


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
    metadata_stores: Optional[Union[Sequence[FilesArg], FilesArg]],
    backend: Optional[CatalogueBackendType] = None,
    storage_options: Optional[Dict[str, Any]] = None,
) -> int:
    num_objects = 0
    storage_options = storage_options or {}
    for _uri in _norm_files(metadata_stores, backend=backend, **storage_options):
        try:
            cat = CatalogueReader.read_catalogue_metadata(
                _uri, backend=backend, **storage_options
            )
            num_objects += cat.get("indexed_objects", 0)
        except (FileNotFoundError, IsADirectoryError, yaml.parser.ParserError):
            pass
    return num_objects


def _get_search(
    config: Dict[str, Datasets],
    search_dirs: Optional[List[str]] = None,
    datasets: Optional[List[str]] = None,
) -> list[CrawlerSettings]:
    _search_items = []
    search_dirs = search_dirs or []
    datasets = datasets or []
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
    uris: Optional[Sequence[str]] = None,
    batch_size: int = 2500,
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
        os.environ["MDC_LOG_SUFFIX"] = log_suffix or os.getenv("MDC_LOG_SUFFIX") or ""
        backends = load_plugins("metadata_crawler.ingester")
        try:
            cls = backends[index_system]
        except KeyError:
            msg = find_closest(
                f"No such backend: {index_system}", index_system, backends.keys()
            )
            raise ValueError(msg) from None
        storage_options = kwargs.pop("storage_options", {})
        progress.start()
        for _uri in uris or [""]:
            async with cls(
                batch_size=batch_size,
                uri=_uri or None,
                storage_options=storage_options,
                progress=progress,
            ) as obj:
                func = getattr(obj, method)
                await func(**kwargs)

    finally:
        os.environ = env
        progress.stop()
        logger.set_level(old_level)


async def async_index(
    index_system: str,
    *metadata_stores: FilesArg,
    batch_size: int = 2500,
    verbosity: int = 0,
    log_suffix: Optional[str] = None,
    backend: Optional[CatalogueBackendType] = None,
    **kwargs: Any,
) -> None:
    """Index metadata in the indexing system.

    Parameters
    ^^^^^^^^^^

    index_system:
        The index server where the metadata is indexed.
    metadata_stores:
        Uri to the metadata store(s).
    batch_size:
        If the index system supports batch-sizes, the size of the batches.
    verbosity:
        Set the verbosity of the system.
    log_suffix:
        Add a suffix to the log file output.
    backend: str
        Backend to be used for the metadata store. If None given (default)
        the backend will be guessed from the storage uri

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
    storage_options: Dict[str, Any] = kwargs.get("storage_options", {})
    _mdata: Optional[Union[FilesArg, Sequence[FilesArg]]] = kwargs.pop(
        "metadata_stores", None
    )
    _mdata = metadata_stores or _mdata
    uris = _norm_files(_mdata, backend=backend, **storage_options)
    print(uris)
    await async_call(
        index_system,
        "index",
        batch_size=batch_size,
        verbosity=verbosity,
        log_suffix=log_suffix,
        uris=_norm_files(_mdata, backend=backend, **storage_options),
        num_objects=_get_num_of_indexed_objects(
            _mdata,
            backend=backend,
            storage_options=storage_options,
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
    *config_files: Union[Path, str, Dict[str, Any], tomlkit.TOMLDocument],
    store: Optional[Union[str, Path, Dict[str, Any], tomlkit.TOMLDocument]] = None,
    data_object: Optional[Union[str, List[str]]] = None,
    data_set: Optional[Union[List[str], str]] = None,
    data_store_prefix: Optional[str] = None,
    collection: Optional[str] = None,
    table: Optional[str] = None,
    batch_size: int = 25_000,
    catalogue_backend: Optional[CatalogueBackendType] = None,
    backend: Optional[CatalogueBackendType] = None,
    comp_level: int = 4,
    storage_options: Optional[Dict[str, Any]] = None,
    shadow: Optional[Union[str, List[str]]] = None,
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

    .. versionchanged:: 2511.0.0

       The catalogue argument has been rearanged and is now a keyword
       argument: ``async_add("data.yaml", "drs-config.toml")`` becomes
       ``async_add("drs-config.toml", store="data.yaml")``. If the ``store`` keyword
       is omitted the output catalogue will be interpreted as config file.

    Parameters
    ^^^^^^^^^^

    config_files:
        Path to the drs-config file / loaded configuration.
    store:
        Path to the intake catalogue.
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
        Name or path of the metadata store.  For the *jsonlines*
        backend this is a filesystem path prefix for the ``.json.gz``
        files (resolved relative to *yaml_path* unless absolute).
        For database backends it serves as the default collection or
        table name.  Defaults to ``"metadata"``.
    collection: str
        Alias for *data_store_prefix* — preferred when using the
        *mongodb* backend.  Maps directly to the MongoDB collection
        name.
    table: str
        Alias for *data_store_prefix* — preferred when using the
        *sql* backend.  Maps directly to the SQL table name.
    backend: str
        Backend to be used for the metadata store. If None given (default)
        the backend will be guessed from the storage uri
    catalogue_backend: str
        Alias for ``backend``
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
             "~/data/drs-config.toml",
             store="my-data.yaml",
             data_set=["cmip6", "cordex"],
        )

    """
    env = cast(os._Environ[str], os.environ.copy())
    old_level = apply_verbosity(verbosity, suffix=log_suffix)
    eval_env_dir = os.getenv("EVALUATION_SYSTEM_CONFIG_DIR")
    cfg_files_fallback = (eval_env_dir,) if eval_env_dir else ()

    backend = backend or catalogue_backend
    try:
        os.environ["MDC_LOG_INIT"] = "1"
        os.environ["MDC_LOG_LEVEL"] = str(get_level_from_verbosity(verbosity))
        os.environ["MDC_LOG_SUFFIX"] = log_suffix or os.getenv("MDC_LOG_SUFFIX") or ""

        config_files = config_files or cfg_files_fallback
        if not all(config_files):
            raise MetadataCrawlerException("You must give a config file/directory")
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
            data_set if isinstance(data_set, (NoneType, list)) else [str(data_set)]
        )
        cfg = DRSConfig.load(*config_files)
        async with DataCollector(
            cfg,
            store,
            IndexName(latest=latest_version, all=all_versions),
            *_get_search(cfg.datasets, data_object, data_set),
            batch_size=batch_size,
            comp_level=comp_level,
            data_store_prefix=data_store_prefix or "",
            table=table or "",
            collection=collection or "",
            n_procs=n_procs,
            storage_options=storage_options or {},
            shadow=shadow,
            backend=backend,
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
            if data_col.ingest_queue.silent is False:
                await data_col.ingest_queue.delete()
                raise EmptyCrawl("Could not fulfill discovery threshold!") from None
    finally:
        os.environ = env
        logger.set_level(old_level)
