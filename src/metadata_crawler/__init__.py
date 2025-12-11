"""Metadata Crawler API high level functions."""

import asyncio
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import tomlkit

try:
    import uvloop

    use_uvloop = True
except ImportError:

    use_uvloop = False  # pragma: no cover


from ._version import __version__
from .api.config import ConfigMerger, DRSConfig
from .api.metadata_stores import CatalogueBackendType, IndexName
from .data_collector import DataCollector
from .logger import logger
from .run import async_add, async_delete, async_index

if use_uvloop:
    async_model = uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
else:
    async_model = asyncio  # pragma: no cover

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


def get_config(*config: Union[Path, str]) -> ConfigMerger:
    """Get a drs config file merged with the default config.

    The method is helpful to inspect all possible configurations and their
    default values.

    Parameters
    ^^^^^^^^^^

    config:
        Path to a user defined config file that is going to be merged with
        the default config.
    """
    _ = DRSConfig.load(*config)
    return ConfigMerger(*config)


def index(
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
    catalogue_files:
        Path to the file(s) where the metadata was stored.
    batch_size:
        If the index system supports batch-sizes, the size of the batches.
    verbosity:
        Set the verbosity level.
    log_suffix:
        Add a suffix to the log file output.

    Other Parameters
    ^^^^^^^^^^^^^^^^

    **kwargs:
        Keyword arguments used to delete data from the index.

    Examples
    ^^^^^^^^

    .. code-block:: python

        index(
            "solr",
            "/tmp/catalog-1.yml",
            "/tmp/catalog-2.yml",
            batch_size=50,
            server="localhost:8983",
        )
    """
    async_model.run(
        async_index(
            index_system,
            *catalogue_files,
            batch_size=batch_size,
            verbosity=verbosity,
            log_suffix=log_suffix,
            **kwargs,
        )
    )


def delete(
    index_system: str,
    batch_size: int = 2500,
    verbosity: int = 0,
    log_suffix: Optional[str] = None,
    **kwargs: Any,
) -> None:
    """Delete metadata from the indexing system.

    Parameters
    ^^^^^^^^^^

    index_system:
        The index server where the metadata is indexed.
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


    Examples
    ^^^^^^^^

    .. code-block:: python

        delete(
            "solr",
            server="localhost:8983",
            facets=[("project", "CMIP6"), ("institute", "MPI-M")],
        )
    """
    async_model.run(
        async_delete(
            index_system, batch_size=batch_size, log_suffix=log_suffix, **kwargs
        )
    )


def add(
    *config_files: Union[Path, str, Dict[str, Any], tomlkit.TOMLDocument],
    store: Optional[Union[str, Path]] = None,
    data_object: Optional[Union[str, List[str]]] = None,
    data_set: Optional[Union[str, List[str]]] = None,
    data_store_prefix: str = "metadata",
    catalogue_backend: CatalogueBackendType = "jsonlines",
    batch_size: int = 25_000,
    comp_level: int = 4,
    storage_options: Optional[Dict[str, Any]] = None,
    shadow: Optional[Union[str, List[str]]] = None,
    latest_version: str = IndexName().latest,
    all_versions: str = IndexName().all,
    n_procs: Optional[int] = None,
    verbosity: int = 0,
    log_suffix: Optional[str] = None,
    password: bool = False,
    fail_under: int = -1,
    **kwargs: Any,
) -> None:
    """Harvest metadata from storage systems and add them to an intake catalogue.

    .. versionchanged:: 2511.0.0

       The catalogue argument has been rearanged and is now a keyword
       argument: ``add("data.yaml", "drs-config.toml")`` becomes
       ``add("drs-config.toml", store="data.yaml")``. If the ``store`` keyword
       is omitted the output catalogue will be interpreted as config file.

    Parameters
    ^^^^^^^^^^

    config_files:
        Path to the drs-config file / loaded configuration.
    store:
        Path to the intake catalogue where the collected metadata will be
        stored.
    data_ojbect:
        Instead of defining datasets that are to be crawled you can crawl
        data based on their directories. The directories must be a root dirs
        given in the drs-config file. By default all root dirs are crawled.
    data_set:
        Datasets that should be crawled. The datasets need to be defined
        in the drs-config file. By default all datasets are crawled.
        Names can contain wildcards such as ``xces-*``.
    data_store_prefix:
        Absolute path or relative path to intake catalogue source
    data_dir:
        Instead of defining datasets are are to be crawled you can crawl
        data based on their directories. The directories must be a root dirs
        given in the drs-config file. By default all root dirs are crawled.
    bach_size:
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
        Display a password prompt and set password before beginning.
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

        add(
            "~/data/drs-config.toml",
            store="my-data.yaml",
            data_set=["cmip6", "cordex"],
        )
    """
    async_model.run(
        async_add(
            *config_files,
            store=store,
            data_object=data_object,
            data_set=data_set,
            batch_size=batch_size,
            comp_level=comp_level,
            password=password,
            catalogue_backend=catalogue_backend,
            data_store_prefix=data_store_prefix,
            shadow=shadow,
            latest_version=latest_version,
            all_versions=all_versions,
            n_procs=n_procs,
            storage_options=storage_options,
            verbosity=verbosity,
            log_suffix=log_suffix,
            fail_under=fail_under,
            **kwargs,
        )
    )
