"""Apply the metdata collector."""

import asyncio
import os
import time
from pathlib import Path
from typing import Any, Optional, Union

import uvloop
from rich import print as pprint
from rich.prompt import Prompt

from .api.config import CrawlerSettings, DRSConfig, strip_protocol
from .api.metadata_stores import IndexName
from .data_collector import DataCollector
from .logger import logger
from .utils import (
    deprecated_key,
    find_closest,
    load_plugins,
    timedelta,
    validate_bbox,
)


@staticmethod
@deprecated_key({"root_dir": "root_path"})
@validate_bbox
def _get_search(
    config_file: Path,
    search_dirs: list[str] | None = None,
    datasets: list[str] | None = None,
) -> list[str]:
    _search_items = []
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
    for _dir in map(strip_protocol, search_dirs or []):
        for name, cfg in config.items():
            if _dir.is_relative_to(strip_protocol(cfg.root_path)):
                _search_items.append(
                    CrawlerSettings(name=name, search_path=str(_dir))
                )

    return _search_items


async def async_call(
    index_system: str,
    method: str,
    batch_size: int = 2500,
    catalogue_file: Optional[Union[str, Path]] = None,
    *args: Any,
    **kwargs: Any,
) -> None:
    """Index metadata."""
    backends = load_plugins("metadata_crawler.ingester")
    try:
        cls = backends[index_system]
    except KeyError:
        msg = find_closest(
            f"No such backend: {index_system}", index_system, backends.keys()
        )
        raise ValueError(msg) from None
    obj = cls(batch_size=batch_size, catalogue_file=catalogue_file)
    func = getattr(obj, method)
    await func(**kwargs)


def call(
    index_system: str,
    method: str,
    batch_size: int = 2500,
    catalogue_file: Optional[Union[str, Path]] = None,
    *args: Any,
    **kwargs: Any,
):
    """Sync version of async_call."""
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    uvloop.run(
        async_call(
            index_system,
            method,
            batch_size=batch_size,
            catalogue_file=catalogue_file,
            *args,
            **kwargs,
        )
    )


async def async_index(
    index_system: str,
    catalogue_file: Union[Path, str],
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
    await async_call(
        index_system,
        "index",
        catalogue_file=catalogue_file,
        batch_size=batch_size,
        **kwargs,
    )


async def async_delete(
    index_system: str, batch_size: int = 2500, **kwargs: Any
) -> None:
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
    await call(index_system, "delete", batch_size=batch_size, **kwargs)


async def async_add(
    store: str | None = None,
    config_file: Path | None | str = None,
    data_object: list[str] | None = None,
    data_set: list[str] | None = None,
    data_store_prefix: str = "metadata",
    batch_size: int = 2500,
    comp_level: int = 4,
    catalogue_backend: str = "sqlite",
    latest_version: str = IndexName().latest,
    all_versions: str = IndexName().all,
    password: bool = False,
    threads: Optional[int] = None,
) -> None:
    """Harvest metdata from sotrage systems and add them to an intake catalogue

    Parameters
    ----------

    store:
        Path to the intake catalogue.
    config_file:
        Path to the drs-config file.
    data_objects:
        Instead of defining datasets that are to be crawled you can crawl
        data based on their directories. The directories must be a root dirs
        given in the drs-config file. By default all root dirs are crawled.
    data_ojbect:
        Objects (directories or catalogue files) that are processed.
    data_set:
        Dataset(s) that should be crawled. The datasets need to be defined
        in the drs-config file. By default all datasets are crawled.
    data_store_prefix: str
        Absolute path or relative path to intake catalogue source
    batch_size:
        Batch size that is used to collect the meta data. This can affect
        preformance.
    comp_level:
        Compression level used to write the meta data to csv.gz
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


    Example
    -------

        ::
            await async_add(
                "my-data.yaml",
                "~/data/drs-config.toml",
                data_set=["cmip6", "cordex"],
            )
    """
    config_file = config_file or os.environ.get("EVALUATION_SYSTEM_CONFIG_DIR")
    if not config_file:
        raise ValueError(("You must give a config file/directory"))
    if config_file.is_dir():
        cfg_file = Path(config_file).expanduser().absolute() / "drs_config.toml"
    else:
        cfg_file = Path(config_file).expanduser().absolute()

    st = time.time()
    passwd = ""
    if password:  # pragma: no cover
        passwd = Prompt.ask(
            "[b]Enter the password", password=True
        )  # pragma: no cover

    env = os.environ.copy()
    try:
        if passwd:
            os.environ["DRS_STORAGE_PASSWD"] = passwd
        async with DataCollector(
            cfg_file,
            store,
            IndexName(latest=latest_version, all=all_versions),
            *_get_search(cfg_file, data_object, data_set),
            batch_size=batch_size,
            comp_level=comp_level,
            backend=catalogue_backend,
            data_store_prefix=data_store_prefix,
            threads=threads,
        ) as data_col:
            await data_col.ingest_data()
            num_files = data_col.ingested_objects
            files_discovered = data_col.crawled_files
    finally:
        os.environ = env
    dt = timedelta(time.time() - st)
    logger.info("Discovered: %s files", f"{files_discovered:10,.0f}")
    logger.info("Ingested: %s files", f"{num_files:10,.0f}")
    logger.info("Spend: %s", dt)
    pprint(
        (
            f"[bold]Ingested [green]{num_files:10,.0f}[/green] "
            f"within [green]{dt}[/green][/bold]"
        )
    )
