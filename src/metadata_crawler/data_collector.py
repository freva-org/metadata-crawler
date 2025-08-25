"""Gather metadata and for adding them to a temporary metadata store."""

from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    Optional,
    Union,
    cast,
)

import tomlkit

from .api.config import CrawlerSettings, DRSConfig
from .api.metadata_stores import CatalogueWriter, IndexName
from .logger import logger
from .utils import Console, PrintLock, create_async_iterator, daemon


class DataCollector:
    """Collect file objects from a given directory object and search for files.

    Parameters
    ----------
    config_file:
        Path to the drs-config file / loaded configuration.
    *search_objects:
        Paths of the search directories. e.g. `root_path` attr in drs_config
    uri: str
        the uir of the metadata store.
    password: str
        Password for the ingestion
    batch_size: int
        Batch size for the ingestion
    """

    def __init__(
        self,
        config_file: Union[Path, str, Dict[str, Any], tomlkit.TOMLDocument],
        metadata_store: Optional[
            Union[Path, str, Dict[str, Any], tomlkit.TOMLDocument]
        ],
        index_name: IndexName,
        *search_objects: CrawlerSettings,
        **kwargs: Any,
    ):
        self._search_objects = search_objects
        if not search_objects:
            raise ValueError("You have to give search directories")
        self._num_files = 0
        self.index_name = index_name
        self.config = DRSConfig.load(config_file)
        self.ingest_queue = CatalogueWriter(
            str(metadata_store or "metadata.yaml"),
            index_name=index_name,
            config=self.config,
            **kwargs,
        )
        self.ingest_queue.run_consumer()
        self._print_status = False
        self._event_loop = asyncio.get_event_loop()

    @property
    def crawled_files(
        self,
    ) -> int:
        """Get the total number of crawled files."""
        return self._num_files

    @property
    def ingested_objects(self) -> int:
        """Get the number of ingested objects."""
        return self.ingest_queue.ingested_objects

    @property
    async def search_objects(self) -> AsyncIterator[tuple[str, str]]:
        """Async iterator for the search directories."""
        for cfg in self._search_objects:
            yield cfg.name, str(cfg.search_path)

    async def __aenter__(self) -> "DataCollector":
        return self

    async def __aexit__(self, *args: Any, **kwargs: Any) -> None:
        self._print_status = False
        self.ingest_queue.join_all_tasks()
        await self.ingest_queue.close()
        for dset in self.config.datasets.values():
            await dset.backend.close()

    @daemon
    def _print_performance(self) -> None:
        while self._print_status is True:
            start = time.time()
            num = self._num_files
            time.sleep(0.1)
            d_num = self._num_files - num
            dt = time.time() - start
            perf_file = d_num / dt
            queue_size = self.ingest_queue.size
            f_col = p_col = q_col = "blue"
            if perf_file > 500:
                f_col = "green"
            if perf_file < 100:
                f_col = "red"
            if queue_size > 100_000:
                q_col = "red"
            if queue_size < 10_000:
                q_col = "green"
            msg = (
                f"[bold]Discovering: [{f_col}]{perf_file:>6,.1f}[/{f_col}] "
                "files / sec. #files discovered: "
                f"[blue]{self.crawled_files:>10,.0f}[/blue]"
                f" in queue: [{q_col}]{queue_size:>6,.0f}[/{q_col}] "
                f"#indexed files: "
                f"[{p_col}]{self.ingested_objects:>10,.0f}[/{p_col}][/bold] "
                f"{20 * ' '}"
            )
            if not PrintLock.locked():  # pragma: no cover
                Console.print(msg, end="\r")
        Console.print()

    async def _ingest_dir(
        self,
        drs_type: str,
        search_dir: str,
        iterable: bool = True,
    ) -> None:
        if iterable:
            try:
                sub_dirs = self.config.datasets[drs_type].backend.iterdir(
                    search_dir
                )
            except Exception as error:
                logger.error(error)
                return
        else:
            sub_dirs = cast(
                AsyncIterator[str], create_async_iterator([search_dir])
            )
        rank = 0
        async for _dir in sub_dirs:
            async for _inp in self.config.datasets[drs_type].backend.rglob(
                _dir, self.config.datasets[drs_type].glob_pattern
            ):
                await self.ingest_queue.put(
                    _inp, drs_type, name=self.index_name.all
                )
                if rank == 0:
                    await self.ingest_queue.put(
                        _inp, drs_type, name=self.index_name.latest
                    )
                self._num_files += 1
            rank += 1
        return None

    async def _iter_content(
        self, drs_type: str, inp_dir: str, pos: int = 0
    ) -> None:
        """Walk recursively content until files are found or until the version."""
        op: Optional[Callable[..., Awaitable[None]]] = None
        store = self.config.datasets[drs_type].backend
        try:
            is_file = await store.is_file(inp_dir)
            iterable = await store.is_dir(inp_dir)
            suffix = await store.suffix(inp_dir)
        except Exception as error:
            logger.error("Error checking file %s", error)
            return
        iterable = False if suffix == ".zarr" else iterable
        if is_file and suffix in self.config.suffixes:
            op = self._ingest_dir
        elif pos <= 0 or suffix == ".zarr":
            op = self._ingest_dir
        if op:
            try:
                await op(drs_type, inp_dir, iterable=iterable)
            except Exception as error:
                logger.error(error)
            return
        # fallback to recursing into sub-dirs
        try:
            async for sub in store.iterdir(inp_dir):
                await self._iter_content(drs_type, sub, pos - 1)
        except Exception as error:
            logger.error(error)

    async def ingest_data(self) -> None:
        """Walk sub directories until files are found or until the version."""
        futures = []
        async for drs_type, path in self.search_objects:
            pos = self.config.max_directory_tree_level(path, drs_type=drs_type)
            future = self._event_loop.create_task(
                self._iter_content(drs_type, path, pos)
            )
            futures.append(future)
        self._print_status = True
        self._print_performance()
        await asyncio.gather(*futures)
        logger.info("%i ingestion tasks have been completed", len(futures))
        self._print_status = False
        self.ingest_queue.join_all_tasks()
