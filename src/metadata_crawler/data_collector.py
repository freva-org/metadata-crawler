"""Gather metadata and for adding them to a temporary metadata store."""

from __future__ import annotations

import asyncio
import os
from multiprocessing import Event, Value
from pathlib import Path
from types import TracebackType
from typing import (
    Any,
    AsyncIterator,
    Callable,
    Coroutine,
    Dict,
    Iterator,
    Optional,
    Tuple,
    Type,
    Union,
    cast,
)

import tomlkit

from .api.config import CrawlerSettings, DRSConfig
from .api.metadata_stores import CatalogueWriter, IndexName
from .api.storage_backend import PathTemplate
from .logger import logger
from .utils import (
    Counter,
    MetadataCrawlerException,
    create_async_iterator,
    print_performance,
)

ScanItem = Tuple[str, str, bool, bool]


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
            raise MetadataCrawlerException("You have to give search directories")
        self._num_files: Counter = Value("i", 0)
        self.index_name = index_name
        self.config = DRSConfig.load(config_file)
        kwargs.setdefault("scan_concurrency", os.getenv("SCAN_CONCURRENCY", "64"))
        self._scan_concurrency: int = int(kwargs.pop("scan_concurrency", 64))
        self._scan_queue: asyncio.Queue[Optional[ScanItem]] = asyncio.Queue(
            maxsize=int(kwargs.pop("scan_queue_size", 10_000))
        )
        self._print_status = Event()
        self.ingest_queue = CatalogueWriter(
            str(metadata_store or "metadata.yaml"),
            index_name=index_name,
            config=config_file,
            **kwargs,
        )
        self.ingest_queue.run_consumer()
        self._max_files = int(cast(str, os.getenv("MDC_MAX_FILES", "-1")))

    @property
    def crawled_files(
        self,
    ) -> int:
        """Get the total number of crawled files."""
        return self._num_files.value

    @property
    def ingested_objects(self) -> int:
        """Get the number of ingested objects."""
        return self.ingest_queue.ingested_objects

    @property
    def search_objects(self) -> Iterator[tuple[str, str]]:
        """Async iterator for the search directories."""
        for cfg in self._search_objects:
            yield cfg.name, str(cfg.search_path)

    async def __aenter__(self) -> "DataCollector":
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: TracebackType,
    ) -> None:
        self._print_status.clear()
        self.ingest_queue.join_all_tasks()
        await self.ingest_queue.close()

        async def _safe_close(b: PathTemplate) -> None:
            try:
                await asyncio.wait_for(b.close(), timeout=3)
            except Exception:
                pass

        await asyncio.gather(
            *[_safe_close(ds.backend) for ds in self.config.datasets.values()],
            return_exceptions=True,
        )

    def _test_env(self) -> bool:
        return (
            True
            if self._max_files > 0 and self._max_files < self.crawled_files
            else False
        )

    async def _ingest_dir(
        self,
        drs_type: str,
        search_dir: str,
        iterable: bool = True,
        is_versioned: bool = True,
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
                if self._test_env():
                    return
                await self.ingest_queue.put(
                    _inp, drs_type, name=self.index_name.all
                )
                if rank == 0 or is_versioned is False:
                    await self.ingest_queue.put(
                        _inp, drs_type, name=self.index_name.latest
                    )
                self._num_files.value += 1
            rank += 1
        return None

    async def _scan_worker(self) -> None:
        """Drain _scan_queue and run _ingest_dir concurrently (bounded pool)."""
        while True:
            item = await self._scan_queue.get()  # blocks
            if item is None:  # sentinel -> exit
                # do not task_done() for sentinel
                break
            drs_type, path, iterable, is_versioned = item
            try:
                await self._ingest_dir(
                    drs_type, path, iterable=iterable, is_versioned=is_versioned
                )
            except Exception as error:
                logger.error(error)
            finally:
                self._scan_queue.task_done()

    async def _iter_content(
        self,
        drs_type: str,
        inp_dir: str,
        pos: int = 0,
        is_versioned: bool = True,
    ) -> None:
        """Walk recursively until files or the version level is reached."""
        store = self.config.datasets[drs_type].backend
        if self._test_env():
            return
        try:
            is_file, iterable, suffix = await asyncio.gather(
                store.is_file(inp_dir),
                store.is_dir(inp_dir),
                store.suffix(inp_dir),
            )
        except Exception as error:
            logger.error("Error checking file %s", error)
            return

        iterable = False if suffix == ".zarr" else iterable
        op: Optional[Callable[..., Coroutine[Any, Any, None]]] = None
        if is_file and suffix in self.config.suffixes:
            op = self._ingest_dir
        elif pos <= 0 or suffix == ".zarr":
            op = self._ingest_dir

        if op is not None:
            # enqueue the heavy scan; workers will run _ingest_dir concurrently
            await self._scan_queue.put(
                (drs_type, inp_dir, iterable, is_versioned)
            )
            return

        # otherwise, recurse sequentially (cheap) — no task per directory
        try:
            async for sub in store.iterdir(inp_dir):
                await self._iter_content(
                    drs_type, sub, pos - 1, is_versioned=is_versioned
                )
        except Exception as error:
            logger.error(error)

    async def ingest_data(self) -> None:
        """Produce scan tasks and process them with a bounded worker pool."""
        self._print_status.set()
        self._num_files.value = 0
        print_performance(
            self._print_status,
            self._num_files,
            self.ingest_queue.queue,
            self.ingest_queue.num_objects,
        )

        async with asyncio.TaskGroup() as tg:
            # start scan workers
            for _ in range(self._scan_concurrency):
                tg.create_task(self._scan_worker())

            # produce scan items by walking roots sequentially
            for drs_type, path in self.search_objects:  # <- property is sync
                pos, is_versioned = self.config.max_directory_tree_level(
                    path, drs_type=drs_type
                )
                if pos < 0:
                    logger.warning(
                        "Can't define latest version of versioned dataset."
                        " This might lead to unexpected results. Try adjusting"
                        " your search path."
                    )

                await self._iter_content(
                    drs_type, path, pos, is_versioned=is_versioned
                )

            # wait until all queued scan items are processed
            await self._scan_queue.join()

            # stop workers (one sentinel per worker)
            for _ in range(self._scan_concurrency):
                await self._scan_queue.put(None)

        logger.info(
            "%i ingestion tasks have been completed", len(self._search_objects)
        )
        self.ingest_queue.join_all_tasks()
        self._print_status.clear()
