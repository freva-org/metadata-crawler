from __future__ import annotations

import asyncio
import time
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Iterable,
    Optional,
    Tuple,
    Union,
)

from anyio import Path

from .api.config import CrawlerSettings, DRSConfig
from .ingester import get_ingest_instance
from .logger import logger
from .utils import Console, PrintLock, daemon


class DataCollector:
    """Collect file objects from a given directory object and search for files.


    Parameters
    ----------
    config_file: Path | str
        Path to the drs config file holding the file type information
    *search_objects: Path | str
        Paths of the search directories. e.g. `root_path` attr in drs_config
    uri: str
        the uir of the metadata store.
    password: str
        Password for the ingestion
    batch_size: int
        Batch size for the ingestion
    cores: str, str
        Names of the cores for latest and all metadata versions.
    """

    def __init__(
        self,
        config_file: Union[Path, str],
        metadata_store: Optional[str] = None,
        *search_objects: CrawlerSettings,
        batch_size: int = 2500,
        comp_level: int = 4,
        cores: Tuple[str, str] = ("latest", "files"),
    ):
        self._search_objects = search_objects
        if not search_objects:
            raise ValueError("You have to give search directories")
        self._num_files = 0
        self._all_versions = cores[-1]
        self._latest_version = cores[0]
        self.config = DRSConfig.load(config_file)
        self.ingest_queue = get_ingest_instance(
            metadata_store=metadata_store,
            batch_size=batch_size,
            comp_level=comp_level,
            cores=cores,
            facets=self.config.facets,
        )
        self.ingest_queue.run_consumer()
        self._print_status = False
        try:
            self._event_loop = asyncio.get_event_loop()
        except RuntimeError:
            self._event_loop = asyncio.new_event_loop()

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

    async def __aenter__(self) -> DataCollector:
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

    @staticmethod
    async def _create_async_iterator(itt: Iterable[Any]) -> AsyncIterator[Any]:
        for item in itt:
            yield item

    async def _ingest_dir(self, drs_type: str, search_dir: str) -> None:
        try:
            sub_dirs = self.config.datasets[drs_type].backend.iterdir(search_dir)
        except Exception as error:
            logger.error(error)
            return
        rank = 0
        async for _dir in sub_dirs:
            async for _inp in self.config.datasets[drs_type].backend.rglob(
                _dir, self.config.datasets[drs_type].glob_pattern
            ):
                future = self.ingest_queue.executor.submit(
                    self.config.read_metadata,
                    drs_type,
                    _inp,
                )
                await self.ingest_queue.put(future, name=self._all_versions)
                if rank == 0:
                    await self.ingest_queue.put(future, name=self._latest_version)
                self._num_files += 1
            rank += 1
        return None

    async def _iter_content(
        self, drs_type: str, inp_dir: str, pos: int = 0
    ) -> None:
        """
        walk recursively content until files are found or until the version.
        """

        op: Optional[Callable[[str, str], Awaitable[None]]] = None
        store = self.config.datasets[drs_type].backend
        try:
            is_file = await store.is_file(inp_dir)
            suffix = await store.suffix(inp_dir)
        except Exception as error:
            logger.error("Error checking file %s", error)
            return
        if is_file and suffix in self.config.suffixes:
            op = self._ingest_dir
        elif pos <= 0 or suffix == ".zarr":
            op = self._ingest_dir
        if op:
            try:
                await op(drs_type, inp_dir)
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
        """
        Walk sub directories until files are found or until the version.
        """
        futures = []
        async for drs_type, path in self.search_objects:
            pos = self.config.max_directory_tree_level(path, drs_type=drs_type)
            future = self._event_loop.create_task(
                self._iter_content(drs_type, path, pos)
            )
            futures.append(future)
        if not futures:
            return
        self._print_status = True
        self._print_performance()
        await asyncio.gather(*futures)
        logger.info("%i ingestion tasks have been completed", len(futures))
        self._print_status = False
        self.ingest_queue.join_all_tasks()
