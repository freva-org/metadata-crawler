"""API for adding new cataloging systems via :py:class:`BaseIngest`
==================================================================
"""

from __future__ import annotations

import abc
import asyncio
import csv
import gzip
import multiprocessing as mp
from concurrent.futures import ThreadPoolExecutor
from queue import Empty, Queue
from threading import Lock, Thread
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    ClassVar,
    Coroutine,
    Dict,
    List,
    Optional,
    Tuple,
)

import yaml

from ..logger import logger


def _run_async(
    method: Callable[[Any], Coroutine[Any, Any, None]],
    *args: Any,
    **kwargs: Any,
) -> Any:
    """Run a async method in a sync. environment."""
    return asyncio.run(method(*args, **kwargs))


class BaseIngest(metaclass=abc.ABCMeta):
    """Base class to ingest metadata into the cataloging system.

    Any data ingestion class that implements metadata ingestion into
    cataloguing systems should inherit from this class.

    This abstract class will setup consumer threads and a fifo queue that wait
    for new data to harvest metadata and add it to the cataloguing system.
    Only :py:func:`add` and :py:func:`delete` are abstract methods that need
    to be implemented for each cataloguing ingestion class. The rest is done
    by this base class.

    Parameters
    ----------
    uri:
        An optional argument for the url or path to the storage system.
    batch_size:
        The amount for metadata that should be gathered `before` ingesting
        it into the catalogue.

    Attributes
    ----------
    """

    name: ClassVar[str] = ""
    """The short name of the cataloguing system."""

    @abc.abstractmethod
    def __init__(
        self,
        uri: Optional[str] = None,
        batch_size: int = 2500,
    ):
        self._done = 0
        self._num_objects = 0
        self.lock = Lock()
        self._tasks = {
            i: Thread(
                target=_run_async,
                args=(
                    self._run_consumer_task,
                    batch_size,
                    i,
                ),
            )
            for i in range(min(mp.cpu_count(), 15))
        }
        self.executor = ThreadPoolExecutor(max_workers=min(mp.cpu_count(), 15))
        self._queue: Queue[Tuple[Awaitable[Dict[str, Any]], str]] = Queue()
        self._uri = uri
        self._inp_data = {}
        self._files = []
        _ = self.input_data
        self.batch_size = batch_size
        self.cores = "files", "latest"

    async def get_metadata(
        self, core: str
    ) -> AsyncIterator[List[Dict[str, Any]]]:
        """Get the metdata in batches."""
        batch = []
        for num, data in enumerate(self.input_data[core]):
            batch.append({k: v for k, v in data.items() if v})
            if num > 0 and num % self.batch_size == 0:
                yield batch
                batch = []
        if batch:
            yield batch

    @property
    def input_data(self) -> Dict[str, csv.DictReader]:
        """Meta data streams."""
        if not self._inp_data and self._uri:
            with open(self._uri) as stream:
                cat = yaml.safe_load(stream)
                for core in cat["sources"]:
                    stream = gzip.open(
                        cat["sources"][core]["args"]["urlpath"], mode="rt"
                    )
                    self._files.append(stream)
                    self._inp_data[core] = csv.DictReader(stream)
        return self._inp_data

    async def put(
        self,
        future: Awaitable[Dict[str, Any]],
        name: str | None = None,
    ) -> None:
        """Add items to the fifo queue.

        This method is used by the data crawling (discovery) method
        to add the name of the catalogue, the path to the input file object
        and a reference of the Data Reference Syntax class for this
        type of dataset.

        Parameters
        ----------
        info_obj:
            The name of the input object path where the metadata should be
            retrieved from catalogue_entry
        cls:
            An instance of the DRS class that fits to the input file object.
        name:
            Name of the catalogue, if applicable. This variable depends on
            the cataloguing system. For example apache solr would use a `core`.
        """
        self._queue.put((name, future))

    @property
    def ingested_objects(self) -> int:
        """Get the number of ingested objects."""
        return self._num_objects

    @property
    def size(self) -> int:
        """Get the size of the worker queue."""
        return self._queue.qsize()

    def join_all_tasks(self) -> None:
        """Block the execution until all tasks are marked as done."""
        logger.debug("Releasing consumers from their duty.")
        with self.lock:
            self._done = 1
        for task in self._tasks.values():
            task.join()
        self.executor.shutdown(wait=True)

    @abc.abstractmethod
    async def delete(self, **kwargs: Any) -> None:
        """Delete data from the cataloguing system.

        Parameters
        ----------
        flush:
            Boolean indicating whether or not the data should be flushed after
            amending the catalogue (if implemented).
        search_keys:
            key-value based query for data that should be deleted.

        """

    @abc.abstractmethod
    async def index(
        self,
        metadata: Optional[dict[str, Any]] = None,
        core: Optional[str] = None,
        **kwags: Any,
    ) -> None:
        """Add metadata into the cataloguing system.

        Parameters
        ----------
        metadata_batch:
            batch of metadata stored in a two valued tuple. The first entry
            of the tuple represents a name of the catalog. This entry
            might have different meanings for different cataloguing systems.
            For example apache solr will receive the name of the ``core``.
            The second  entry is the meta data itself, saved in a dictionary.
        flush:
            Boolean indicating whether or not the data should be flushed after
            adding to the catalogue (if implemented)
        """

    async def __aenter__(self) -> "BaseIngest":
        return self

    async def __aexit__(self, *args: Any, **kwargs: Any) -> None:
        await self.close()

    async def close(self) -> None:
        """Base method for closing any connections."""
        for stream in self._files:
            stream.close()

    def run_consumer(self) -> None:
        """Setup all the consumers."""
        for task in self._tasks.values():
            task.start()

    async def _run_consumer_task(self, batch_size: int, this_worker: int) -> None:
        """Setup a consumer task waiting for incoming data to be ingested."""
        logger.info("Adding %i consumer to consumers.", this_worker)
        batch: list[tuple[str, dict[str, str | list[str] | list[float]]]] = []
        nbatch = 0
        while self._done == 0:
            try:
                name, future = self._queue.get(timeout=1)
                metadata = future.result()
            except Empty:
                continue
            except Exception as error:
                logger.error(error)
                continue
            nbatch += 1
            batch.append((name, metadata.copy()))
            del metadata
            if nbatch >= batch_size:
                logger.info("Ingesting %i items", batch_size)
                try:
                    await self.add(batch, flush=True)
                    with self.lock:
                        self._num_objects += len(batch)
                except Exception as error:  # pragma: no cover
                    logger.error(error)  # pragma: no cover
                del batch
                nbatch = 0
                batch = []
        # If in the meanwhile new data has arrive, take it.
        # TODO: Find out an approach to put this under a test.
        while not self._queue.empty():
            try:
                name, future = self._queue.get(timeout=1)
                metadata = future.result()
            except Empty:
                break
            except Exception as error:
                logger.error(error)
                continue
            batch.append((name, metadata.copy()))
            del metadata
        if batch:
            logger.info("Ingesting last %i items", len(batch))
            try:
                await self.add(batch, flush=True)
                with self.lock:
                    self._num_objects += len(batch)
            except Exception as error:  # pragma: no cover
                logger.error(error)
        logger.info("Closing consumer %i", this_worker)
        del batch
