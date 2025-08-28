"""Metadata Storage definitions."""

from __future__ import annotations

import abc
import gzip
import json
import multiprocessing as mp
import os
import re
import time
from datetime import datetime
from enum import Enum
from io import BytesIO
from multiprocessing import queues, sharedctypes
from pathlib import Path
from typing import (
    Any,
    AsyncIterator,
    ClassVar,
    Dict,
    List,
    Literal,
    NamedTuple,
    Optional,
    Tuple,
    Type,
    TypeAlias,
    Union,
    cast,
)

import fsspec
import orjson
import tomlkit
import yaml

import metadata_crawler

from ..logger import logger
from ..utils import create_async_iterator
from .config import DRSConfig, SchemaField
from .storage_backend import MetadataType

ISO_FORMAT_REGEX = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z?$"
)

BATCH_SECS_THRESHOLD = 20
BATCH_ITEM = List[Tuple[str, Dict[str, Any]]]


ConsumerQueueType = queues.Queue[Union[int, Tuple[str, str, MetadataType]]]
WriterQueueType = queues.SimpleQueue[Union[int, BATCH_ITEM]]


class Stream(NamedTuple):
    """A representation of a path stream as named tuple."""

    name: str
    path: str


class DateTimeEncoder(json.JSONEncoder):
    """JSON‐Encoder that emits datetimes as ISO‐8601 strings."""

    def default(self, obj: Any) -> Any:
        """Set default time encoding."""
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


class DateTimeDecoder(json.JSONDecoder):
    """JSON Decoder that converts ISO‐8601 strings to datetime objects."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(object_hook=self._decode_objects, *args, **kwargs)

    def _decode_datetime(self, obj: Any) -> Any:
        if isinstance(obj, list):
            return list(map(self._decode_datetime, obj))
        elif isinstance(obj, dict):
            for key in obj:
                obj[key] = self._decode_datetime(obj[key])
        if isinstance(obj, str):
            try:
                return datetime.fromisoformat(obj.replace("Z", "+00:00"))
            except ValueError:
                return obj
        return obj

    def _decode_objects(self, obj: Dict[str, Any]) -> Any:
        for key, value in obj.items():
            obj[key] = self._decode_datetime(value)
        return obj


class IndexName(NamedTuple):
    """A paired set of metadata indexes representations.

        - `latest`: Metadata for the latest version of each dataset.
        - `files`: Metadata for all available versions of datasets.

    This abstraction is backend-agnostic and can be used with any index system,
    such as Apache Solr cores, MongoDB collections, or SQL tables.

    """

    latest: str = "latest"
    all: str = "files"


class IndexStore:
    """Base class for all metadata stores."""

    suffix: ClassVar[str]
    """Path suffix of the metadata store."""

    driver: ClassVar[str]
    """Intake driver."""

    def __init__(
        self,
        path: str,
        index_name: IndexName,
        schema: Dict[str, SchemaField],
        batch_size: int = 25_000,
        mode: Literal["r", "w"] = "r",
        storage_options: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        self.storage_options = storage_options or {}
        self._ctx = mp.get_context("spawn")
        self.queue: WriterQueueType = self._ctx.SimpleQueue()
        self._sent = 42
        self._fs, self._is_local_path = self.get_fs(path, **self.storage_options)
        self._path = self._fs.unstrip_protocol(path)
        self.schema = schema
        self.batch_size = batch_size
        self.index_names: Tuple[str, str] = (index_name.latest, index_name.all)
        self.mode = mode
        self._rows_since_flush = 0
        self._last_flush = time.time()
        self._paths: List[Stream] = []
        for name in self.index_names:
            out_path = self.get_path(name)
            self._paths.append(Stream(name=name, path=out_path))

    @staticmethod
    def get_fs(
        uri: str, **storage_options: Any
    ) -> Tuple[fsspec.AbstractFileSystem, bool]:
        """Get the base-url from a path."""
        storage_options = storage_options or {"anon": True}
        protocol, path = fsspec.core.split_protocol(uri)
        protocol = protocol or "file"
        fs = fsspec.filesystem(protocol, **storage_options)
        return fs, protocol == "file"

    @abc.abstractmethod
    async def read(
        self,
        index_name: str,
    ) -> AsyncIterator[List[Dict[str, Any]]]:
        """Yield batches of metadata records from a specific table.

        Parameters
        ^^^^^^^^^^
        index_name:
            The name of the index_name.

        Yields
        ^^^^^^
        List[Dict[str, Any]]:
            Deserialised metadata records.
        """
        yield [{}]  # pragma: no cover

    def get_path(self, path_suffix: Optional[str] = None) -> str:
        """Construct a path name for a given suffix."""
        path = self._path.removesuffix(self.suffix)
        new_path = (
            f"{path}-{path_suffix}{self.suffix}"
            if path_suffix
            else f"{path}{self.suffix}"
        )
        return new_path

    def join(self) -> None:
        """Shutdown the writer task."""
        self.queue.put(self._sent)
        if self.proc is not None:
            self.proc.join()

    def close(self) -> None:
        """Shutdown the write worker."""
        self.join()

    @property
    def proc(self) -> Optional["mp.process.BaseProcess"]:
        """The writer process."""
        raise NotImplementedError("This property must be defined.")

    @abc.abstractmethod
    def get_args(self, index_name: str) -> Dict[str, Any]:
        """Define the intake arguments."""
        ...  # pragma: no cover


class JSONLineWriter:
    """Write JSONLines to disk."""

    def __init__(
        self, *streams: Stream, comp_level: int = 4, **storage_options: Any
    ) -> None:

        self._comp_level = comp_level
        self._streams: Dict[str, BytesIO] = {}
        self._index_names = [s.name for s in streams]
        for _stream in streams:
            fs, _ = IndexStore.get_fs(_stream.path, **storage_options)
            parent = os.path.dirname(_stream.path).rstrip("/")
            try:
                fs.makedirs(parent, exist_ok=True)
            except Exception:  # pragma: no cover
                pass  # pragma: no cover
            self._streams[_stream.name] = fs.open(_stream.path, mode="wb")

    @classmethod
    def as_daemon(
        cls,
        queue: WriterQueueType,
        semaphore: int,
        *streams: Stream,
        comp_level: int = 4,
        **storage_options: Any,
    ) -> None:
        """Start the writer process as a daemon."""
        this = cls(*streams, comp_level=comp_level, **storage_options)
        get = queue.get
        add = this._add
        while True:
            item = get()
            if item == semaphore:
                logger.info("Closing writer task.")
                break
            try:
                add(cast(BATCH_ITEM, item))
            except Exception as error:
                logger.error(error)
        this.close()

    @staticmethod
    def _encode_records(records: List[Dict[str, Any]]) -> bytes:
        """Serialize a list of dicts into one JSONL bytes blob."""
        parts = [orjson.dumps(rec) for rec in records]
        return b"".join(p + b"\n" for p in parts)

    def _gzip_once(self, payload: bytes) -> bytes:
        """Compress a whole JSONL blob into a single gz member (fast)."""
        return gzip.compress(payload, compresslevel=self._comp_level)

    def _add(self, metadata_batch: List[Tuple[str, Dict[str, Any]]]) -> None:
        """Add a batch of metadata to the gzip store."""
        by_index: Dict[str, List[Dict[str, Any]]] = {
            name: [] for name in self._index_names
        }
        for index_name, metadata in metadata_batch:
            by_index[index_name].append(metadata)
        for index_name, records in by_index.items():
            if not records:
                continue
            payload = self._encode_records(records)
            gz = self._gzip_once(payload)
            self._streams[index_name].write(gz)

    def close(self) -> None:
        """Close the files."""
        for stream in self._streams.values():
            try:
                stream.flush()
            except Exception:
                pass
            stream.close()


class JSONLines(IndexStore):
    """Write metadata to gzipped JSONLines files."""

    suffix = ".json.gz"
    driver = "intake.source.jsonfiles.JSONLinesFileSource"

    def __init__(
        self,
        path: str,
        index_name: IndexName,
        schema: Dict[str, SchemaField],
        mode: Literal["w", "r"] = "r",
        storage_options: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ):
        super().__init__(
            path,
            index_name,
            schema,
            mode=mode,
            storage_options=storage_options,
        )
        _comp_level = int(kwargs.get("comp_level", "4"))
        self._proc: Optional["mp.process.BaseProcess"] = None
        if mode == "w":
            self._proc = self._ctx.Process(
                target=JSONLineWriter.as_daemon,
                args=(
                    self.queue,
                    self._sent,
                )
                + tuple(self._paths),
                kwargs={**{"comp_level": _comp_level}, **self.storage_options},
                daemon=True,
            )
            self._proc.start()

    @property
    def proc(self) -> Optional["mp.process.BaseProcess"]:
        """The writer process."""
        return self._proc

    def get_args(self, index_name: str) -> Dict[str, Any]:
        """Define the intake arguments."""
        return {
            "urlpath": self.get_path(index_name),
            "compression": "gzip",
            "text_mode": True,
            "storage_options": self.storage_options,
        }

    async def read(
        self,
        index_name: str,
    ) -> AsyncIterator[List[Dict[str, Any]]]:
        """Yield batches of metadata records from a specific table.

        Parameters
        ^^^^^^^^^^
        index_name:
            The name of the index_name.

        Yields
        ^^^^^^^
        List[Dict[str, Any]]:
            Deserialised metadata records.
        """
        with self._fs.open(
            self.get_path(index_name),
            mode="rt",
            compression="gzip",
            encoding="utf-8",
        ) as stream:
            chunk: List[Dict[str, Any]] = []
            async for line in create_async_iterator(stream):
                chunk.append(json.loads(line, cls=DateTimeDecoder))
                if len(chunk) >= self.batch_size:
                    yield chunk
                    chunk = []
            if chunk:
                yield chunk


class CatalogueBackends(Enum):
    """Define the implemented catalogue backends."""

    jsonlines = JSONLines


CatalogueBackendType: TypeAlias = Literal["jsonlines"]


class CatalogueReader:
    """Backend for reading the content of an intake catalogue.

    Parameters
    ^^^^^^^^^^
    catalogue_file:
        Path to the intake catalogue
    batch_size:
        Size of the metadata chunks that should be read.
    """

    def __init__(
        self,
        catalogue_file: Union[str, Path],
        batch_size: int = 2500,
        storage_options: Optional[Dict[str, Any]] = None,
    ) -> None:
        catalogue_file = str(catalogue_file)
        storage_options = storage_options or {}
        fs, _ = IndexStore.get_fs(catalogue_file, **storage_options)
        path = fs.unstrip_protocol(catalogue_file)
        with fs.open(path) as stream:
            cat = yaml.safe_load(stream.read())
        _schema_json = cat["metadata"]["schema"]
        schema = {s["key"]: SchemaField(**s) for k, s in _schema_json.items()}
        index_name = IndexName(**cat["metadata"]["index_names"])
        cls: Type[IndexStore] = CatalogueBackends[
            cat["metadata"]["backend"]
        ].value
        storage_options = cat["metadata"].get("storage_options", {})
        self.store = cls(
            cat["metadata"]["prefix"],
            index_name,
            schema,
            mode="r",
            batch_size=batch_size,
            storage_options=storage_options,
        )


class QueueConsumer:
    """Class that consumes the file discovery queue."""

    def __init__(
        self,
        config: Optional[Union[str, Path]],
        num_objects: "sharedctypes.Synchronized[Any]",
        writer_queue: WriterQueueType,
    ) -> None:
        self.config = DRSConfig.load(config)
        self._writer_queue = writer_queue
        self._num_objects = num_objects

    def _flush_batch(
        self,
        batch: List[Tuple[str, Dict[str, Any]]],
    ) -> None:
        logger.info("Ingesting %i items", len(batch))
        try:
            self._writer_queue.put(batch.copy())
            with self._num_objects.get_lock():
                self._num_objects.value += len(batch)
        except Exception as error:  # pragma: no cover
            logger.error(error)  # pragma: no cover
        batch.clear()

    @classmethod
    def run_consumer_task(
        cls,
        queue: ConsumerQueueType,
        writer_queue: WriterQueueType,
        config: Optional[Union[str, Path]],
        num_objects: "sharedctypes.Synchronized[Any]",
        batch_size: int,
        poison_pill: int,
    ) -> None:
        """Set up a consumer task waiting for incoming data to be ingested."""
        this = cls(config, num_objects, writer_queue)
        this_worker = os.getpid()
        logger.info("Adding %i consumer to consumers.", this_worker)
        batch: List[Tuple[str, Dict[str, Any]]] = []
        append = batch.append
        read_metadata = this.config.read_metadata
        flush = this._flush_batch
        get = queue.get
        while True:
            item = get()
            if item == poison_pill:
                break
            try:
                name, drs_type, inp = cast(Tuple[str, str, MetadataType], item)
                metadata = read_metadata(drs_type, inp)
            except Exception as error:
                logger.error(error)
                continue
            append((name, metadata))
            if len(batch) >= batch_size:
                flush(batch)
        if batch:
            flush(batch)
        logger.info("Closing consumer %i", this_worker)


class CatalogueWriter:
    """Create intake catalogues that store metadata entries.

    Parameters
    ^^^^^^^^^^
    yaml_path:
        Path the to intake catalogue that should be created.
    index_name:
        Names of the metadata indexes.
    data_store_prefix:
        Prefix of the path/url where the metadata is stored.
    batch_size:
        Size of the metadata chunks that should be added to the data store.
    index_schema:
        Schema of the metadata
    """

    def __init__(
        self,
        yaml_path: str,
        index_name: IndexName,
        data_store_prefix: str = "metadata",
        backend: str = "jsonlines",
        batch_size: int = 2500,
        config: Optional[
            Union[Path, str, Dict[str, Any], tomlkit.TOMLDocument]
        ] = None,
        n_procs: Optional[int] = None,
        storage_options: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        self.config = DRSConfig.load(config)
        storage_options = storage_options or {}
        self.fs, _ = IndexStore.get_fs(yaml_path, **storage_options)
        self.path = self.fs.unstrip_protocol(yaml_path)
        scheme, _, _ = data_store_prefix.rpartition("://")
        self.backend = backend
        if not scheme and not os.path.isabs(data_store_prefix):
            data_store_prefix = os.path.join(
                os.path.abspath(os.path.dirname(yaml_path)), data_store_prefix
            )
        self.prefix = data_store_prefix
        self.index_name = index_name
        cls: Type[IndexStore] = CatalogueBackends[backend].value
        self.store = cls(
            data_store_prefix,
            index_name,
            self.config.index_schema,
            mode="w",
            storage_options=storage_options,
            **kwargs,
        )
        self._ctx = mp.get_context("spawn")
        self._queue: ConsumerQueueType = self._ctx.Queue()
        self._poison_pill = 13
        self._num_objects = self._ctx.Value("i", 0)
        n_procs = n_procs or min(mp.cpu_count(), 15)
        batch_size_per_proc = max(int(batch_size / n_procs), 100)
        self._tasks = [
            self._ctx.Process(
                target=QueueConsumer.run_consumer_task,
                args=(
                    self._queue,
                    self.store.queue,
                    config,
                    self._num_objects,
                    batch_size_per_proc,
                    self._poison_pill,
                ),
            )
            for i in range(n_procs)
        ]

    async def put(
        self,
        inp: MetadataType,
        drs_type: str,
        name: str = "",
    ) -> None:
        """Add items to the fifo queue.

        This method is used by the data crawling (discovery) method
        to add the name of the catalogue, the path to the input file object
        and a reference of the Data Reference Syntax class for this
        type of dataset.

        Parameters
        ^^^^^^^^^^
        inp:
            Path and metadata of the discovered object.
        drs_type:
            The data type the discovered object belongs to.
        name:
            Name of the catalogue, if applicable. This variable depends on
            the cataloguing system. For example apache solr would use a `core`.
        """
        self._queue.put((name, drs_type, inp))

    @property
    def ingested_objects(self) -> int:
        """Get the number of ingested objects."""
        return cast(int, self._num_objects.value)

    @property
    def size(self) -> int:
        """Get the size of the worker queue."""
        return self._queue.qsize()

    def join_all_tasks(self) -> None:
        """Block the execution until all tasks are marked as done."""
        logger.debug("Releasing consumers from their duty.")
        for _ in self._tasks:
            self._queue.put(self._poison_pill)
        for task in self._tasks:
            task.join()
        self.store.join()

    async def close(self) -> None:
        """Close any connections."""
        self.store.join()
        self.store.close()
        self._create_catalogue_file()

    def run_consumer(self) -> None:
        """Set up all the consumers."""
        for task in self._tasks:
            task.start()

    def _create_catalogue_file(self) -> None:
        catalog = {
            "description": (
                f"{metadata_crawler.__name__} "
                f"(v{metadata_crawler.__version__})"
                f" at {datetime.now().strftime('%c')}"
            ),
            "metadata": {
                "version": 1,
                "backend": self.backend,
                "prefix": self.prefix,
                "storage_options": self.store.storage_options,
                "index_names": {
                    "latest": self.index_name.latest,
                    "all": self.index_name.all,
                },
                "schema": {
                    k: json.loads(s.model_dump_json())
                    for k, s in self.store.schema.items()
                },
            },
            "sources": {
                self.index_name.latest: {
                    "description": "Latest metadata versions.",
                    "driver": self.store.driver,
                    "args": self.store.get_args(self.index_name.latest),
                },
                self.index_name.all: {
                    "description": "All metadata versions only.",
                    "driver": self.store.driver,
                    "args": self.store.get_args(self.index_name.all),
                },
            },
        }
        with self.fs.open(self.path, "w", encoding="utf-8") as f:
            yaml.safe_dump(
                catalog,
                f,
                sort_keys=False,  # preserve our ordering
                default_flow_style=False,
            )
