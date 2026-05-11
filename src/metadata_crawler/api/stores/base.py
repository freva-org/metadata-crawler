"""Base classes and shared types for metadata storage backends."""

from __future__ import annotations

import abc
import json
import multiprocessing as mp
import os
import time
from datetime import datetime
from types import NoneType
from typing import (
    Any,
    AsyncIterator,
    ClassVar,
    Dict,
    List,
    Literal,
    NamedTuple,
    Optional,
    Set,
    Tuple,
    TypeAlias,
    Union,
    cast,
)

import fsspec

from ...logger import logger
from ...utils import SimpleQueueLike
from ..config import SchemaField

BATCH_SECS_THRESHOLD = 20

MetadataRecord: TypeAlias = Dict[str, Any]
"""A single metadata record: key -> value of heterogeneous types."""

BATCH_ITEM: TypeAlias = List[Tuple[str, MetadataRecord]]
WriterQueueType: TypeAlias = SimpleQueueLike[Union[int, BATCH_ITEM]]
StorageOptions: TypeAlias = Dict[str, Any]


class Stream(NamedTuple):
    """A representation of a uri stream as named tuple."""

    name: str
    path: str


class DateTimeEncoder(json.JSONEncoder):
    """JSON-Encoder that emits datetimes as ISO-8601 strings."""

    def default(self, obj: object) -> str:
        """Set default time encoding."""
        if isinstance(obj, datetime):
            _date: str = obj.isoformat()
        else:
            _date = super().default(obj)
        return _date


class DateTimeDecoder(json.JSONDecoder):
    """JSON Decoder that converts ISO-8601 strings to datetime objects."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(object_hook=self._decode_objects, *args, **kwargs)

    def _decode_datetime(self, obj: object) -> object:
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

    def _decode_objects(self, obj: Dict[str, object]) -> Dict[str, object]:
        for key, value in obj.items():
            obj[key] = self._decode_datetime(value)
        return obj


class IndexName(NamedTuple):
    """A paired set of metadata indexes representations.

        - ``latest``: Metadata for the latest version of each dataset.
        - ``files``: Metadata for all available versions of datasets.

    This abstraction is backend-agnostic and can be used with any index system,
    such as Apache Solr cores, MongoDB collections, or SQL tables.

    """

    latest: str = "latest"
    all: str = "files"


class IndexStore:
    """Base class for all metadata stores.

    Subclasses must implement :py:meth:`read`, :py:meth:`get_args` and
    the :py:attr:`proc` property.  Filesystem-backed stores can rely on the
    default :py:meth:`_init_storage`; database-backed stores should override
    it to set up their own connection state.
    """

    suffix: ClassVar[str] = ""
    """Path suffix of the metadata store."""

    driver: ClassVar[str] = ""
    """Intake driver."""

    has_catalogue_storage: ClassVar[bool] = False
    """Whether this backend stores catalogue metadata internally."""

    _epoch_key: ClassVar[str] = "_crawl_epoch"
    """Key for keeping track of last crawls. Used by :py:meth:`sweep`."""

    def __init__(
        self,
        path: str,
        index_name: IndexName,
        schema: Dict[str, SchemaField],
        batch_size: int = 25_000,
        mode: Literal["r", "w"] = "r",
        storage_options: Optional[StorageOptions] = None,
        shadow: Optional[Union[str, List[str]]] = None,
        **kwargs: Any,
    ) -> None:

        self.storage_options: StorageOptions = storage_options or {}
        self._shadow_options: List[str] = (
            shadow or [] if isinstance(shadow, (list, NoneType)) else [shadow]
        )
        self._ctx: mp.context.SpawnContext = mp.get_context("spawn")
        self.queue: WriterQueueType = self._ctx.SimpleQueue()
        self._sent: int = 42
        self.schema: Dict[str, SchemaField] = schema
        self.batch_size: int = batch_size
        self.index_names: Tuple[str, str] = (index_name.latest, index_name.all)
        self.mode: Literal["r", "w"] = mode
        self._rows_since_flush: int = 0
        self._last_flush: float = time.time()
        self._paths: List[Stream] = []
        self.max_workers: int = max(1, (os.cpu_count() or 4))
        self._timestamp_keys: Set[str] = {
            k
            for k, col in schema.items()
            if getattr(getattr(col, "base_type", None), "value", None) == "timestamp"
        }
        self._init_storage(path, **kwargs)

    # ------------------------------------------------------------------
    # Storage initialisation -- override for non-filesystem backends
    # ------------------------------------------------------------------

    def _init_storage(self, path: str, **kwargs: Any) -> None:
        """Set up filesystem-based storage.

        Database-backed stores should override this method to establish
        their own connection state instead of calling into *fsspec*.
        """
        self._fs: fsspec.AbstractFileSystem
        self._is_local_path: bool
        self._fs, self._is_local_path = self.get_fs(path, **self.storage_options)
        self._path: str = self._fs.unstrip_protocol(path)
        for name in self.index_names:
            out_path = self.get_path(name)
            self._paths.append(Stream(name=name, path=out_path))

    # ------------------------------------------------------------------
    # Filesystem helpers (used by the default _init_storage path)
    # ------------------------------------------------------------------

    @staticmethod
    def get_fs(
        uri: str, **storage_options: Any
    ) -> Tuple[fsspec.AbstractFileSystem, bool]:
        """Get the base-url from a path."""
        protocol, _ = fsspec.core.split_protocol(uri)
        protocol = protocol or "file"
        if protocol == "s3" and "key" not in storage_options:
            storage_options.setdefault("anon", True)
        fs = fsspec.filesystem(protocol, **storage_options)
        return fs, protocol == "file"

    def get_path(self, path_suffix: Optional[str] = None) -> str:
        """Construct a path name for a given suffix."""
        path = self._path.removesuffix(self.suffix)
        new_path = (
            f"{path}-{path_suffix}{self.suffix}"
            if path_suffix
            else f"{path}{self.suffix}"
        )
        return new_path

    # ------------------------------------------------------------------
    # Housekeeping functions
    # -------------------------------------------------------------------

    def sweep(self, epoch: float) -> None:
        """Delete all records whose epoch differs from *epoch*."""

    # ------------------------------------------------------------------
    # Abstract interface
    # ------------------------------------------------------------------

    @abc.abstractmethod
    async def read(
        self,
        index_name: str,
    ) -> AsyncIterator[List[MetadataRecord]]:
        """Yield batches of metadata records from a specific table.

        Parameters
        ^^^^^^^^^^
        index_name:
            The name of the index_name.

        Yields
        ^^^^^^
        List[MetadataRecord]:
            Deserialised metadata records.
        """
        yield [{}]  # pragma: no cover

    @abc.abstractmethod
    def get_args(self, index_name: str) -> Dict[str, Any]:
        """Define the intake arguments."""
        ...  # pragma: no cover

    @property
    def proc(self) -> Optional[mp.process.BaseProcess]:
        """The writer process."""
        raise NotImplementedError("This property must be defined.")  # pragma: no cover

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def join(self) -> None:
        """Shutdown the writer task."""
        self.queue.put(self._sent)
        if self.proc is not None:
            self.proc.join()

    def close(self) -> None:
        """Shutdown the write worker."""
        self.join()

    # ------------------------------------------------------------------
    # Catalogue helpers
    # ------------------------------------------------------------------

    def catalogue_storage_options(self, path: Optional[str] = None) -> StorageOptions:
        """Construct the storage options for the catalogue."""
        is_s3 = (path or "").startswith("s3://")
        opts: StorageOptions = {
            k: v
            for k, v in self.storage_options.items()
            if k not in self._shadow_options
        }
        shadow_keys = {
            "key",
            "secret",
            "token",
            "username",
            "user",
            "password",
            "secret_file",
            "secretfile",
        }
        opts |= {"anon": True} if is_s3 and not shadow_keys & opts.keys() else {}
        return opts

    def write_catalogue_metadata(self, indexed_objects: int = 0) -> None:
        """Persist catalogue metadata inside the backend itself."""
        raise NotImplementedError(
            f"{type(self).__name__} does not support internal "
            "catalogue metadata storage."
        )

    @classmethod
    def read_catalogue_metadata(cls, url: str, **kwargs: Any) -> MetadataRecord:
        """Read catalogue metadata from the backend."""
        raise NotImplementedError(
            f"{cls.__name__} does not support internal catalogue metadata storage."
        )


class BackendWriter:
    """Base class for inserting metadata via a multi-proc. queue."""

    backend: ClassVar[str]
    _epoch_key: ClassVar[str] = IndexStore._epoch_key

    @abc.abstractmethod
    def __init__(
        self,
        *streams: Stream,
        **storage_options: Any,
    ) -> None:
        """Each store writer must implement a __init__ method."""

    @classmethod
    def as_daemon(
        cls,
        queue: WriterQueueType,
        semaphore: int,
        *schemes: Stream,
        storage_options: Optional[StorageOptions] = None,
    ) -> None:
        """Start the writer process as a daemon."""
        try:
            this = cls(*schemes, **(storage_options or {}))
        except Exception as error:
            logger.critical("Writer daemon failed to start: %s", error)
            raise SystemExit(1)
        get = queue.get
        add = this.add
        while True:
            item = get()
            if item == semaphore:
                logger.info("Closing %s writer task.", cls.backend)
                break
            try:
                add(cast(BATCH_ITEM, item))
            except Exception as error:
                logger.error(error)
        this.close()

    @abc.abstractmethod
    def add(self, metadata_batch: List[Tuple[str, MetadataRecord]]) -> None:
        """Add a batch of metadata to the metadata store."""

    @abc.abstractmethod
    def close(self) -> None:
        """Close the writer."""
