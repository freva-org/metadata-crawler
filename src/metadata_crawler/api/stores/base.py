"""Base classes and shared types for metadata storage backends."""

from __future__ import annotations

import abc
import json
import multiprocessing as mp
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from types import NoneType
from typing import (
    Annotated,
    Any,
    AsyncIterator,
    ClassVar,
    Dict,
    FrozenSet,
    List,
    Literal,
    NamedTuple,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    TypeAlias,
    TypeVar,
    Union,
    cast,
)
from urllib.parse import unquote, urlsplit, urlunsplit

import fsspec
import pydantic
from typing_extensions import TypedDict

from ...logger import logger
from ...utils import Counter, SimpleQueueLike
from ..config import BaseType, SchemaField

CREDENTIAL_KEYS = frozenset(
    {
        "key",
        "secret",
        "token",
        "username",
        "user",
        "password",
        "passwd",
        "secret_file",
        "secretfile",
        "os_password",
        "os_auth_token",
    }
)


PLUGIN_GROUP = "metadata_crawler.stores"
_plugins_loaded = False

BATCH_SECS_THRESHOLD = 20
_GLOB_CHARS = frozenset("*?")

ConnT = TypeVar("ConnT", bound="BaseConnection")
LOCATION_KEYS = ("url", "path", "uri")


MetadataRecord: TypeAlias = Dict[str, Any]
"""A single metadata record: key -> value of heterogeneous types."""

FacetValue: TypeAlias = Union[str, int, float]
Facet: TypeAlias = Tuple[str, FacetValue]

UNFILTERABLE_TYPES = frozenset({"bbox", "daterange", "timestamp"})

BATCH_ITEM: TypeAlias = List[Tuple[str, MetadataRecord]]
WriterQueueType: TypeAlias = SimpleQueueLike[Union[int, BATCH_ITEM]]
StorageOptions: TypeAlias = Dict[str, Any]
CatalogueBackendType: TypeAlias = Literal["mongodb", "postgresql", "intake"]


def glob_to_regex(glob: str) -> str:
    """Anchored regex for a glob; only ``*`` and ``?`` are wildcards."""
    esc = re.escape(glob).replace(r"\*", ".*").replace(r"\?", ".")
    return f"^{esc}$"


def glob_to_like(glob: str) -> str:
    r"""Construct SQL ``LIKE`` pattern, escaping ``%``, ``_`` and ``\\``."""
    out = []
    for char in glob:
        if char == "*":
            out.append("%")
        elif char == "?":
            out.append("_")
        elif char in "%_\\":
            out.append("\\" + char)
        else:
            out.append(char)
    return "".join(out)


class ItemsDict(TypedDict):
    """Representation of the indexed items."""

    latest: str
    all: str


class CrawlerOptions(TypedDict):
    """Representation of the metadata-crawler software information."""

    name: str
    version: str


class StoreMetadata(pydantic.BaseModel):
    """Description of the metadata store."""

    version: int
    backend: str
    prefix: str
    storage_options: pydantic.JsonValue
    index_names: ItemsDict
    indexed_objects: int
    total_objects: int
    timestamp: str
    crawler: CrawlerOptions
    the_schema: Annotated[
        pydantic.JsonValue, pydantic.Field(serialization_alias="schema")
    ]


class S3Options(pydantic.BaseModel):
    """Options for s3fs. Unknown keys are passed on to s3fs unchanged."""

    model_config = pydantic.ConfigDict(extra="allow", frozen=True)

    endpoint_url: Optional[str] = None
    anon: Optional[bool] = None
    key: Optional[pydantic.SecretStr] = None
    secret: Optional[pydantic.SecretStr] = None
    token: Optional[pydantic.SecretStr] = None

    @pydantic.model_validator(mode="after")
    def _key_and_secret_together(self) -> "S3Options":
        if (self.key is None) != (self.secret is None):
            raise ValueError("S3 'key' and 'secret' must be given together")
        return self

    def options(self, *, reveal: bool = False) -> StorageOptions:
        """Options for s3fs; secrets stay masked unless *reveal*."""
        data = {**dict(self), **(self.model_extra or {})}
        return {
            key: BaseConnection._reveal(value) if reveal else value
            for key, value in data.items()
            if value is not None
        }


class Credentials(pydantic.BaseModel):
    """Username and password, with the aliases the backends accept."""

    model_config = pydantic.ConfigDict(populate_by_name=True)

    username: Optional[str] = pydantic.Field(default=None, alias="user")
    password: Optional[pydantic.SecretStr] = pydantic.Field(
        default=None, alias="passwd"
    )


class BaseConnection(pydantic.BaseModel, abc.ABC):
    """How to reach a store. Each store backend subclasses this."""

    model_config = pydantic.ConfigDict(
        extra="forbid", frozen=True, populate_by_name=True
    )

    backend: ClassVar[str]
    """Name of the backend, as used in configs and catalogue metadata."""

    schemes: ClassVar[FrozenSet[str]] = frozenset()
    """URL schemes this backend claims, used when no backend is given."""

    fallback: ClassVar[bool] = False
    """Use this backend for URLs whose scheme no backend claims."""

    _registry: ClassVar[Dict[str, Type["BaseConnection"]]] = {}

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        backend = cls.__dict__.get("backend")
        if backend is None:  # an intermediate base class, not a backend
            return
        existing = BaseConnection._registry.get(backend)
        if existing is not None and existing.__qualname__ != cls.__qualname__:
            raise TypeError(
                f"backend {backend!r} is already registered by {existing.__qualname__}"
            )
        BaseConnection._registry[backend] = cls

    @classmethod
    def registered(cls) -> Dict[str, Type["BaseConnection"]]:
        """All known connection models, including those of store plugins."""
        cls._load_plugins()
        return dict(BaseConnection._registry)

    @classmethod
    def for_backend(cls, backend: str) -> Type["BaseConnection"]:
        """Get the backend class that fits to a name."""
        models = cls.registered()
        try:
            return models[backend]
        except KeyError:
            known = ", ".join(sorted(models)) or "none"
            raise ValueError(f"unknown backend {backend!r} (known: {known})") from None

    @classmethod
    def for_url(cls, url: str) -> Type["BaseConnection"]:
        """Get the model claiming the URL's scheme, or the fallback backend."""
        import fsspec

        scheme, path = fsspec.core.split_protocol(url)
        scheme = scheme or "file"
        models = cls.registered().values()
        claiming = [m for m in models if scheme in m.schemes]
        if len(claiming) > 1:
            names = ", ".join(sorted(m.backend for m in claiming))
            raise ValueError(
                f"scheme {scheme!r} is claimed by several backends ({names}); "
                "set 'backend' explicitly"
            )
        if claiming:
            return claiming[0]
        fallbacks = [m for m in models if m.fallback]
        if len(fallbacks) != 1:
            raise ValueError(f"no backend handles {url!r}; set 'backend' explicitly")
        return fallbacks[0]

    name: str
    url: str = pydantic.Field(alias="uri")
    description: Optional[str] = None
    secrets: Optional[str] = pydantic.Field(
        default=None,
        description="Table in secrets.toml holding the credentials, "
        "if not the one with the store's own name.",
    )

    @property
    @abc.abstractmethod
    def store_uri(self) -> str:
        """Construrct the uri to the store of truth."""

    @classmethod
    def from_url(
        cls: Type[ConnT],
        url: Optional[str] = None,
        **options: Any,
    ) -> ConnT:
        """Define an ad-hoc connection for a URL that isn't configured."""
        return cls.model_validate({"name": url, "url": url, **options})

    def storage_options(self, *, reveal: bool = False) -> StorageOptions:
        """Backend options without the descriptive keys."""
        return {}

    def catalogue_options(self) -> StorageOptions:
        """Options that are safe to write into catalogue metadata."""
        return {
            key: value
            for key, value in self.storage_options().items()
            if not isinstance(value, pydantic.SecretStr)
        }

    @classmethod
    def _load_plugins(cls) -> None:
        """Import store plugins once; importing them registers their models."""
        global _plugins_loaded
        if _plugins_loaded:
            return
        _plugins_loaded = True
        from importlib.metadata import entry_points

        for entry_point in entry_points(group=PLUGIN_GROUP):
            entry_point.load()

    @staticmethod
    def _reveal(value: Any) -> Any:
        return (
            value.get_secret_value() if isinstance(value, pydantic.SecretStr) else value
        )

    @staticmethod
    def _url_defaults(data: Dict[str, Any], default_port: Optional[int]) -> None:
        """Take host, port, database and credentials from the URL if not given."""
        url = data.get("url") or data.get("uri") or data.get("path", "")
        parts = urlsplit(str(url))
        data.setdefault("host", parts.hostname or "localhost")
        if parts.port or default_port:
            data.setdefault("port", parts.port or default_port)
        path = parts.path.strip("/")
        if path:
            data.setdefault("database", unquote(path))
        if parts.username and not ({"username", "user"} & data.keys()):
            data["username"] = unquote(parts.username)
        if parts.password and not ({"password", "passwd"} & data.keys()):
            data["password"] = unquote(parts.password)
        if parts.username or parts.password:
            # The credentials now live in the (masked) fields, keep them out
            # of the url and of a name that was derived from it.
            netloc = parts.netloc.rpartition("@")[-1]
            clean = urlunsplit(parts._replace(netloc=netloc))
            for key in ("url", "uri", "path", "name"):
                if data.get(key) == url:
                    data[key] = clean


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


class IndexStore(abc.ABC):
    """Base class for all metadata stores.

    Subclasses must implement :py:meth:`_read` and the :py:attr:`proc` property.
    Filesystem-backed stores can rely on the default :py:meth:`_init_storage`;
    database-backed stores should override it to set up their own connection
    state.
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
        mode: Literal["r", "w", "a"] = "r",
        storage_options: Optional[StorageOptions] = None,
        shadow: Optional[Union[str, List[str]]] = None,
        **kwargs: Any,
    ) -> None:
        self.storage_options: StorageOptions = storage_options or {}
        self._shadow_options: List[str] = (
            shadow or [] if isinstance(shadow, (list, NoneType)) else [shadow]
        )
        self._ctx: mp.context.SpawnContext = mp.get_context("spawn")
        _writer_qsize = int(os.getenv("MDC_WRITER_QUEUE_SIZE", "0")) or 64
        self.queue: WriterQueueType = self._ctx.Queue(maxsize=_writer_qsize)
        self._sent: int = 42
        self.schema: Dict[str, SchemaField] = schema
        self.batch_size: int = batch_size
        self.index_names: Tuple[str, str] = (index_name.latest, index_name.all)
        self.mode: Literal["r", "w", "a"] = mode
        self._rows_since_flush: int = 0
        self._last_flush: float = time.time()
        self._paths: List[Stream] = []
        self.max_workers: int = max(1, (os.cpu_count() or 4))
        self._timestamp_keys: Set[str] = {
            k
            for k, col in schema.items()
            if getattr(getattr(col, "base_type", None), "value", None) == "timestamp"
        }
        self.counter: Counter = self._ctx.Value("i", 0)
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

    @property
    @abc.abstractmethod
    def total_objects(self) -> int:
        """Get the number of total objects in this store."""
        raise NotImplementedError("This must be defined on backend level.")

    def count_stale_objects(self, epoch: float) -> int:
        """Count the number of stale (outdated objects)."""
        return 0

    def sweep(self, epoch: float) -> None:
        """Delete all records whose epoch differs from *epoch*."""

    def sanitize_facets(self, facets: Sequence[Facet]) -> Dict[str, List[FacetValue]]:
        """Sanitize the facets."""
        grouped: Dict[str, List[FacetValue]] = {}

        for key, values in facets:
            key = key.lower()
            v = values.lower() if isinstance(values, str) else values
            field = self.schema.get(key)
            if field is None:
                logger.warning("Facet %s not in in metadata schema", key)
            elif (
                field.type in UNFILTERABLE_TYPES
                or field.base_type == BaseType.timestamp
            ):
                logger.warning("Ignoring invalid facet %s of type %s", key, field.type)
            else:
                grouped.setdefault(key, []).append(v)
        return grouped

    def split_facet_values(
        self, key: str, values: List[FacetValue]
    ) -> Tuple[List[FacetValue], List[str]]:
        """Split facet values into typed exact values and glob patterns."""
        field = self.schema[key]
        exact: List[FacetValue] = []
        globs: List[str] = []
        for value in values:
            if field.base_type == BaseType.string:
                text = str(value)
                (globs if _GLOB_CHARS & set(text) else exact).append(text)
                continue
            try:
                exact.append(
                    int(value) if field.base_type == BaseType.integer else float(value)
                )
            except (TypeError, ValueError):
                logger.warning("Ignoring invalid value %r for facet %s", value, key)
        return exact, globs

    async def remove(self, *facets: Facet, dry_run: bool = False) -> int:
        """Delete items from a meta data store.

        Parameters
        ^^^^^^^^^^
        index_name:
            The name of the index_name.
        facets:
            The search facets that need to match for the removal.
        dry_run:
            Do not delete data from the store.

        Returns
        ^^^^^^^
        int:
            Number of removed object for this index.
        """
        if self.mode != "r":
            raise RuntimeError("Cannot remove entries from a store opened for writing.")
        groups = self.sanitize_facets(facets)
        if not facets or not groups:
            logger.warning("No facets given, nothing to remove.")
            return 0

        return await self._remove(groups, dry_run)

    async def read(
        self,
        index_name: str,
        *facets: Facet,
        parse_timestamps: bool = True,
    ) -> AsyncIterator[List[MetadataRecord]]:
        """Yield batches of metadata records from a specific table.

        Parameters
        ^^^^^^^^^^
        index_name:
            The name of the index_name.
        parse_timestamps:
            Parse timestamps to datetimes

        Yields
        ^^^^^^
        List[MetadataRecord]:
            Deserialised metadata records.
        """
        groups = self.sanitize_facets(facets)
        if facets and not groups:
            raise ValueError("None of the given facets is valid.")
        async for record in self._read(
            index_name,
            parse_timestamps=parse_timestamps,
        ):
            yield record

    def get_args(self, index_name: str) -> Dict[str, Any]:
        """Define the intake arguments."""
        return {}

    # ------------------------------------------------------------------
    # Abstract interface
    # ------------------------------------------------------------------

    @abc.abstractmethod
    async def _read(
        self,
        index_name: str,
        *,
        parse_timestamps: bool = True,
    ) -> AsyncIterator[List[MetadataRecord]]:
        """Yield batches of metadata records from a specific table.

        Parameters
        ^^^^^^^^^^
        index_name:
            The name of the index_name.
        parse_timestamps:
            Parse timestamps to datetimes

        Yields
        ^^^^^^
        List[MetadataRecord]:
            Deserialised metadata records.
        """
        yield [{}]  # pragma: no cover

    @abc.abstractmethod
    async def _remove(self, grouped: Dict[str, List[FacetValue]], dry_run: bool) -> int:
        """Apply Backend-specific removal of entries matching *grouped* facets."""
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
        if self.has_catalogue_storage:
            # A DB store deons't need storage options to be encoded.
            return {}
        is_s3 = (path or "").startswith("s3://")
        hidden = CREDENTIAL_KEYS | set(self._shadow_options)
        opts: StorageOptions = {
            k: v for k, v in self.storage_options.items() if k not in hidden
        }
        if is_s3 and not CREDENTIAL_KEYS & self.storage_options.keys():
            opts["anon"] = True
        return opts

    def write_catalogue_metadata(self, payload: Dict[str, Any]) -> None:
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

    @staticmethod
    def normalise_uris(
        uri: Optional[Union[str, Path, Sequence[Union[str, Path]]]],
    ) -> List[str]:
        """Coerce the ``uri`` argument into a list of non-empty store uris."""
        if uri is None:
            return []
        if isinstance(uri, (str, Path)):
            uri = [uri]
        return [str(_uri) for _uri in uri if _uri is not None and str(_uri)]


class BackendWriter:
    """Base class for inserting metadata via a multi-proc. queue."""

    backend: ClassVar[str]
    _epoch_key: ClassVar[str] = IndexStore._epoch_key
    has_catalogue_storage: ClassVar[bool] = False

    def __init__(
        self,
        counter: Counter,
        *streams: Stream,
        **storage_options: Any,
    ) -> None:
        """Each store writer must implement a __init__ method."""
        self.indexed_objects = 0
        self._counter = counter
        self.streams = streams
        self.storage_options = storage_options
        self._write_pool = ThreadPoolExecutor(max_workers=len(self.streams) or 1)
        self.__post_init__()

    def set_total_items_written(self) -> int:
        """Get the number of total written items."""
        with self._counter.get_lock():
            self._counter.value = self.indexed_objects
        return self.indexed_objects

    @abc.abstractmethod
    def __post_init__(self) -> None:
        """Set up the storage backend for writing."""
        raise NotImplementedError("__post_init__ must be implemented.")

    @classmethod
    def as_daemon(
        cls,
        queue: WriterQueueType,
        semaphore: int,
        counter: Counter,
        *schemes: Stream,
        storage_options: Optional[StorageOptions] = None,
    ) -> None:
        """Start the writer process as a daemon."""
        try:
            this = cls(counter, *schemes, **(storage_options or {}))
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
    def _write_table(self, table_name: str, rows: List[MetadataRecord]) -> int:
        """Per backend implementation of the actual write/push."""
        raise NotImplementedError("Backends must implement their writers.")

    def add(self, metadata_batch: List[Tuple[str, MetadataRecord]]) -> None:
        """Add a batch of metadata to the metadata store."""
        by_table: Dict[str, List[MetadataRecord]] = {s.name: [] for s in self.streams}
        now = time.time()
        for table_name, metadata in metadata_batch:
            if table_name in by_table and self.has_catalogue_storage:
                metadata[self._epoch_key] = now
            by_table[table_name].append(metadata)

        futures = [
            self._write_pool.submit(self._write_table, table_name, rows)
            for table_name, rows in by_table.items()
            if rows
        ]
        self.indexed_objects += sum([f.result() for f in futures])

    @abc.abstractmethod
    def _close(self) -> None:
        """Close the writer."""

    def close(self) -> None:
        """Close the writer."""
        self.set_total_items_written()
        self._close()
