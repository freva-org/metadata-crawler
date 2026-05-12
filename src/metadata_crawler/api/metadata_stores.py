"""Metadata Storage definitions.

Backend-specific implementations live in :mod:`~.stores`.  This module
provides the catalogue writer / reader orchestration, the backend
registry (:class:`CatalogueBackends`), and the queue consumer that
bridges crawling with writing.
"""

from __future__ import annotations

import json
import multiprocessing as mp
import os
import re
import select
import sys
import time
from datetime import datetime
from enum import Enum, EnumType
from multiprocessing import sharedctypes
from pathlib import Path
from typing import (
    Any,
    Dict,
    List,
    Literal,
    Optional,
    Tuple,
    Type,
    TypeAlias,
    Union,
    cast,
)

import yaml

import metadata_crawler

from ..logger import logger
from ..utils import (
    Counter,
    MetadataCrawlerException,
    QueueLike,
)
from .config import DRSConfig, SchemaField
from .storage_backend import MetadataType
from .stores import MongoDB, PostgreSQL

# Re-export everything that was previously importable from this module
# so that ``from ..api.metadata_stores import IndexStore`` etc. keep working.
from .stores.base import (
    IndexName,
    IndexStore,
    StoreMetadata,
    WriterQueueType,
)
from .stores.jsonlines import JSONLines

ISO_FORMAT_REGEX = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z?$")

ConsumerQueueType: TypeAlias = QueueLike[Union[int, Tuple[str, str, MetadataType]]]

_GLOB_CHARS = re.compile(r"[*?\[\]{]")


# ------------------------------------------------------------------
# Backend registry
# ------------------------------------------------------------------


class _BackendMeta(EnumType):
    def __getitem__(cls, name: str) -> "CatalogueBackends":  # type: ignore[override]
        try:
            return super().__getitem__(name)
        except KeyError:
            available = ", ".join(cls.__members__)
            raise ValueError(
                f"Unknown backend {name!r}. Available backends: {available}"
            ) from None


class CatalogueBackends(Enum, metaclass=_BackendMeta):
    """Define the implemented catalogue backends."""

    intake = JSONLines
    mongodb = MongoDB
    postgresql = PostgreSQL
    jsonlines = JSONLines


CatalogueBackendType: TypeAlias = Literal["mongodb", "postgresql", "intake"]


# ------------------------------------------------------------------
# Queue consumer (runs in spawned processes)
# ------------------------------------------------------------------


class QueueConsumer:
    """Class that consumes the file discovery queue."""

    def __init__(
        self,
        config: Dict[str, Any],
        num_objects: "sharedctypes.Synchronized[Any]",
        writer_queue: WriterQueueType,
        silent: bool = False,
    ) -> None:
        self.config = DRSConfig(**config)
        self._writer_queue = writer_queue
        self.num_objects = num_objects
        self._silent = silent

    def _flush_batch(
        self,
        batch: List[Tuple[str, Dict[str, Any]]],
    ) -> None:
        logger.info("Ingesting %i items", len(batch))
        try:
            self._writer_queue.put(batch.copy())
            if self._silent is False:
                with self.num_objects.get_lock():
                    self.num_objects.value += len(batch)
        except Exception as error:  # pragma: no cover
            logger.error(error)  # pragma: no cover
        batch.clear()

    @classmethod
    def run_consumer_task(
        cls,
        queue: ConsumerQueueType,
        writer_queue: WriterQueueType,
        config: Dict[str, Any],
        num_objects: "sharedctypes.Synchronized[Any]",
        batch_size: int,
        poison_pill: int,
        silent: bool = False,
    ) -> None:
        """Set up a consumer task waiting for incoming data to be ingested."""
        this = cls(config, num_objects, writer_queue, silent=silent)
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
            except MetadataCrawlerException as error:
                logger.warning(error)
                continue
            except Exception as error:
                logger.error(error)
                continue
            append((name, metadata))
            if len(batch) >= batch_size:
                flush(batch)
        if batch:
            flush(batch)
        logger.info("Closing consumer %i", this_worker)


# ------------------------------------------------------------------
# Catalogue writer
# ------------------------------------------------------------------


class CatalogueWriter:
    """Create intake catalogues that store metadata entries.

    Parameters
    ^^^^^^^^^^
    catalogue_path:
        Path the to intake catalogue that should be created.
        Can be ``None`` for database backends that store their own
        catalogue metadata internally.
    index_name:
        Names of the metadata indexes.
    config:
        Metadata Config class
    data_store_prefix:
        Name or path of the metadata store.  For the *intake*
        backend this is a filesystem path prefix for the ``.json.gz``
        files (resolved relative to *yaml_path* unless absolute).
        For database backends it serves as the default collection or
        table name.  Defaults to ``"metadata"``.
    collection:
        Alias for *data_store_prefix* — preferred when using the
        *mongodb* backend.  Maps directly to the MongoDB collection
        name.
    table:
        Alias for *data_store_prefix* — preferred when using the
        *sqlalchemy* backend.  Maps directly to the SQL table name.
    batch_size:
        Size of the metadata chunks that should be added to the data store.
    n_procs:
        Number of processes collecting metadata
    storage_options:
        Set additional storage options for adding metadata to the metadata store
    shadow:
        'Shadow' this storage options. This is useful to hide secrets in public
        data catalogues.
    """

    def __init__(
        self,
        store_uri: str,
        index_name: IndexName,
        config: DRSConfig,
        *,
        data_store_prefix: Optional[str] = None,
        collection: Optional[str] = None,
        table: Optional[str] = None,
        batch_size: int = 25_000,
        backend: Optional[CatalogueBackendType] = None,
        n_procs: Optional[int] = None,
        storage_options: Optional[Dict[str, Any]] = None,
        shadow: Optional[Union[str, List[str]]] = None,
        **kwargs: Any,
    ) -> None:
        self.config = config
        storage_options = storage_options or {}
        _store_path = collection or table or data_store_prefix or "metadata"
        scheme, _, _ = _store_path.rpartition("://")
        self.silent = bool(int(os.getenv("MDC_SILENT", "0")))
        self.backend = backend or CatalogueReader.backend_from_store_url(store_uri)
        self.epoch = self._get_epoch()
        # YAML catalogue setup -- only for file-based backends.
        self.path: Optional[str] = store_uri
        self.fs = None
        if self.backend in ("intake",):
            self.fs, _ = IndexStore.get_fs(store_uri, **storage_options)
            self.path = self.fs.unstrip_protocol(store_uri)
            if not scheme and not os.path.isabs(_store_path):
                _store_path = os.path.join(
                    os.path.abspath(os.path.dirname(store_uri)),
                    _store_path,
                )
        else:
            _store_path = self.path

        self.prefix = _store_path
        self.index_name = index_name
        cls: Type[IndexStore] = CatalogueBackends[self.backend].value
        self.store = cls(
            _store_path,
            index_name,
            self.config.index_schema,
            mode="w",
            storage_options=storage_options,
            shadow=shadow,
            **kwargs,
        )
        self._ctx = mp.get_context("spawn")
        self.queue: ConsumerQueueType = self._ctx.Queue()
        self._poison_pill = 13
        self.num_objects: Counter = self._ctx.Value("i", 0)
        n_procs = n_procs or min(mp.cpu_count(), 15)
        batch_size_per_proc = max(int(batch_size / n_procs), 100)
        self._tasks = [
            self._ctx.Process(
                target=QueueConsumer.run_consumer_task,
                args=(
                    self.queue,
                    self.store.queue,
                    getattr(self.config, "_model_dict", {}),
                    self.num_objects,
                    batch_size_per_proc,
                    self._poison_pill,
                ),
                kwargs={"silent": self.silent},
            )
            for i in range(n_procs)
        ]

    @staticmethod
    def confirm_sweep(stale: int, total: int, timeout: int = 20) -> bool:
        """Prompt for sweep confirmation with a countdown.

        Returns ``True`` only on explicit 'y'.  Auto-aborts after
        *timeout* seconds or immediately in non-interactive sessions.
        """
        ratio = stale / total * 100
        msg = (
            f"\nWARNING: Sweep would remove {stale} of {total} "
            f"records ({ratio:.0f}%).\n"
        )
        if not sys.stdin.isatty():
            logger.critical("%s Non-interactive session — aborting sweep.", msg.strip())
            return False

        sys.stderr.write(msg)
        sys.stderr.write(f"Continue? [y/N] (auto-abort in {timeout}s): ")
        sys.stderr.flush()
        ready, _, _ = select.select([sys.stdin], [], [], timeout)
        if ready:
            answer = sys.stdin.readline().strip().lower()
            return answer in ("y", "yes")

        sys.stderr.write("\nTimeout — aborting sweep.\n")
        return False

    @staticmethod
    def _get_epoch() -> float:
        grace_env = os.getenv("MDC_GRACE_DAYS", "5")
        grace_days = int(grace_env) if grace_env.isdigit() else 5
        grace_sec = max(86400.0 * grace_days, 10)
        return time.time() - grace_sec

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
            the cataloguing system. For example apache solr would use a ``core``.
        """
        self.queue.put((name, drs_type, inp))

    @property
    def ingested_objects(self) -> int:
        """Get the number of ingested objects."""
        return self.num_objects.value

    @property
    def size(self) -> int:
        """Get the size of the worker queue."""
        return self.queue.qsize()

    def join_all_tasks(self) -> None:
        """Block the execution until all tasks are marked as done."""
        logger.debug("Releasing consumers from their duty.")
        for _ in self._tasks:
            self.queue.put(self._poison_pill)
        for task in self._tasks:
            task.join()
        self.store.join()

    async def close(self, create_catalogue: bool = True) -> None:
        """Close any connections."""
        self.store.join()
        total = self.store.total_objects
        stale = self.store.count_stale_objects(self.epoch)
        if total > 0 and stale / total > 0.75:
            if self.confirm_sweep(stale, total):
                self.store.sweep(self.epoch)
            else:
                raise SystemExit(
                    f"Sweep aborted: {stale}/{total} records would be removed. "
                    "Investigate and re-run."
                )
        else:
            self.store.sweep(self.epoch)
        self.store.close()
        if create_catalogue:
            if self.store.has_catalogue_storage:
                self.store.write_catalogue_metadata(
                    self.metadata.model_dump(by_alias=True)
                )
            elif self.fs:
                self._create_catalogue_file()

    async def delete(self) -> None:
        """Delete all stores."""
        await self.close(False)
        for name in self.index_name.latest, self.index_name.all:
            path = self.store.get_path(name)
            if hasattr(self.store, "_fs"):
                self.store._fs.rm(path) if self.store._fs.exists(path) else None
        if self.fs is not None and self.path:
            self.fs.rm(self.path) if self.fs.exists(self.path) else None

    def run_consumer(self) -> None:
        """Set up all the consumers."""
        for task in self._tasks:
            task.start()

    @property
    def metadata(self) -> StoreMetadata:
        """Define the metadata that will get added to the metadata store."""
        return StoreMetadata(
            version=1,
            backend=self.backend,
            prefix=self.prefix,
            storage_options=self.store.catalogue_storage_options(self.prefix),
            index_names={"latest": self.index_name.latest, "all": self.index_name.all},
            indexed_objects=self.ingested_objects,
            total_objects=self.store.total_objects or self.ingested_objects,
            timestamp=datetime.now().strftime("%c"),
            crawler={
                "name": metadata_crawler.__name__,
                "version": f"v{metadata_crawler.__version__}",
            },
            the_schema={
                k: json.loads(s.model_dump_json()) for k, s in self.store.schema.items()
            },
        )

    def _create_catalogue_file(self) -> None:
        if not self.fs:
            return
        timestamp = datetime.now().strftime("%c")
        catalog = {
            "description": (
                f"{metadata_crawler.__name__} "
                f"(v{metadata_crawler.__version__})"
                f" at {timestamp}"
            ),
            "metadata": self.metadata.model_dump(by_alias=True),
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


# ------------------------------------------------------------------
# Catalogue reader
# ------------------------------------------------------------------


class CatalogueReader:
    """Backend for reading the content of an intake catalogue.

    Parameters
    ^^^^^^^^^^
    storage_uri:
        Path to the intake catalogue for intake backends / connection url for
        db based backends.
    batch_size:
        Size of the metadata chunks that should be read.
    """

    def __init__(
        self,
        store_url: Union[str, Path],
        batch_size: int = 2500,
        storage_options: Optional[Dict[str, Any]] = None,
        backend: Optional[CatalogueBackendType] = None,
    ) -> None:
        backend = backend or self.backend_from_store_url(store_url)
        store_url = str(store_url)
        storage_options = storage_options or {}
        meta = self.read_catalogue_metadata(store_url, backend, **storage_options)
        store_cls: Type[IndexStore] = CatalogueBackends[backend].value
        if store_cls.has_catalogue_storage is False:
            store_cls = CatalogueBackends[meta["backend"]].value
            store_path = meta["prefix"]
            storage_options = meta.get("storage_options", {})
        else:
            store_path = store_url
        _schema_json = meta["schema"]
        schema = {s["key"]: SchemaField(**s) for k, s in _schema_json.items()}
        index_name = IndexName(**meta["index_names"])

        self.store = store_cls(
            store_path,
            index_name,
            schema,
            mode="r",
            batch_size=batch_size,
            storage_options=storage_options,
        )

    @classmethod
    def read_catalogue_metadata(
        cls,
        uri: Union[str, Path],
        backend: Optional[CatalogueBackendType] = None,
        **storage_options: Any,
    ) -> Dict[str, Any]:
        """Read the metadata from a metadata store."""
        backend = backend or cls.backend_from_store_url(uri)
        store_cls: Type[IndexStore] = CatalogueBackends[backend].value
        return store_cls.read_catalogue_metadata(str(uri), **storage_options)

    @staticmethod
    def backend_from_store_url(
        store_url: Union[Path, str],
    ) -> CatalogueBackendType:
        """Guess the storage backend from a given storage url.

        Parameters
        ^^^^^^^^^^
        store_url:
            Connection URI for the database backend, e.g.
            ``/home/user/foo.yml``
            ``s3:///store/user/foo.yml``
            ``mongodb://localhost:27017`` or
            ``postgresql://user:pw@host/dbname``.
        """
        suffix = Path(store_url).suffix.lower()
        if isinstance(store_url, Path) or suffix in (".yml", ".yaml"):
            return "intake"
        store_url = str(store_url)
        protocol = store_url.partition("://")[0] or os.getenv("MDC_BACKEND", "")
        if protocol.startswith("mongo"):
            return "mongodb"
        return "postgresql"

    @classmethod
    def rglob_stores(
        cls,
        path: Union[str, Path],
        backend: Optional[CatalogueBackendType] = None,
        **storage_options: Any,
    ) -> List[str]:
        """Recursively get a find target files."""
        backend = backend or cls.backend_from_store_url(path)
        path = str(path).rstrip("/")
        if backend not in ("intake", "jsonlines"):
            return [path]
        fs, is_local = IndexStore.get_fs(path, **storage_options)
        if _GLOB_CHARS.search(path):
            return [
                str(f) if is_local else fs.unstrip_protocol(str(f))
                for f in fs.glob(path)
            ]
        try:
            if fs.isdir(path):
                return [
                    str(f) if is_local else fs.unstrip_protocol(str(f))
                    for f in fs.find(path)
                    if f.endswith(".yml")
                ]
        except (FileNotFoundError, NotImplementedError):
            pass
        return [path]
