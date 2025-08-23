"""Metadata Storage definitions."""

import abc
import asyncio
import json
import multiprocessing as mp
import os
import re
import time
from datetime import datetime
from enum import Enum
from functools import cached_property
from io import TextIOWrapper
from pathlib import Path
from queue import Empty, Queue
from threading import Lock, Thread
from typing import (
    Any,
    AsyncIterator,
    Callable,
    ClassVar,
    Coroutine,
    Dict,
    List,
    Literal,
    NamedTuple,
    Optional,
    Tuple,
    Type,
    Union,
)
from urllib.parse import urlsplit

import duckdb
import fsspec
import pyarrow as pa
import yaml

import metadata_crawler

from ..logger import logger
from ..utils import create_async_iterator
from .config import DRSConfig, SchemaField
from .storage_backend import Metadata

ISO_FORMAT_REGEX = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z?$"
)

BATCH_SECS_THRESHOLD = 20


def _run_async(
    method: Callable[[Any], Coroutine[Any, Any, None]],
    *args: Any,
    **kwargs: Any,
) -> Any:
    """Run a async method in a sync. environment."""
    return asyncio.run(method(*args, **kwargs))


class DateTimeEncoder(json.JSONEncoder):
    """
    JSON‐Encoder that emits datetimes as ISO‐8601 strings.
    """

    def default(self, obj: Any) -> Any:
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


class DateTimeDecoder(json.JSONDecoder):
    """
    JSON Decoder that converts ISO‐8601 strings to datetime objects.
    """

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


class DuckDBTypeMap(str, Enum):
    """Mapping from logical BaseType values to SQLAlchemy types."""

    string = "VARCHAR"
    integer = "BIGINT"
    float = "DOUBLE"
    timestamp = "TIMESTAMP"

    @classmethod
    def get_type(cls, _type: str, multi_valued: bool = False) -> str:
        """Get the type duckdb."""
        if not multi_valued:
            return cls[_type].value
        return f"{cls[_type].value}[]"


class IndexName(NamedTuple):
    """A paired set of metadata indexes representing:

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
        lock: Optional[Lock] = None,
        batch_size: int = 2500,
        mode: Literal["r", "w"] = "r",
        storage_options: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        self._lock = lock or Lock()
        self.storage_options = storage_options or {}
        self._fs, self._is_local_path = self.get_fs(path, **self.storage_options)
        self._path = self._fs.unstrip_protocol(path)
        self.schema = schema
        self.batch_size = min(1000, batch_size)
        self.index_names: Tuple[str, str] = (index_name.latest, index_name.all)
        self.mode = mode
        self._rows_since_flush = 0
        self._last_flush = time.time()

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

        Yields:
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

    @abc.abstractmethod
    async def add(self, metadata_batch: List[Tuple[str, Dict[str, Any]]]) -> None:
        """Add a chunk metadata to the store."""
        ...  # pragma: no cover

    @abc.abstractmethod
    def close(self) -> None:
        """Close the data store."""
        ...  # pragma: no cover

    @abc.abstractmethod
    def get_args(self, index_name: str) -> Dict[str, Any]:
        """Define the intake arguments."""
        ...  # pragma: no cover


class DuckDB(IndexStore):
    """
    IndexStore implementation using native DuckDB.
    Can write to disk, memory, or remote S3 via httpfs.
    """

    driver = "parquet"
    suffix = ".parquet"

    def __init__(
        self,
        path: str,
        index_names: IndexName,
        schema: Dict[str, SchemaField],
        mode: Literal["r", "w"] = "r",
        lock: Optional[Lock] = None,
        duckdb_config: Optional[Dict[str, Any]] = None,
        storage_options: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ):
        """
        Parameters
        ^^^^^^^^^^
        path
            file:// path, local filepath, ':memory:', or 's3://bucket/path.duckdb'
        index_names
            tuple of table names to create
        schema
            list of SchemaField defining column names/types
        mode
            'w' = recreate tables, 'a' = append
        duckdb_config
            dict of PRAGMA settings (e.g. S3 credentials, region, endpoint)
        lock
            optional threading.Lock for thread safety
        """
        super().__init__(
            path, index_names, schema, lock=lock, storage_options=storage_options
        )
        self.mode = mode
        if mode == "w":
            self._create_tables()

    @property
    def duckdb_httpfs_settings(self) -> Dict[str, Any]:
        """
        Return DuckDB httpfs S3 settings derived from
        storage_options when path is s3://...
        Empty dict for local paths.
        """
        if self._is_local_path:
            # You can extend this for gs://, abfs://, etc. For now only s3.
            return {}

        so = self.storage_options
        key = so.get("key") or so.get("username")  # s3fs accepts "key"
        secret = so.get("secret") or so.get("password")
        token = so.get("token")  # optional session token

        endpoint_url = so.get("endpoint_url")
        region = so.get("region") or so.get("region_name") or "eu-dkrz-1"

        endpoint_hostport = None
        use_ssl = None

        use_ssl = True
        url_style = "auto"
        url_style = "auto"
        if endpoint_url:
            url_style = so.get("addressing_style") or "path"
            ep = urlsplit(endpoint_url)
            endpoint_hostport = ep.netloc or ep.path
            use_ssl = ep.scheme == "https"

        settings: Dict[str, Any] = {
            "s3_access_key_id": key or "",
            "s3_secret_access_key": secret or "",
            "s3_region": region,
            "s3_url_style": url_style,
            "s3_use_ssl": bool(use_ssl),
        }
        if endpoint_hostport:
            settings["s3_endpoint"] = endpoint_hostport
        if token:
            settings["s3_session_token"] = token
        return settings

    def prepare_duckdb_connection(self, con: duckdb.DuckDBPyConnection) -> None:
        """
        Install/load httpfs and apply settings if needed.
        """
        con.execute("INSTALL httpfs;")
        con.execute("LOAD httpfs;")
        for k, v in self.duckdb_httpfs_settings.items():
            if isinstance(v, bool):
                con.execute(f"SET {k} = {'true' if v else 'false'};")
            else:
                # Use parameter binding to be safe with strings
                con.execute(f"SET {k} = ?", [v])

    @cached_property
    def con(self) -> duckdb.DuckDBPyConnection:
        """Make a db connection."""
        con = duckdb.connect(read_only=False)
        self.prepare_duckdb_connection(con)
        return con

    def get_cursor(self) -> duckdb.DuckDBPyConnection:
        con = duckdb.connect(read_only=False)
        cursor = con.cursor()
        self.prepare_duckdb_connection(cursor)
        return cursor

    def _duck_type(self, fld: SchemaField) -> str:
        """Translate SchemaField to DuckDB column type."""
        base = fld.base_type.value
        return DuckDBTypeMap.get_type(
            base, multi_valued=fld.length is not None or fld.multi_valued
        )

    def _create_tables(self) -> None:
        for table in self.index_names:
            cols = []
            self.con.execute(f"DROP TABLE IF EXISTS {table};")
            for name, fld in self.schema.items():
                coldef = f"{name} {self._duck_type(fld)}"
                if fld.required:
                    coldef += " NOT NULL"
                cols.append(coldef)
            col_expr = ", ".join(cols)
            self.con.execute(
                f'CREATE TABLE IF NOT EXISTS "{table}"  ({col_expr});'
            )
            self.con.execute(
                (
                    f'CREATE TABLE IF NOT EXISTS "staging_{table}" '
                    f'AS SELECT * FROM "{table}" WHERE 0=1;'
                )
            )

    def _flush_locked(self) -> None:
        """Export only staging rows to Parquet, then clear staging."""
        any_export = False
        for table in self.index_names:
            res = self.con.execute(
                f'SELECT COUNT(*) FROM "staging_{table}"'
            ).fetchone()
            n = res[0] if res else 0
            if n == 0:
                continue
            any_export = True
            escaped_path = self.get_path(table).replace("'", "''")
            self.con.execute("BEGIN;")
            escaped_table = f"staging_{table}".replace('"', '""')
            sql = (
                f'COPY (SELECT * FROM "{escaped_table}") TO '
                f"'{escaped_path}' (FORMAT PARQUET, COMPRESSION ZSTD);"
            )
            logger.debug("Executing COPY:\n%s", sql)
            try:
                # Write only staging rows to new parquet files
                self.con.execute(sql)
                logger.debug("Truncating table staging_%s", table)
                self.con.execute(f'TRUNCATE TABLE "staging_{table}";')
                self.con.execute("COMMIT;")
            except Exception as error:
                self.con.execute("ROLLBACK;")
                logger.error(error)

        if any_export:
            self._rows_since_flush = 0
            self._last_flush = time.time()

    async def add(self, metadata_batch: List[Tuple[str, Dict[str, Any]]]) -> None:
        """Batch‐insert metadata dicts."""
        by_table: Dict[str, List[List[Any]]] = {t: [] for t in self.index_names}
        for table, meta in metadata_batch:
            row = [meta[name] for name in self.schema]
            by_table[table].append(row)

        with self._lock:

            for table, rows in by_table.items():
                if not rows:
                    continue
                placeholders = ", ".join("?" for _ in self.schema)
                sql = f'INSERT INTO "staging_{table}" VALUES ({placeholders});'

                await asyncio.to_thread(self.con.executemany, sql, rows)
                self._rows_since_flush += len(rows)
            if (
                self._rows_since_flush >= self.batch_size
                or (time.time() - self._last_flush) >= BATCH_SECS_THRESHOLD
            ):
                await asyncio.to_thread(self._flush_locked)

    def flush(self) -> None:
        """Public flush (threads safe)."""
        with self._lock:
            self._flush_locked()

    async def read(self, index_name: str) -> AsyncIterator[List[Dict[str, Any]]]:
        """
        Stream rows from DuckDB using a DBAPI cursor.
        Yields batches of dicts of size self.batch_size.
        """
        escaped_file = self.get_path(index_name).replace("'", "''")
        sql = f"SELECT * FROM read_parquet('{escaped_file}')"
        logger.debug("Reading from file: %s", sql)
        cur = await asyncio.to_thread(self.get_cursor)
        try:
            await asyncio.to_thread(cur.execute, sql)
            for table in await asyncio.to_thread(
                cur.fetch_record_batch, self.batch_size
            ):
                yield pa.Table.from_batches([table]).to_pylist()
        finally:
            await asyncio.to_thread(cur.close)

    def close(self) -> None:
        if self.mode == "w":
            self.flush()
        self.con.close()

    def get_args(self, index_name: str) -> Dict[str, Any]:
        """
        For intake or downstream tools:
        returns URI and a simple SQL expression.
        """
        return {
            "urlpath": self.get_path(index_name),
            "engine": "pyarrow",
            "storage_options": self.storage_options,
        }


class JSONLines(IndexStore):
    """Write metadata to gzipped JSONLines files."""

    suffix = ".json.gz"
    driver = "intake.source.jsonfiles.JSONLinesFileSource"

    def __init__(
        self,
        path: str,
        index_name: IndexName,
        schema: Dict[str, SchemaField],
        lock: Optional[Lock] = None,
        mode: Literal["w", "r"] = "r",
        storage_options: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ):
        self._lock = lock or Lock()
        self._streams: Dict[str, TextIOWrapper] = {}
        super().__init__(
            path,
            index_name,
            schema,
            lock=lock,
            mode=mode,
            storage_options=storage_options,
        )
        comp_level = int(kwargs.get("comp_level", "4"))
        for name in self.index_names:
            out_path = self.get_path(name)
            if mode == "w":
                parent = os.path.dirname(out_path).rstrip("/")
                try:
                    self._fs.makedirs(parent, exist_ok=True)
                except Exception:  # pragma: no cover
                    pass  # pragma: no cover
            self._streams[name] = self._fs.open(
                out_path,
                mode=f"{mode}t",
                compression="gzip",
                compression_level=comp_level,
                encoding="utf-8",
                newline="\n",
            )

    async def add(self, metadata_batch: List[Tuple[str, Dict[str, Any]]]) -> None:
        """Add a batch of metadata to the gzip store."""
        for index_name, metadata in metadata_batch:
            await asyncio.to_thread(
                self._streams[index_name].write,
                json.dumps(metadata, ensure_ascii=False, cls=DateTimeEncoder)
                + "\n",
            )

    def close(self) -> None:
        """Close the files."""
        for stream in self._streams.values():
            stream.flush()
            stream.close()

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

        Yields:
            List[Dict[str, Any]]:
                Deserialised metadata records.
        """
        chunk: List[Dict[str, Any]] = []

        async for line in create_async_iterator(self._streams[index_name]):
            chunk.append(json.loads(line, cls=DateTimeDecoder))
            if len(chunk) >= self.batch_size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk


class CatalogueBackends(Enum):
    """Define the implemented catalogue backends."""

    duckdb = DuckDB
    jsonlines = JSONLines


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


class CatalogueWriter:
    """Create intake catalogues that store metadata entries for versioned
    datasets (all versions and leatest versions).

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
        backend: str = "sqlite",
        batch_size: int = 2500,
        config: Optional[DRSConfig] = None,
        threads: Optional[int] = None,
        storage_options: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        self.config = config or DRSConfig.load()
        self._done = 0
        self._num_objects = 0
        self.lock = Lock()
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
            lock=self.lock,
            mode="w",
            storage_options=storage_options,
            **kwargs,
        )
        threads = threads or (min(mp.cpu_count(), 15))
        self._tasks = {
            i: Thread(
                target=_run_async,
                args=(
                    self._run_consumer_task,
                    batch_size,
                    i,
                ),
            )
            for i in range(threads)
        }
        self._queue: Queue[Tuple[str, str, Metadata]] = Queue()

    def write_batch(self, index_name: str, batch: List[Dict[str, Any]]) -> None:
        """Write a batch of metadata to the storage system."""

    async def put(
        self,
        inp: Metadata,
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
        info_obj:
            The name of the input object path where the metadata should be
            retrieved from catalogue_entry
        cls:
            An instance of the DRS class that fits to the input file object.
        name:
            Name of the catalogue, if applicable. This variable depends on
            the cataloguing system. For example apache solr would use a `core`.
        """
        self._queue.put((name, drs_type, inp))

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

    async def close(self) -> None:
        """Base method for closing any connections."""
        self.store.close()
        self._create_catalogue_file()

    def run_consumer(self) -> None:
        """Setup all the consumers."""
        for task in self._tasks.values():
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

    async def _run_consumer_task(self, batch_size: int, this_worker: int) -> None:
        """Setup a consumer task waiting for incoming data to be ingested."""
        logger.info("Adding %i consumer to consumers.", this_worker)
        batch: list[tuple[str, dict[str, str | list[str] | list[float]]]] = []
        nbatch = 0
        while self._done == 0:
            try:
                name, drs_type, inp = self._queue.get(timeout=1)
                metadata = self.config.read_metadata(drs_type, inp)
            except Empty:
                continue
            except Exception as error:
                logger.error(error)
                continue
            nbatch += 1
            batch.append((name, metadata))
            if nbatch >= batch_size:
                logger.info("Ingesting %i items", batch_size)
                try:
                    await self.store.add(batch)
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
                name, drs_type, inp = self._queue.get(timeout=1)
                metadata = self.config.read_metadata(drs_type, inp)
            except Empty:
                break
            except Exception as error:
                logger.error(error)
                continue
            batch.append((name, metadata))
            del metadata
        if batch:
            logger.info("Ingesting last %i items", len(batch))
            try:
                await self.store.add(batch)
                with self.lock:
                    self._num_objects += len(batch)
            except Exception as error:  # pragma: no cover
                logger.error(error)
                raise
        logger.info("Closing consumer %i", this_worker)
        del batch
