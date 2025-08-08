"""Metadata Storage definitions."""

import abc
import asyncio
import gzip
import json
import multiprocessing as mp
import os
import re
from datetime import datetime
from enum import Enum
from functools import cached_property
from io import TextIOWrapper
from pathlib import Path
from queue import Empty, Queue
from threading import Lock, Thread
from typing import (
    Any,
    Awaitable,
    Callable,
    ClassVar,
    Coroutine,
    Dict,
    Iterator,
    List,
    Literal,
    NamedTuple,
    Optional,
    Tuple,
    Union,
)

import duckdb
import yaml

import metadata_crawler

from ..backends.base import Metadata
from ..logger import logger
from .config import DRSConfig, SchemaField

ISO_FORMAT_REGEX = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z?$"
)


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

    def __init__(self, *args, **kwargs) -> None:
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


def json_hook(d: Dict[str, Any]) -> Dict[str, Any]:
    """
    JSON object‐hook that attempts to parse any ISO‐8601 string
    values back into datetime.datetime.
    """
    for k, v in d.items():
        if isinstance(v, str):
            try:
                d[k] = datetime.fromisoformat(v)
            except ValueError:
                pass
    return d


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
        **kwargs,
    ) -> None:
        self._lock = lock or Lock()
        self._path = path
        self.schema = schema
        self.batch_size = batch_size
        self.index_names = (index_name.latest, index_name.all)
        self.mode = mode

    async def serialise_or_deserialise(
        self, obj: Dict[str, Any], action: Literal["serialise", "deserialise"]
    ) -> Dict[str, Any]:
        """Searialise or deserialise collections of items."""
        funcs = {
            "timestamp": {
                "serialise": datetime.isoformat,
                "deserialise": datetime.fromisoformat,
            }
        }
        for key, value in obj.items():
            if self.schema[key].length or self.schema[key].multi_valued:
                typ = self.schema[key].base_type
                if typ in funcs:
                    obj[key] = list(map(funcs[typ][action], value))
        return obj

    def get_path(self, path_suffix: Optional[str] = None) -> str:
        """Construct a path name for a given suffix."""
        path = self._path.removesuffix(self.suffix)
        if path_suffix:
            return f"{path}-{path_suffix}{self.suffix}"
        return f"{path}{self.suffix}"

    @abc.abstractmethod
    async def add(self, metadata_batch: List[Tuple[str, Dict[str, Any]]]) -> None:
        """Add a chunk metadata to the store."""
        ...

    @abc.abstractmethod
    async def read(
        self,
        index_name: str,
    ) -> Iterator[List[Dict[str, Any]]]:
        """Yield batches of metadata records from a specific table.

        Parameters
        ----------
        index_name:
            The name of the index.

        Yields:
            List[Dict[str, Any]]:
                Deserialised metadata records.
        """
        ...

    @abc.abstractmethod
    def close(self) -> None:
        """Close the data store."""
        ...

    @abc.abstractmethod
    def get_args(self, index_name: str) -> Dict[str, Any]:
        """Define the intake arguments."""
        ...


class DuckDB(IndexStore):
    """
    IndexStore implementation using native DuckDB.
    Can write to disk, memory, or remote S3 via httpfs.
    """

    driver = "duckdb"
    suffix = ".duckdb"

    def __init__(
        self,
        path: str,
        index_names: Tuple[str, str],
        schema: Dict[str, SchemaField],
        mode: str = "r",
        lock: Optional[Lock] = None,
        duckdb_config: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ):
        """
        Parameters
        ----------
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
        super().__init__(path, index_names, schema, lock=lock)
        self.mode = mode
        self.duckdb_config = duckdb_config or {}
        self._path = self._path + self.suffix
        if mode == "w":
            self._create_tables()

    @cached_property
    def con(self) -> duckdb.DuckDBPyConnection:
        """Make a db connection."""
        # open DB (file, memory, or S3 path)
        con = duckdb.connect(self._path, read_only=False)
        # load httpfs for S3 if needed
        con.execute("INSTALL httpfs;")
        con.execute("LOAD httpfs;")
        # apply any PRAGMA configs
        for key, val in self.duckdb_config.items():
            con.execute(f"PRAGMA {key} = '{val}';")
        return con

    def _duck_type(self, fld: SchemaField) -> str:
        """Translate SchemaField → DuckDB column type."""
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
            ddl = f"CREATE TABLE IF NOT EXISTS {table} (\n  "
            ddl += ",\n  ".join(cols) + "\n);"
            logger.debug("Executing duckdb statement:\n%s", ddl)
            self.con.execute(ddl)

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
                # build parametrized INSERT
                placeholders = ", ".join("?" for _ in self.schema)
                sql = f"INSERT INTO {table} VALUES ({placeholders});"
                # use DuckDB’s executemany
                self.con.executemany(sql, rows)

    async def read(self, index_name: str) -> Iterator[List[Dict[str, Any]]]:
        """
        Stream rows from DuckDB using a DBAPI cursor.
        Yields batches of dicts of size self.batch_size.
        """
        cur = self.con.cursor()
        cur.execute(f"SELECT * FROM {index_name};")
        cols = [col[0] for col in cur.description]
        while True:
            rows = cur.fetchmany(self.batch_size)
            if not rows:
                break
            batch = []
            for r in rows:
                rec = {col: val for col, val in zip(cols, r)}
                batch.append(rec)
            yield batch

    def close(self) -> None:
        self.con.close()

    def get_args(self, index_name: str) -> Dict[str, Any]:
        """
        For intake or downstream tools:
        returns URI and a simple SQL expression.
        """
        return {
            "uri": self._path,
            "sql_expr": f"SELECT * FROM {index_name}",
        }


class JSONLines(IndexStore):
    """Write metadata to gzipped JSONLines files."""

    suffix = ".json.gz"
    driver = "intake.source.jsonfiles.JSONLinesFileSource"

    def __init__(
        self,
        path: str,
        index_name: IndexName,
        schema: List[SchemaField],
        lock: Optional[Lock] = None,
        mode: Literal["w", "r"] = "r",
        **kwargs: Any,
    ):
        self._lock = lock or Lock()
        self._streams: Dict[str, TextIOWrapper] = {}
        super().__init__(path, index_name, schema, lock, mode=mode)
        comp_level = int(kwargs.get("comp_level", "4"))
        for index_name in self.index_names:
            out_path = self.get_path(index_name)
            self._streams[index_name] = gzip.open(
                out_path, mode=f"{mode}t", compresslevel=comp_level
            )

    async def add(self, metadata_batch: List[Tuple[str, Dict[str, Any]]]) -> None:
        """Add a batch of metadata to the gzip store."""
        for index_name, metadata in metadata_batch:
            self._streams[index_name].write(
                json.dumps(metadata, ensure_ascii=False, cls=DateTimeEncoder)
                + "\n"
            )

    def close(self):
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
        }

    async def read(
        self,
        index_name: str,
    ) -> Iterator[List[Dict[str, Any]]]:
        """Yield batches of metadata records from a specific table.

        Parameters
        ----------
        index_name:
            The name of the index_name.

        Yields:
            List[Dict[str, Any]]:
                Deserialised metadata records.
        """
        chunk: List[Dict[str, Any]] = []
        for line in self._streams[index_name]:
            chunk.append(json.loads(line, cls=DateTimeDecoder))
            if len(chunk) >= self.batch_size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk


class CatalogueBackends(Enum):
    """Define the implmented catalogu backends."""

    duckdb = DuckDB
    jsonlines = JSONLines


class CatalogueReader:
    """Backend for reading the content of an intake catalogue.

    Parameters
    ----------

    catalogue_file:
        Path to the intake catalogue
    batch_size:
        Size of the metadata chunks that should be read.

    """

    def __init__(
        self, catalogue_file: Union[str, Path], batch_size: int = 2500
    ) -> None:

        cat = yaml.safe_load(Path(catalogue_file).read_text())
        _schema_json = cat["metadata"]["schema"]
        schema = {s["key"]: SchemaField(**s) for k, s in _schema_json.items()}
        index_name = IndexName(**cat["metadata"]["index_names"])
        cls = CatalogueBackends[cat["metadata"]["backend"]].value
        self.store = cls(
            cat["metadata"]["prefix"],
            index_name,
            schema,
            mode="r",
            batch_size=batch_size,
        )


class CatalogueWriter:
    """Create intake catalogues that store metadata entries for versioned
    datasets (all versions and leatest versions).

    Parameters
    ----------
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
        **kwargs: str,
    ) -> None:
        self.config = config or DRSConfig.load()
        self._done = 0
        self._num_objects = 0
        self.lock = Lock()
        self.yaml_path = Path(yaml_path).with_suffix(".yaml")
        scheme, _, _ = data_store_prefix.rpartition("://")
        self.backend = backend
        if not scheme:
            data_store_prefix = os.path.abspath(data_store_prefix)
        self.prefix = data_store_prefix
        self.index_name = index_name
        cls = CatalogueBackends[backend].value
        self.store = cls(
            data_store_prefix,
            index_name,
            self.config.index_schema,
            lock=self.lock,
            mode="w",
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
        self._queue: Queue[Tuple[Awaitable[Dict[str, Any]], str]] = Queue()

    def write_batch(self, index_name: str, batch: List[Dict[str, Any]]) -> None:
        """Write a batch of metadata to the storage system."""

    async def put(
        self,
        inp: Metadata,
        drs_type: str,
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

    async def __aenter__(self) -> "CatalogueWriter":
        return self

    async def __aexit__(self, *args: Any, **kwargs: Any) -> None:
        await self.close()

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
        with self.yaml_path.open("w", encoding="utf-8") as f:
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
            del metadata
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
