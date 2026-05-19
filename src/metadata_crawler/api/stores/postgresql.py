"""PostgreSQL metadata storage backend.

The *write* path uses raw :mod:`psycopg` (v3) with ``executemany``
for maximum throughput.  :mod:`sqlalchemy` is used only for table
creation (DDL) and cold-path operations (reads, catalogue metadata,
sweep).  Native ``ARRAY`` column types are used for multi-valued
fields so datetimes and strings round-trip without JSON serialisation.

The writer daemon runs in a spawned process.  The reader offloads
cursor iteration to a single-thread
:class:`~concurrent.futures.ThreadPoolExecutor` one batch at a time
so the event loop stays responsive without pulling the entire
table into memory.

.. note::

   ``sqlalchemy`` and ``psycopg`` are **optional** dependencies.
   Install them separately (``pip install sqlalchemy psycopg`` /
   ``conda install sqlalchemy psycopg``) before selecting
   ``backend="postgresql"``.
"""

from __future__ import annotations

import asyncio
import json
import multiprocessing as mp
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    ClassVar,
    Dict,
    Iterator,
    List,
    Literal,
    Optional,
    Tuple,
    Union,
)
from urllib.parse import parse_qs, urlparse

from ...logger import logger
from ..config import BaseType, SchemaField
from .base import (
    BackendWriter,
    IndexName,
    IndexStore,
    MetadataRecord,
    StorageOptions,
    Stream,
)

if TYPE_CHECKING:
    import sqlalchemy as sa

_IMPORT_ERR = (
    "The postgresql storage backend requires 'sqlalchemy' and 'psycopg'. "
    "Install them with:  pip install sqlalchemy psycopg"
)
_DEFAULT_DB_SCHEMA = "metadata_crawler"

# ------------------------------------------------------------------
# Schema helpers
# ------------------------------------------------------------------


def _sa_column_type(field: SchemaField) -> "sa.types.TypeEngine[Any]":
    """Map a :class:`SchemaField` to a PostgreSQL-native column type."""
    import sqlalchemy as sa

    match field.base_type:
        case BaseType.string:
            if field.multi_valued:
                return sa.ARRAY(sa.Text())
            return sa.Text()
        case BaseType.integer:
            if field.multi_valued:
                return sa.ARRAY(sa.BigInteger())
            return sa.BigInteger()
        case BaseType.float:
            if field.multi_valued:
                return sa.ARRAY(sa.Float())
            return sa.Float()
        case BaseType.timestamp:
            if field.multi_valued or field.length is not None:
                return sa.ARRAY(sa.DateTime(timezone=True))
            return sa.DateTime(timezone=True)


def _build_table(
    table_name: str,
    schema: Dict[str, SchemaField],
    metadata: "sa.MetaData",
    epoch_key: str = "_crawl_epoch",
) -> "sa.Table":
    """Create a :class:`sqlalchemy.Table` from the crawler schema."""
    import sqlalchemy as sa

    columns: List[sa.Column[Any]] = []
    # Internal bookkeeping, not part of the metadata schema
    columns.append(sa.Column(epoch_key, sa.Float(), index=True, nullable=True))
    for key, field in schema.items():
        col_type = _sa_column_type(field)
        is_array = isinstance(col_type, sa.ARRAY)
        columns.append(
            sa.Column(
                key,
                col_type,
                unique=field.unique and not is_array,
                nullable=not field.required,
                index=field.indexed and not is_array,
            )
        )
    return sa.Table(table_name, metadata, *columns)


# ------------------------------------------------------------------
# URL helpers
# ------------------------------------------------------------------
def _get_storage_options(url: str, key: str = "search_path") -> str:
    """Get storage options from url."""
    for opt in parse_qs(urlparse(url).query).get("options", []):
        if key in opt:
            return opt.split("=", 1)[-1]
    return ""  # pragma: no cover


def _get_storage_url(url: str, hide_password: bool = False, **kwargs: Any) -> str:
    """Normalise a PostgreSQL connection URL.

    Ensures the ``psycopg`` (v3) driver is selected and merges
    credential / database overrides from *kwargs* into the URL.
    """
    try:
        import sqlalchemy as sa
    except ImportError:
        raise ImportError(_IMPORT_ERR) from None

    netloc, _, rest = url.rpartition("://")
    netloc = netloc or "postgresql"
    parsed = sa.engine.make_url(f"{netloc}://{rest}")
    parsed = parsed.set(drivername="postgresql+psycopg")
    parsed = parsed.set(port=parsed.port or kwargs.get("port", "5432"))
    username = kwargs.get("username") or kwargs.get("user")
    password = kwargs.get("password") or kwargs.get("passwd")
    database = kwargs.get("database") or kwargs.get("db")
    db_schema = kwargs.get("db_schema") or kwargs.get("pg_schema") or _DEFAULT_DB_SCHEMA

    if username:
        parsed = parsed.set(username=username)
    if password:
        parsed = parsed.set(password=password)
    if database:
        parsed = parsed.set(database=database)
    if db_schema and "options" not in parsed.query:
        parsed = parsed.update_query_dict(
            {
                "options": f"-csearch_path={db_schema}",
            }
        )
    return parsed.render_as_string(hide_password=hide_password)


def _get_psycopg_url(sa_url: str) -> str:
    """Convert a SQLAlchemy URL to a plain psycopg connection string."""
    return sa_url.replace("postgresql+psycopg://", "postgresql://")


@contextmanager
def _open_db_connection(
    url: str, sanitise: bool = True, **storage_options: Any
) -> "Iterator[sa.Connection]":
    try:
        import sqlalchemy as sa
    except ImportError:
        raise ImportError(_IMPORT_ERR) from None

    url = _get_storage_url(url, **storage_options) if sanitise else url
    engine: sa.Engine = sa.create_engine(url, pool_pre_ping=True)
    with engine.begin() as conn:
        yield conn
    engine.dispose()


# ------------------------------------------------------------------
# Writer daemon (runs in a spawned process, always sync)
# ------------------------------------------------------------------


class PostgreSQLWriter(BackendWriter):
    """Synchronous writer that upserts metadata into PostgreSQL.

    Uses raw :mod:`psycopg` ``executemany`` with pipelining for
    maximum write throughput.  SQLAlchemy is used only for initial
    table creation (DDL).
    """

    backend: ClassVar[str] = "PostgresQL"
    has_catalogue_storage: ClassVar[bool] = True

    def __post_init__(self) -> None:
        try:
            import psycopg as pg
            import sqlalchemy as sa
        except ImportError:
            raise ImportError(_IMPORT_ERR) from None

        unique_key = self.storage_options.pop("unique_key", "file")
        schema_json = self.storage_options.pop("schema_json", "{}")
        self._url = _get_storage_url(self.streams[0].path, **self.storage_options)
        self._db_schema = _get_storage_options(self._url).partition(",")[0]
        self._pending: Dict[str, int] = {s.name: 0 for s in self.streams}
        self._commit_interval: int = 10

        # --- SQLAlchemy: DDL only (create tables if needed) ---
        engine: sa.Engine = sa.create_engine(self._url, pool_pre_ping=True)
        with engine.begin() as conn:
            conn.execute(sa.schema.CreateSchema(self._db_schema, if_not_exists=True))
        sa_meta: sa.MetaData = sa.MetaData(schema=self._db_schema)
        schema: Dict[str, SchemaField] = {
            k: SchemaField(**v) for k, v in json.loads(schema_json).items()
        }
        self._unique_key = unique_key
        tables: Dict[str, sa.Table] = {
            s.name: _build_table(s.name, schema, sa_meta, epoch_key=self._epoch_key)
            for s in self.streams
        }
        sa_meta.create_all(engine)
        engine.dispose()

        # --- Raw psycopg3: pre-build upsert SQL, open connection ---
        self._conns: Dict[str, pg.Connection[Any]] = {
            s.name: pg.connect(_get_psycopg_url(self._url), autocommit=False)
            for s in self.streams
        }
        self._upsert_sql: Dict[str, str] = {}
        for name, table in tables.items():
            columns = [col.name for col in table.columns]
            col_list = ", ".join(f'"{c}"' for c in columns)
            placeholders = ", ".join(f"%({c})s" for c in columns)
            update_set = ", ".join(
                f'"{c}" = EXCLUDED."{c}"' for c in columns if c != unique_key
            )
            self._upsert_sql[name] = (
                f"INSERT INTO {table.name} ({col_list}) "
                f"VALUES ({placeholders}) "
                f'ON CONFLICT ("{unique_key}") '
                f"DO UPDATE SET {update_set}"
            )

    def _write_table(self, table_name: str, rows: List[MetadataRecord]) -> int:
        conn = self._conns[table_name]
        cursor = conn.cursor()
        cursor.executemany(self._upsert_sql[table_name], rows)
        self.indexed_objects += len(rows)
        self._pending[table_name] += 1
        if self._pending[table_name] >= self._commit_interval:
            conn.commit()
            self._pending[table_name] = 0
        return len(rows)

    def _close(self) -> None:
        """Close the raw connection."""
        for conn in self._conns.values():
            conn.commit()
            conn.close()


# ------------------------------------------------------------------
# Store class
# ------------------------------------------------------------------


class PostgreSQL(IndexStore):
    """Read and write metadata in a PostgreSQL database.

    Parameters
    ^^^^^^^^^^
    path:
        PostgreSQL connection URL, e.g.
        ``postgresql://user:pw@host/dbname``.
    index_name:
        Names of the metadata indexes (used as table names).
    schema:
        The metadata schema definition.
    """

    suffix = ""
    driver = "postgresql"
    has_catalogue_storage = True

    _CATALOGUE_TABLE = "_catalogue"

    def __init__(
        self,
        path: str,
        index_name: IndexName,
        schema: Dict[str, SchemaField],
        mode: Literal["w", "r"] = "r",
        storage_options: Optional[StorageOptions] = None,
        shadow: Optional[Union[str, List[str]]] = None,
        batch_size: int = 25_000,
        **kwargs: Any,
    ):
        super().__init__(
            path,
            index_name,
            schema,
            mode=mode,
            shadow=shadow,
            storage_options=storage_options,
            batch_size=batch_size,
            **kwargs,
        )

    # ------------------------------------------------------------------
    # Override filesystem init
    # ------------------------------------------------------------------

    def _init_storage(self, path: str, **kwargs: Any) -> None:
        """Set up PostgreSQL connection state instead of fsspec."""
        self._url: str = _get_storage_url(path, **self.storage_options)
        self._db_schema = _get_storage_options(self._url).partition(",")[0]
        self._unique_key: str = self._resolve_unique_key()
        self._proc: Optional[mp.process.BaseProcess] = None
        if self.mode == "w":
            schema_json = json.dumps(
                {k: json.loads(s.model_dump_json()) for k, s in self.schema.items()}
            )
            streams = (Stream(name=n, path=self._url) for n in self.index_names)
            args = (self.queue, self._sent, self.counter) + tuple(streams)
            kwargs = {k: v for (k, v) in self.storage_options.items()}
            kwargs["unique_key"] = self._unique_key
            kwargs["schema_json"] = schema_json
            self._proc = self._ctx.Process(
                target=PostgreSQLWriter.as_daemon,
                args=args,
                kwargs={"storage_options": kwargs},
                daemon=True,
            )
            self._proc.start()

    def _resolve_unique_key(self) -> str:
        """Determine the unique key from the schema."""
        for name, field in self.schema.items():
            if field.unique:
                return name
        return "file"

    # ------------------------------------------------------------------
    # IndexStore interface
    # ------------------------------------------------------------------

    @property
    def proc(self) -> Optional[mp.process.BaseProcess]:
        """The writer process."""
        return self._proc

    @classmethod
    def _write_metadata(
        cls, engine: "sa.Engine", payload: Dict[str, Any], db_schema: str
    ) -> None:
        try:
            import sqlalchemy as sa
            from sqlalchemy.dialects.postgresql import insert
        except ImportError:
            raise ImportError(_IMPORT_ERR) from None
        value = json.dumps(payload)
        meta_table: sa.Table = sa.Table(
            cls._CATALOGUE_TABLE,
            sa.MetaData(schema=db_schema),
            sa.Column("key", sa.Text(), primary_key=True),
            sa.Column("value", sa.Text()),
        )
        meta_table.create(engine, checkfirst=True)
        with engine.begin() as conn:
            stmt = insert(meta_table).values(key="metadata", value=value)
            stmt = stmt.on_conflict_do_update(
                index_elements=["key"],
                set_={"value": value},
            )
            conn.execute(stmt)

    def write_catalogue_metadata(self, payload: Dict[str, Any]) -> None:
        """Store catalogue metadata in a ``_catalogue`` table."""
        try:
            import sqlalchemy as sa
        except ImportError:
            raise ImportError(_IMPORT_ERR) from None
        engine: sa.Engine = sa.create_engine(self._url, pool_pre_ping=True)
        try:
            self._write_metadata(engine, payload, self._db_schema)
        finally:
            engine.dispose()

    @property
    def total_objects(self) -> int:
        """Get the number of total metadata objects in all tables."""
        try:
            import sqlalchemy as sa
        except ImportError:
            raise ImportError(_IMPORT_ERR) from None
        total = 0
        with _open_db_connection(self._url, sanitise=False) as conn:
            for name in self.index_names:
                row = conn.execute(sa.text(f"SELECT COUNT(*) FROM {name}")).fetchone()
                total += 0 if row is None else row[0]
        return total

    @classmethod
    def read_catalogue_metadata(cls, url: str, **kwargs: Any) -> MetadataRecord:
        """Read catalogue metadata from the ``_catalogue`` table."""
        try:
            import sqlalchemy as sa
        except ImportError:
            raise ImportError(_IMPORT_ERR) from None
        payload: Optional[MetadataRecord] = None
        db_schema = kwargs.get("db_schema") or kwargs.get(
            "pg_schema", _DEFAULT_DB_SCHEMA
        )
        db_schema = _get_storage_options(url).partition(",")[0] or db_schema
        with _open_db_connection(url, **kwargs) as conn:
            result: "sa.CursorResult[Tuple[str]]" = conn.execute(
                sa.text(
                    f"SELECT value FROM {db_schema}.{cls._CATALOGUE_TABLE} WHERE key = 'metadata'"
                )
            )
            row = result.fetchone()
            payload = None if row is None else json.loads(row[0])
        if payload is None:
            raise ValueError(
                f"No catalogue metadata found in table '{cls._CATALOGUE_TABLE}'"
            )
        return payload

    async def read(
        self,
        index_name: str,
    ) -> AsyncIterator[List[MetadataRecord]]:
        """Yield batches of metadata records from a PostgreSQL table.

        Uses synchronous :mod:`sqlalchemy` in a single-thread executor,
        fetching one batch at a time so memory stays bounded.

        Parameters
        ^^^^^^^^^^
        index_name:
            Name of the table to read from.

        Yields
        ^^^^^^
        List[MetadataRecord]:
            Deserialised metadata records.
        """
        try:
            import sqlalchemy as sa
            from sqlalchemy.exc import NoSuchTableError
        except ImportError:
            raise ImportError(_IMPORT_ERR) from None

        loop = asyncio.get_running_loop()
        batch_size = self.batch_size
        url = self._url

        def _open() -> Tuple[
            "sa.Engine", Optional["sa.Connection"], Optional["sa.CursorResult[Any]"]
        ]:
            engine: sa.Engine = sa.create_engine(url, pool_pre_ping=True)
            sa_meta: sa.MetaData = sa.MetaData(schema=self._db_schema)
            sa_meta.reflect(bind=engine)
            try:
                table = sa.Table(
                    index_name,
                    sa_meta,
                    schema=self._db_schema,
                    autoload_with=engine,
                )
            except NoSuchTableError:
                engine.dispose()
                logger.critical("Cloud not find table %s", index_name)
                return engine, None, None

            conn = engine.connect()
            result = conn.execute(sa.select(table))
            return engine, conn, result

        def _next_batch(
            result: "sa.CursorResult[Any]",
        ) -> Optional[List[MetadataRecord]]:
            batch: List[MetadataRecord] = []
            for row in result.mappings():
                record = {str(k): v for k, v in row.items()}
                record.pop(self._epoch_key, None)
                batch.append(record)
                if len(batch) >= batch_size:
                    return batch
            return batch or None

        def _close(engine: "sa.Engine", conn: Optional["sa.Connection"]) -> None:
            if conn is not None:
                conn.close()
            engine.dispose()

        with ThreadPoolExecutor(max_workers=1) as pool:
            engine, conn, result = await loop.run_in_executor(pool, _open)
            if result is None:
                return
            try:
                while True:
                    batch = await loop.run_in_executor(pool, _next_batch, result)
                    if batch is None:
                        break
                    yield batch
            finally:
                await loop.run_in_executor(pool, _close, engine, conn)

    def count_stale_objects(self, epoch: float) -> int:
        """Count the number of stale (outdated objects)."""
        try:
            import sqlalchemy as sa
        except ImportError:
            raise ImportError(_IMPORT_ERR) from None
        total = 0
        with _open_db_connection(self._url, sanitise=False) as conn:
            for name in self.index_names:
                row = conn.execute(
                    sa.text(
                        f"SELECT COUNT(*) FROM {name} WHERE {self._epoch_key} < :epoch"
                    ),
                    {"epoch": epoch},
                ).fetchone()
                total += 0 if row is None else row[0]
        return total

    def sweep(self, epoch: float) -> None:
        """Delete all records whose epoch differs from *epoch*."""
        try:
            import sqlalchemy as sa
        except ImportError:
            raise ImportError(_IMPORT_ERR) from None

        total = 0
        with _open_db_connection(self._url, sanitise=False) as conn:
            for name in self.index_names:
                result = conn.execute(
                    sa.text(f"DELETE FROM {name} WHERE {self._epoch_key} < :epoch"),
                    {"epoch": epoch},
                )
                total += result.rowcount
        logger.info("Cleaned up %i old entries from database.", total)
