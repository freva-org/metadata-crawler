"""PostgreSQL metadata storage backend.

Both the *read* and *write* paths use synchronous :mod:`sqlalchemy`
targeting PostgreSQL exclusively.  Native ``ARRAY`` column types are
used for multi-valued fields so datetimes and strings round-trip
without JSON serialisation.

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
import time
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

# ------------------------------------------------------------------
# Schema helpers
# ------------------------------------------------------------------


def _sa_column_type(field: SchemaField) -> sa.types.TypeEngine[Any]:
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
    metadata: sa.MetaData,
    epoch_key: str = "_crawl_epoch",
) -> sa.Table:
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
    if username:
        parsed = parsed.set(username=username)
    if password:
        parsed = parsed.set(password=password)
    if database:
        parsed = parsed.set(database=database)

    return parsed.render_as_string(hide_password=hide_password)


@contextmanager
def _open_db_connection(
    url: str, sanitise: bool = True, **storage_options: Any
) -> Iterator["sa.Connection"]:
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

    Uses ``INSERT ... ON CONFLICT DO UPDATE`` for atomic upserts.
    """

    backend: ClassVar[str] = "PostgresQL"

    def __post_init__(self) -> None:
        try:
            import sqlalchemy as sa
        except ImportError:
            raise ImportError(_IMPORT_ERR) from None
        unique_key = self.storage_options.pop("unique_key", "file")
        schema_json = self.storage_options.pop("schema_json", "{}")
        url = _get_storage_url(self.streams[0].path, **self.storage_options)
        self._engine: sa.Engine = sa.create_engine(url, pool_pre_ping=True)
        self._sa_meta: sa.MetaData = sa.MetaData()
        self._schema: Dict[str, SchemaField] = {
            k: SchemaField(**v) for k, v in json.loads(schema_json).items()
        }
        self._unique_key = unique_key
        self._tables: Dict[str, sa.Table] = {
            s.name: _build_table(
                s.name, self._schema, self._sa_meta, epoch_key=self._epoch_key
            )
            for s in self.streams
        }
        self._sa_meta.create_all(self._engine)

    def add(self, metadata_batch: List[Tuple[str, MetadataRecord]]) -> None:
        """Upsert a batch into the appropriate tables."""
        by_table: Dict[str, List[MetadataRecord]] = {name: [] for name in self._tables}
        now = time.time()
        for table_name, metadata in metadata_batch:
            if table_name in by_table:
                metadata[self._epoch_key] = now
                by_table[table_name].append(metadata)

        with self._engine.begin() as conn:
            for table_name, rows in by_table.items():
                if not rows:
                    continue
                table = self._tables[table_name]
                self._upsert_rows(conn, table, rows)
                self.indexed_objects += len(rows)

    def _upsert_rows(
        self,
        conn: sa.Connection,
        table: sa.Table,
        rows: List[MetadataRecord],
    ) -> None:
        """Insert rows, updating on unique-key conflict."""
        from sqlalchemy.dialects.postgresql import insert

        uk = self._unique_key
        for row in rows:
            stmt = insert(table).values(**row)
            stmt = stmt.on_conflict_do_update(
                index_elements=[uk],
                set_={k: v for k, v in row.items() if k != uk},
            )
            conn.execute(stmt)

    def close(self) -> None:
        """Dispose of the engine."""
        self._engine.dispose()


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
        kwargs.pop("collection", None)
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
        self._unique_key: str = self._resolve_unique_key()
        self._proc: Optional[mp.process.BaseProcess] = None
        if self.mode == "w":
            schema_json = json.dumps(
                {k: json.loads(s.model_dump_json()) for k, s in self.schema.items()}
            )
            streams = (Stream(name=n, path=self._url) for n in self.index_names)
            args = (self.queue, self._sent) + tuple(streams)
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

    def write_catalogue_metadata(self, payload: Dict[str, Any]) -> None:
        """Store catalogue metadata in a ``_catalogue`` table."""
        try:
            import sqlalchemy as sa
            from sqlalchemy.dialects.postgresql import insert
        except ImportError:
            raise ImportError(_IMPORT_ERR) from None
        value = json.dumps(payload)
        engine: sa.Engine = sa.create_engine(self._url, pool_pre_ping=True)
        try:
            meta_table: sa.Table = sa.Table(
                self._CATALOGUE_TABLE,
                sa.MetaData(),
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

        try:
            with _open_db_connection(url, **kwargs) as conn:
                result: sa.CursorResult[Tuple[str]] = conn.execute(
                    sa.text(
                        f"SELECT value FROM {cls._CATALOGUE_TABLE} "
                        "WHERE key = 'metadata'"
                    )
                )
                row = result.fetchone()
                payload = None if row is None else json.loads(row[0])
        except Exception as error:
            logger.warning(error)
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
        except ImportError:
            raise ImportError(_IMPORT_ERR) from None

        loop = asyncio.get_running_loop()
        batch_size = self.batch_size
        url = self._url
        full_table_name = index_name

        def _open() -> Tuple[
            sa.Engine, Optional[sa.Connection], Optional[sa.CursorResult[Any]]
        ]:
            engine: sa.Engine = sa.create_engine(url, pool_pre_ping=True)
            sa_meta: sa.MetaData = sa.MetaData()
            sa_meta.reflect(bind=engine)
            table = sa_meta.tables.get(full_table_name)
            if table is None:
                engine.dispose()
                return engine, None, None
            conn = engine.connect()
            result = conn.execute(sa.select(table))
            return engine, conn, result

        def _next_batch(
            result: sa.CursorResult[Any],
        ) -> Optional[List[MetadataRecord]]:
            batch: List[MetadataRecord] = []
            for row in result.mappings():
                record = {str(k): v for k, v in row.items()}
                record.pop(self._epoch_key, None)
                batch.append(record)
                if len(batch) >= batch_size:
                    return batch
            return batch or None

        def _close(engine: sa.Engine, conn: Optional[sa.Connection]) -> None:
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
