"""MongoDB metadata storage backend.

Both the *read* and *write* paths use synchronous :mod:`pymongo`.
The writer daemon runs in a spawned process.  The reader offloads
cursor iteration to a single-thread
:class:`~concurrent.futures.ThreadPoolExecutor` one batch at a time
so the event loop stays responsive without pulling the entire
collection into memory.

.. note::

   ``pymongo`` is an **optional** dependency.  Install it separately
   (``pip install pymongo`` / ``conda install pymongo``) before
   selecting ``backend="mongodb"``.
"""

from __future__ import annotations

import asyncio
import multiprocessing as mp
from concurrent.futures import ThreadPoolExecutor
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    ClassVar,
    Dict,
    List,
    Literal,
    Optional,
    Tuple,
    Union,
)
from urllib.parse import (
    ParseResult,
    parse_qs,
    quote_plus,
    urlencode,
    urlparse,
    urlunparse,
)

from ...logger import logger
from ..config import SchemaField
from .base import (
    BackendWriter,
    IndexName,
    IndexStore,
    MetadataRecord,
    StorageOptions,
    Stream,
)

if TYPE_CHECKING:
    from pymongo import MongoClient
    from pymongo.collection import Collection
    from pymongo.cursor import Cursor

_IMPORT_ERR = (
    "The mongodb storage backend requires 'pymongo'. "
    "Install it with:  pip install pymongo"
)


def _sanitise_uri(uri: str, timeout_ms: int = 5000, **kwargs: Any) -> str:
    """Merge storage options into a MongoDB URI.

    This function is idempotent -- calling it on an already-sanitised
    URI with the same keyword arguments produces the same result.
    """
    uri = str(uri)
    netloc, _, body = uri.rpartition("://")
    netloc = netloc or "mongodb"
    parsed = urlparse(f"{netloc}://{body}")

    # Credentials -- kwargs override URI, URI is the fallback
    username = kwargs.get("username") or kwargs.get("user") or parsed.username
    password = kwargs.get("password") or kwargs.get("passwd") or parsed.password
    database = kwargs.get("database") or kwargs.get("db")
    port = str(kwargs.get("port", "")) or "27017"

    # Netloc
    host = parsed.hostname or "localhost"
    port = f":{parsed.port}" if parsed.port else f":{port}"
    if username:
        creds = quote_plus(str(username))
        if password:
            creds = f"{creds}:{quote_plus(str(password))}"
        netloc = f"{creds}@{host}{port}"
    else:
        netloc = f"{host}{port}"

    # Path (database)
    path = f"/{database}" if database else parsed.path.rstrip("/")

    # Query params -- kwargs merge in, existing values preserved
    query = parse_qs(parsed.query)
    for key, value in kwargs.items():
        if key not in ("username", "user", "password", "database"):
            query[key] = [str(value)]
    if "authsource" not in {k.lower() for k in query}:
        query["authSource"] = ["admin"]
    if "timeoutms" not in {k.lower() for k in query}:
        query["timeoutMS"] = [str(timeout_ms)]

    result = urlunparse(
        ParseResult(
            "mongodb",
            netloc,
            path,
            parsed.params,
            urlencode(query, doseq=True),
            parsed.fragment,
        )
    )
    return result


# ------------------------------------------------------------------
# Writer daemon (runs in a spawned process, always sync)
# ------------------------------------------------------------------


class MongoDBWriter(BackendWriter):
    """Synchronous writer that bulk-upserts metadata into MongoDB.

    Intended to run inside a spawned process, consuming batches from
    a multiprocessing queue -- the same pattern as
    :py:class:`~.jsonlines.JSONLineWriter`.
    """

    backend: ClassVar[str] = "MongoDB"
    has_catalogue_storage: ClassVar[bool] = True

    def __post_init__(self) -> None:
        try:
            from pymongo import MongoClient as _MongoClient
        except ImportError:
            raise ImportError(_IMPORT_ERR) from None
        unique_key = self.storage_options.pop("unique_key", "file")
        uri = _sanitise_uri(self.streams[0].path, **self.storage_options)
        self._client: "MongoClient[MetadataRecord]" = _MongoClient(uri)
        db = self._client.get_default_database(default="metadata")
        self._collections: Dict[str, "Collection[MetadataRecord]"] = {
            s.name: db[s.name] for s in self.streams
        }
        self._unique_key = unique_key
        for col in self._collections.values():
            col.create_index(unique_key, unique=True)

    def _write_table(self, table_name: str, rows: List[MetadataRecord]) -> int:
        from pymongo import UpdateOne

        ops = [
            UpdateOne(
                {self._unique_key: doc[self._unique_key]},
                {"$set": doc},
                upsert=True,
            )
            for doc in rows
            if self._unique_key in doc
        ]
        if ops:
            self._collections[table_name].bulk_write(ops, ordered=False)
            self.indexed_objects += len(ops)
        return len(rows)

    def _close(self) -> None:
        """Close the client connection."""
        self._client.close()


# ------------------------------------------------------------------
# Store class
# ------------------------------------------------------------------


class MongoDB(IndexStore):
    """Read and write metadata in a MongoDB database.

    Parameters
    ^^^^^^^^^^
    path:
        MongoDB connection URI, e.g.
        ``mongodb://localhost:27017``.
    index_name:
        Names of the metadata indexes (used as collection names).
    schema:
        The metadata schema definition.
    """

    suffix = ""
    driver = "mongodb"
    has_catalogue_storage = True

    _CATALOGUE_COLLECTION = "_catalogue"

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
        """Set up MongoDB connection state instead of fsspec."""
        self._uri: str = _sanitise_uri(path, **self.storage_options)
        self._unique_key: str = self._resolve_unique_key()
        parsed = urlparse(self._uri)
        self._database: str = parsed.path.strip("/") or "metadata"
        self._proc: Optional[mp.process.BaseProcess] = None
        if self.mode == "w":
            streams = (Stream(name=n, path=self._uri) for n in self.index_names)
            args = (self.queue, self._sent, self.counter) + tuple(streams)
            kwargs = {k: v for (k, v) in self.storage_options.items()}
            kwargs["unique_key"] = self._unique_key
            self._proc = self._ctx.Process(
                target=MongoDBWriter.as_daemon,
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
        cls, client: "MongoClient[MetadataRecord]", payload: Dict[str, Any]
    ) -> None:
        payload["_id"] = "metadata"
        db = client.get_default_database(default="metadata")
        col = db[cls._CATALOGUE_COLLECTION]
        col.replace_one(
            {"_id": "metadata"},
            payload,
            upsert=True,
        )

    def write_catalogue_metadata(self, payload: Dict[str, Any]) -> None:
        """Store catalogue metadata in a ``_catalogue`` collection."""
        try:
            from pymongo import MongoClient as _MongoClient
        except ImportError:
            raise ImportError(_IMPORT_ERR) from None

        client: "MongoClient[MetadataRecord]" = _MongoClient(self._uri)
        payload["_id"] = "metadata"
        try:
            self._write_metadata(client, payload)
        finally:
            client.close()

    @property
    def total_objects(self) -> int:
        """Get the number of total metadata objects in all collection."""
        try:
            from pymongo import MongoClient as _MongoClient
        except ImportError:
            raise ImportError(_IMPORT_ERR) from None

        client: "MongoClient[MetadataRecord]" = _MongoClient(self._uri)
        total = 0
        try:
            db = client.get_default_database(default="metadata")
            for name in self.index_names:
                total += db[name].estimated_document_count()
        finally:
            client.close()
        return total

    @classmethod
    def read_catalogue_metadata(cls, url: str, **kwargs: Any) -> MetadataRecord:
        """Read catalogue metadata from the ``_catalogue`` collection."""
        try:
            from pymongo import MongoClient as _MongoClient
        except ImportError:
            raise ImportError(_IMPORT_ERR) from None

        uri = _sanitise_uri(url, **kwargs)
        client: "MongoClient[MetadataRecord]" = _MongoClient(uri)
        try:
            db = client.get_default_database(default="metadata")
            doc = db[cls._CATALOGUE_COLLECTION].find_one({"_id": "metadata"})
            if doc is None:
                raise ValueError(
                    f"No catalogue metadata found in collection "
                    f"{cls._CATALOGUE_COLLECTION}"
                )
            doc.pop("_id", None)
            return doc
        finally:
            client.close()

    async def read(
        self,
        index_name: str,
    ) -> AsyncIterator[List[MetadataRecord]]:
        """Yield batches of metadata records from a MongoDB collection.

        Uses synchronous :mod:`pymongo` in a single-thread executor,
        fetching one batch at a time so memory stays bounded.

        Parameters
        ^^^^^^^^^^
        index_name:
            Name of the collection to read from.

        Yields
        ^^^^^^
        List[MetadataRecord]:
            Deserialised metadata records.
        """
        try:
            from pymongo import MongoClient as _MongoClient
        except ImportError:
            raise ImportError(_IMPORT_ERR) from None

        loop = asyncio.get_running_loop()
        batch_size = self.batch_size
        uri = self._uri
        collection_name = index_name

        def _open() -> Tuple["MongoClient[MetadataRecord]", "Cursor[MetadataRecord]"]:
            client: "MongoClient[MetadataRecord]" = _MongoClient(uri)
            db = client.get_default_database(default="metadata")
            cursor = db[collection_name].find(
                {}, {"_id": 0, self._epoch_key: 0}, batch_size=batch_size
            )
            return client, cursor

        def _next_batch(
            cursor: "Cursor[MetadataRecord]",
        ) -> Optional[List[MetadataRecord]]:
            batch: List[MetadataRecord] = []
            for doc in cursor:
                batch.append(doc)
                if len(batch) >= batch_size:
                    return batch
            return batch or None

        with ThreadPoolExecutor(max_workers=1) as pool:
            client, cursor = await loop.run_in_executor(pool, _open)
            try:
                while True:
                    batch = await loop.run_in_executor(pool, _next_batch, cursor)
                    if batch is None:
                        break
                    yield batch
            finally:
                await loop.run_in_executor(pool, client.close)

    def count_stale_objects(self, epoch: float) -> int:
        """Count the number of stale (outdated objects)."""
        from pymongo import MongoClient as _MongoClient

        client: "MongoClient[MetadataRecord]" = _MongoClient(self._uri)
        total = 0
        try:
            db = client.get_default_database(default="metadata")
            for name in self.index_names:
                total += db[name].count_documents({self._epoch_key: {"$lt": epoch}})
        finally:
            client.close()
        return total

    def sweep(self, epoch: float) -> None:
        """Delete all records whose epoch differs from *epoch*."""
        from pymongo import MongoClient as _MongoClient

        client: "MongoClient[MetadataRecord]" = _MongoClient(self._uri)
        total = 0
        try:
            db = client.get_default_database(default="metadata")
            for name in self.index_names:
                result = db[name].delete_many({self._epoch_key: {"$lt": epoch}})
                total += result.deleted_count
        finally:
            client.close()
        logger.info("Cleaned up %i old entries from database.", total)
