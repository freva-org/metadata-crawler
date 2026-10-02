"""API for adding new cataloging systems."""

from __future__ import annotations

import abc
from pathlib import Path
from types import TracebackType
from typing import (
    Any,
    AsyncIterator,
    ClassVar,
    Dict,
    List,
    Optional,
    Self,
    Sequence,
    Tuple,
    Type,
    Union,
    cast,
    get_type_hints,
)

from ..logger import logger
from ..utils import Console, IndexProgress
from .config import SchemaField
from .metadata_stores import CatalogueReader
from .stores import IndexStore
from .stores.base import BaseConnection


class BaseIndex:
    """Base class to index metadata in the indexing system.

    Any data ingestion class that implements metadata ingestion into
    cataloguing systems should inherit from this class.

    This abstract class will setup consumer threads and a fifo queue that wait
    for new data to harvest metadata and add it to the cataloguing system.
    Only :py:func:`add` and :py:func:`delete` are abstract methods that need
    to be implemented for each cataloguing ingestion class. The rest is done
    by this base class.

    Parameters
    ^^^^^^^^^^
    uri:
        Uri to the metadata store, or a sequence of uris. All stores are read
        into the *same* index target, which is what makes a single blue/green
        rotation cover the whole ingest.
    batch_size:
        The amount for metadata that should be gathered `before` ingesting
        it into the catalogue.
    progress:
        Optional rich progress object that should display the progress of the
        tasks.
    target:
        Connection to the index system, e.g. a named connection from
        ``connections.toml``. It must be an instance of :attr:`connection`.
        Options given explicitly to :meth:`index` or :meth:`delete` win over
        the values of the target.

        .. versionadded:: 2610.0.0

    Attributes
    ^^^^^^^^^^
    """

    connection: ClassVar[Optional[Type[BaseConnection]]] = None
    """The connection model of the index system, if it supports named targets.

    Mark the option of :meth:`index` and :meth:`delete` that says where the
    index system is (``--server``, ``--url``, ...) with
    ``cli_parameter(..., connection=True)``. That option then also accepts
    the name of a connection, which reaches the instance as :attr:`target`.
    """

    target: Optional[BaseConnection] = None
    """The connection to the index system, if one was given."""

    def __init__(
        self,
        uri: Optional[Union[str, Path, Sequence[Union[str, Path]]]] = None,
        batch_size: int = 2500,
        storage_options: Optional[Dict[str, Any]] = None,
        progress: Optional[IndexProgress] = None,
        target: Optional[BaseConnection] = None,
        **kwargs: Any,
    ) -> None:
        self.target = self._check_target(target)
        self._stores: List[IndexStore] = []
        self.progress = progress or IndexProgress(total=-1)
        for _uri in IndexStore.normalise_uris(uri):
            _reader = CatalogueReader(
                store_url=_uri,
                batch_size=batch_size,
                storage_options=storage_options,
            )
            self._stores.append(_reader.store)
        self.__post_init__()

    @classmethod
    def _check_target(
        cls, target: Optional[BaseConnection]
    ) -> Optional[BaseConnection]:
        """Make sure *target* is a connection this index system can use."""
        if target is None:
            return None
        if cls.connection is None:
            raise TypeError(f"{cls.__name__} doesn't support connection targets")
        if not isinstance(target, cls.connection):
            raise TypeError(
                f"{cls.__name__} needs a {cls.connection.backend} connection, "
                f"{target.name!r} is a {target.backend} connection"
            )
        return target

    @classmethod
    def target_option(cls, method: str) -> Optional[str]:
        """Name of the option of *method* that accepts a connection, if any."""
        func = getattr(cls, method, None)
        if func is None or cls.connection is None:
            return None
        hints = get_type_hints(func, include_extras=True)
        for name, hint in hints.items():
            for extra in getattr(hint, "__metadata__", ()):
                if isinstance(extra, dict) and extra.get("connection"):
                    return name
        return None

    @property
    def _store(self) -> Optional[IndexStore]:
        """First metadata store, used as the schema / index-name reference."""
        return self._stores[0] if self._stores else None

    def __post_init__(self) -> None: ...

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None: ...

    @property
    def index_schema(self) -> Dict[str, SchemaField]:
        """Get the index schema."""
        return cast(Dict[str, SchemaField], getattr(self._store, "schema", {}))

    @property
    def index_names(self) -> Tuple[str, str]:
        """Get the names of the indexes for latests and all data.

        All configured stores must agree; indexing stores with different
        index names into one target would silently mix them up.
        """
        names = {
            cast(Tuple[str, str], getattr(store, "index_names", ("", "")))
            for store in self._stores
        }
        if len(names) > 1:
            raise ValueError(
                "The metadata stores disagree on their index names "
                f"({sorted(names)}); refusing to index them into one target."
            )
        return names.pop() if names else ("", "")

    async def get_metadata(
        self, index_name: str
    ) -> AsyncIterator[List[Dict[str, Any]]]:
        """Get the metadata of an index in batches.

        Batches of *all* configured metadata stores are chained, so a caller
        sees one continuous stream per index name.

        Parameters
        ^^^^^^^^^^
        index_name:
            Name of the index that should be read.
        """
        if not self._stores:
            return
        num_items = 0
        for store in self._stores:
            logger.debug("Reading index %s from %s", index_name, store)
            async for batch in store.read(index_name):
                yield batch
                self.progress.update(len(batch))
                num_items += len(batch)
        # NB: this counts the records *read from the stores*, not the records
        # accepted by the index system. The ingester reports the latter.
        msg = f"Read {num_items:10,.0f} items for index {index_name}"
        Console.print(msg) if Console.is_terminal else print(msg)

    @abc.abstractmethod
    async def delete(self, **kwargs: Any) -> None:
        """Delete data from the cataloguing system.

        Parameters
        ^^^^^^^^^^
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
        ^^^^^^^^^^
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
