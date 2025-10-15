"""API for adding new cataloging systems."""

from __future__ import annotations

import abc
from pathlib import Path
from typing import (
    Any,
    AsyncIterator,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
    cast,
)

from ..logger import logger
from ..utils import Console, IndexProgress
from .config import SchemaField
from .metadata_stores import CatalogueReader, IndexStore


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
    catalogue_file:
        Path to the intake catalogue
    batch_size:
        The amount for metadata that should be gathered `before` ingesting
        it into the catalogue.
    progress:
        Optional rich progress object that should display the progress of the
        tasks.

    Attributes
    ^^^^^^^^^^
    """

    def __init__(
        self,
        catalogue_file: Optional[Union[str, Path]] = None,
        batch_size: int = 2500,
        storage_options: Optional[Dict[str, Any]] = None,
        progress: Optional[IndexProgress] = None,
        **kwargs: Any,
    ) -> None:
        self._store: Optional[IndexStore] = None
        self.progress = progress or IndexProgress(total=-1)
        if catalogue_file is not None:
            _reader = CatalogueReader(
                catalogue_file=catalogue_file or "",
                batch_size=batch_size,
                storage_options=storage_options,
            )
            self._store = _reader.store
        self.__post_init__()

    def __post_init__(self) -> None: ...

    @property
    def index_schema(self) -> Dict[str, SchemaField]:
        """Get the index schema."""
        return cast(Dict[str, SchemaField], getattr(self._store, "schema", {}))

    @property
    def index_names(self) -> Tuple[str, str]:
        """Get the names of the indexes for latests and all data."""
        return cast(
            Tuple[str, str], getattr(self._store, "index_names", ("", ""))
        )

    async def get_metadata(
        self, index_name: str
    ) -> AsyncIterator[List[Dict[str, Any]]]:
        """Get the metadata of an index in batches.

        Parameters
        ^^^^^^^^^^
        index_name:
            Name of the index that should be read.
        """
        if self._store:
            batch = []
            num_items = 0
            logger.info("Indexing %s", index_name)
            async for batch in self._store.read(index_name):
                yield batch
                self.progress.update(len(batch))
                num_items += len(batch)
            msg = f"Indexed {num_items:10,.0f} items for index {index_name}"
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
