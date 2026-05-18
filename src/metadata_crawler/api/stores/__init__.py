"""Metadata storage backends.

This package provides pluggable backends for reading and writing
metadata collected by the crawler.  Each backend is an
:class:`IndexStore` subclass registered in
:class:`~metadata_crawler.api.metadata_stores.CatalogueBackends`.
"""

from .base import (
    BATCH_ITEM,
    BATCH_SECS_THRESHOLD,
    CatalogueBackendType,
    DateTimeDecoder,
    DateTimeEncoder,
    IndexName,
    IndexStore,
    StoreMetadata,
    Stream,
    WriterQueueType,
)
from .jsonlines import JSONLines, JSONLineWriter
from .mongodb import MongoDB, MongoDBWriter
from .postgresql import PostgreSQL, PostgreSQLWriter

__all__ = [
    # base
    "BATCH_ITEM",
    "BATCH_SECS_THRESHOLD",
    "CatalogueBackendType",
    "DateTimeDecoder",
    "DateTimeEncoder",
    "IndexName",
    "IndexStore",
    "StoreMetadata",
    "Stream",
    "WriterQueueType",
    # jsonlines
    "JSONLines",
    "JSONLineWriter",
    # mongodb
    "MongoDB",
    "MongoDBWriter",
    # sqlalchemy
    "PostgreSQL",
    "PostgreSQLWriter",
]
