"""Collection of aync data ingest classes."""

from __future__ import annotations

import asyncio
import re
import time
from datetime import datetime
from functools import cached_property
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Dict,
    List,
    Optional,
    Tuple,
    TypeAlias,
)
from urllib.parse import ParseResult, parse_qs, urlencode, urlparse, urlunparse

from pymongo import AsyncMongoClient, DeleteMany, UpdateOne

from ..api.cli import cli_function, cli_parameter
from ..api.index import BaseIndex
from ..logger import logger

if TYPE_CHECKING:
    from pymongo.asynchronous.collection import AsyncCollection
    from pymongo.asynchronous.database import AsyncDatabase
    from pymongo.typings import _DocumentType


MetadataRecord: TypeAlias = Dict[str, Any]
"""A single metadata record: key -> value of heterogeneous types."""


class MongoIndex(BaseIndex):
    """Ingest metadata into a mongoDB server."""

    def __post_init__(self) -> None:
        self._raw_uri = ""
        self._url = ""
        self._client: Optional[AsyncMongoClient[MetadataRecord]] = None

    @property
    def uri(self) -> str:
        """Create the connection uri for the mongoDB."""
        if self._url:
            return self._url
        parsed_url = urlparse(self._raw_uri)
        query = parse_qs(parsed_url.query)
        if "timeout" not in parsed_url.query.lower():
            query["timeoutMS"] = ["5000"]
        new_query = urlencode(query, doseq=True)
        self._url = urlunparse(
            ParseResult(
                parsed_url.scheme or "mongodb",
                parsed_url.netloc,
                parsed_url.path.rstrip("/"),
                parsed_url.params,
                new_query,
                parsed_url.fragment,
            )
        )
        return self._url

    @cached_property
    def unique_index(self) -> str:
        """Get the index."""
        for name, schema in self.index_schema.items():
            if schema.unique:
                return name
        raise ValueError("The schema doesn't define a unique value.")

    @property
    def client(self) -> AsyncMongoClient[MetadataRecord]:
        """Get the mongoDB client."""
        if self._client is None:
            logger.debug("Creating async mongoDB client: %s", self.uri)
            self._client = AsyncMongoClient(self.uri)
        return self._client

    async def _bulk_upsert(
        self, chunk: List[Dict[str, Any]], collection: "AsyncCollection[_DocumentType]"
    ) -> None:
        ops = [
            UpdateOne(
                {self.unique_index: m[self.unique_index]},
                {"$set": m},
                upsert=True,
            )
            for m in chunk
        ]
        await collection.bulk_write(ops, ordered=False)

    async def _index_collection(
        self, db: "AsyncDatabase[MetadataRecord]", collection: str, suffix: str = ""
    ) -> None:
        """Index a collection."""
        col = collection + suffix
        await db[col].create_index(self.unique_index, unique=True)
        async for chunk in self.get_metadata(collection):
            await self._bulk_upsert(chunk, db[col])

    async def _prep_db_connection(
        self, database: str, url: str
    ) -> "AsyncDatabase[MetadataRecord]":

        await self.close()
        self._raw_uri = url or ""
        return self.client[database]

    @cli_function(
        help="Add metadata to the mongoDB metadata server.",
    )
    async def index(
        self,
        *,
        url: Annotated[
            Optional[str],
            cli_parameter(
                "--url",
                help="The <host>:<port> to the mngoDB server",
                type=str,
            ),
        ] = None,
        database: Annotated[
            str,
            cli_parameter(
                "--database",
                "--db",
                help="The DB name holding the metadata.",
                type=str,
                default="metadata",
            ),
        ] = "metadata",
        index_suffix: Annotated[
            Optional[str],
            cli_parameter(
                "--index-suffix",
                help="Suffix for the latest and all version collections.",
                type=str,
            ),
        ] = None,
        rotate: Annotated[
            bool,
            cli_parameter(
                "--rotate",
                "--blue-green",
                action="store_true",
                help=(
                    "Blue/green deploy: index into fresh collections, then "
                    "atomically rename them onto the live collections, dropping "
                    "the old ones."
                ),
            ),
        ] = False,
        min_docs: Annotated[
            int,
            cli_parameter(
                "--min-docs",
                help=(
                    "Abort the rotation (dropping the new collections) if a "
                    "freshly indexed collection holds fewer than this many "
                    "documents."
                ),
                type=int,
            ),
        ] = 1,
    ) -> None:
        """Add metadata to the mongoDB metadata server."""
        db = await self._prep_db_connection(database, url or "")
        suffix = index_suffix or ""
        if rotate and not suffix:
            suffix = datetime.now().strftime("_%Y%m%dT%H%M%S%f")
        async with asyncio.TaskGroup() as tg:
            for collection in self.index_names:
                tg.create_task(
                    self._index_collection(db, collection, suffix=suffix)
                )
        if rotate:
            await self._rotate(db, suffix, min_docs)

    async def _rotate(
        self, db: "AsyncDatabase[MetadataRecord]", suffix: str, min_docs: int
    ) -> None:
        """Validate freshly indexed collections and promote them atomically.

        Both new collections are validated *before* any rename, so a bad index
        leaves the live collections untouched. ``renameCollection`` with
        ``dropTarget=True`` swaps a new collection onto its live name and drops
        the old data in a single atomic step (the unique index is carried over
        with the rename). The renames happen one collection at a time; a failure
        between them leaves a mixed state, which is surfaced as an error.

        The document counts use ``estimated_document_count`` (O(1) collection
        metadata); it is exact around zero and only approximate for very large
        collections, which is sufficient for a post-index health gate.
        """
        counts = {
            collection: await db[collection + suffix].estimated_document_count()
            for collection in self.index_names
        }
        logger.info("Indexed docs per new collection: %s", counts)
        if any(n < min_docs for n in counts.values()):
            for collection in self.index_names:
                await db[collection + suffix].drop()
            raise SystemExit(
                f"Rotation aborted: doc counts {counts} below "
                f"--min-docs={min_docs}; new collections dropped, "
                "live collections untouched."
            )
        for collection in self.index_names:
            await db[collection + suffix].rename(collection, dropTarget=True)
            logger.info("Promoted %s -> %s", collection + suffix, collection)

    async def close(self) -> None:
        """Close the mongoDB connection."""
        self._client.close() if self._client is not None else None
        self._url = ""
        self._raw_uri = ""

    @cli_function(
        help="Remove metadata from the mongoDB metadata server.",
    )
    async def delete(
        self,
        *,
        url: Annotated[
            Optional[str],
            cli_parameter(
                "--url",
                help="The <host>:<port> to the mngoDB server",
                type=str,
            ),
        ] = None,
        database: Annotated[
            str,
            cli_parameter(
                "--database",
                "--db",
                help="The DB name holding the metadata.",
                type=str,
                default="metadata",
            ),
        ] = "metadata",
        facets: Annotated[
            Optional[List[Tuple[str, str]]],
            cli_parameter(
                "-f",
                "--facets",
                type=str,
                nargs=2,
                action="append",
                help="Search facets matching the delete query.",
            ),
        ] = None,
    ) -> None:
        """Remove metadata from the mongoDB metadata server."""
        db = await self._prep_db_connection(database, url or "")
        if not facets:
            logger.info("Nothing to delete")
            return

        def glob_to_regex(glob: str) -> str:
            """Turn a shell‐style glob into a anchored mongo regex."""
            # escape everything, then un-escape our wildcards
            esc = re.escape(glob)
            esc = esc.replace(r"\*", ".*").replace(r"\?", ".")
            return f"^{esc}$"

        ops: List[DeleteMany] = []
        for field, val in facets:
            if "*" in val or "?" in val:
                pattern = glob_to_regex(val)
                ops.append(DeleteMany({field: {"$regex": pattern}}))
            else:
                ops.append(DeleteMany({field: val}))
        logger.debug("Deleting entries matching %s", ops)
        for collection in await db.list_collection_names():
            await db[collection].bulk_write(ops, ordered=False)
