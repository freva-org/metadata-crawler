"""Collection of aync data ingest classes."""

from __future__ import annotations

import re
from functools import cached_property
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import ParseResult, parse_qs, urlencode, urlparse, urlunparse

from motor.motor_asyncio import (
    AsyncIOMotorClient,
    AsyncIOMotorCollection,
    AsyncIOMotorDatabase,
)
from pymongo import DeleteMany, UpdateOne

from ..api.cli import Annotated, cli_function, cli_parameter
from ..api.index import BaseIndex
from ..logger import logger


class MongoIndex(BaseIndex):
    """Ingest metadata into a mongoDB server."""

    def __init__(
        self,
        catalogue_file: str,
        batch_size: int = 2500,
        **kwargs: str,
    ) -> None:
        super().__init__(catalogue_file, batch_size)
        self._raw_uri = ""
        self._url = ""
        self._client: Optional[AsyncIOMotorClient] = None

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
    def client(self) -> AsyncIOMotorClient:
        """Get the mongoDB client."""
        if self._client is None:
            logger.debug("Creating async mongoDB client: %s", self.uri)
            self._client = AsyncIOMotorClient(self.uri)
        return self._client

    async def _bulk_upsert(
        self, chunk: List[Dict[str, Any]], collection: AsyncIOMotorCollection
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

    async def _prep_db_connection(
        self, database: str, url: str
    ) -> AsyncIOMotorDatabase:

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
    ) -> None:
        db = await self._prep_db_connection(database, url)
        for collection in self.index_names:
            db[collection].create_index(self.unique_index, unique=True)
            async for chunk in self.get_metadata(collection):
                await self._bulk_upsert(chunk, db[collection])

    async def close(self) -> None:
        """Close the mongoDB connection."""
        if self._client is not None:
            await self._client.close()
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
            List[Tuple[str, str]],
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
        db = await self._prep_db_connection(database, url)
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
