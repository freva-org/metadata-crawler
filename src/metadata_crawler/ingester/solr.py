"""Collection of aync data ingest classes."""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Annotated, Any, Dict, List, Optional

import aiohttp

from ..api.cli import cli_function, cli_parameter
from ..api.index import BaseIndex
from ..api.metadata_stores import IndexName
from ..logger import logger


class SolrIndex(BaseIndex):
    """Ingest metadata into an apache solr server."""

    def __post_init__(self) -> None:
        self.timeout = aiohttp.ClientTimeout(total=50)
        self._uri: str = ""

    async def solr_url(self, server: str, core: str) -> str:
        """Construct the solr url from a given solr core."""
        if not self._uri:
            scheme, _, server = server.rpartition("://")
            scheme = scheme or "http"
            solr_server, _, solr_port = server.partition(":")
            solr_port = solr_port or "8983"
            solr_server = solr_server or "localhost"
            self._uri = f"{scheme}://{solr_server}:{solr_port}/solr"
        return f"{self._uri}/{core}/update/json?commit=true"

    @cli_function(
        help="Remove metadata from the apache solr server.",
    )
    async def delete(
        self,
        *,
        server: Annotated[
            Optional[str],
            cli_parameter(
                "-sv",
                "--server",
                help="The <host>:<port> to the solr server",
                type=str,
            ),
        ] = None,
        facets: Annotated[
            Optional[List[tuple[str, str]]],
            cli_parameter(
                "-f",
                "--facets",
                type=str,
                nargs=2,
                action="append",
                help="Search facets matching the delete query.",
            ),
        ] = None,
        latest_version: Annotated[
            str,
            cli_parameter(
                "--latest-version",
                type=str,
                help="Name of the core holding 'latest' metadata.",
            ),
        ] = IndexName().latest,
        all_versions: Annotated[
            str,
            cli_parameter(
                "--all-versions",
                type=str,
                help="Name of the core holding 'all' metadata versions.",
            ),
        ] = IndexName().all,
    ) -> None:
        """Remove metadata from the apache solr server."""
        query = []
        for key, value in facets or []:
            if key.lower() == "file":
                if value[0] in (os.sep, "/"):
                    value = f"\\{value}"
                value = value.replace(":", "\\:")
            else:
                value = value.lower()
            query.append(f"{key.lower()}:{value}")
        query_str = " AND ".join(query)
        server = server or ""
        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            logger.debug("Deleting entries matching %s", query_str)
            for core in (all_versions, latest_version):
                url = await self.solr_url(server, core)
                async with session.post(
                    url, json={"delete": {"query": query_str}}
                ) as resp:
                    level = (
                        logging.WARNING
                        if resp.status not in (200, 201)
                        else logging.DEBUG
                    )
                    logger.log(level, await resp.text())

    def _convert(self, metadata: Dict[str, Any]) -> Dict[str, Any]:
        for k, v in metadata.items():
            match self.index_schema[k].type:
                case "bbox":
                    metadata[k] = f"ENVELOPE({v[0]}, {v[1]}, {v[3]}, {v[2]})"
                case "daterange":
                    metadata[k] = f"[{v[0].isoformat()} TO {v[-1].isoformat()}]"

        return metadata

    async def _index_core(self, server: str, core: str, suffix: str) -> None:
        """Index data to a solr core."""
        url = await self.solr_url(server, core + suffix)
        async for chunk in self.get_metadata(core):
            async with aiohttp.ClientSession(
                timeout=self.timeout, raise_for_status=True
            ) as session:
                try:
                    payload = list(map(self._convert, chunk))
                    async with session.post(url, json=payload) as resp:
                        logger.debug(await resp.text())
                except Exception as error:
                    logger.log(
                        logging.WARNING,
                        error,
                        exc_info=logger.level < logging.INFO,
                    )

    @cli_function(
        help="Add metadata to the apache solr metadata server.",
    )
    async def index(
        self,
        *,
        server: Annotated[
            Optional[str],
            cli_parameter(
                "-sv",
                "--server",
                help="The <host>:<port> to the solr server",
                type=str,
            ),
        ] = None,
        index_suffix: Annotated[
            Optional[str],
            cli_parameter(
                "--index-suffix",
                help="Suffix for the latest and all version collections.",
                type=str,
            ),
        ] = None,
    ) -> None:
        """Add metadata to the apache solr metadata server."""
        async with asyncio.TaskGroup() as tg:
            for core in self.index_names:
                tg.create_task(
                    self._index_core(
                        server or "", core, suffix=index_suffix or ""
                    )
                )
