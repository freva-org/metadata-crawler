"""Collection of aync data ingest classes."""

from __future__ import annotations

import os
from collections import defaultdict
from typing import Annotated, Any, Optional

import aiohttp

from ..api.cli import cli_function, cli_parameter
from ..logger import logger
from .base import BaseIngest


class SolrIngest(BaseIngest):
    """Ingest metadata into an apache solr server."""

    def __init__(
        self,
        catalogue_file: str,
        batch_size: int = 2500,
    ) -> None:
        super().__init__(catalogue_file=catalogue_file, batch_size=batch_size)
        self.core = "files"
        self.latest = "latest"

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
            str,
            cli_parameter(
                "-s",
                "--server",
                help="The <host>:<port> to the solr server",
                type=str,
            ),
        ] = None,
        facets: Annotated[
            list[tuple[str, str]],
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
        query = []
        for key, value in facets.items():
            if key.lower() == "file":
                if value[0] in (os.sep, "/"):
                    value = f"\\{value}"
                value = value.replace(":", "\\:")
            else:
                value = value.lower()
            query.append(f"{key.lower()}:{value}")
        query_str = " AND ".join(query)
        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            logger.debug("Deleting entries matching %s", query_str)
            for core in (self.core, self.latest):
                url = await self.solr_url(core)
                async with session.post(
                    url, json={"delete": {"query": query_str}}
                ) as resp:
                    if resp.status not in (200, 201):
                        logger.debug(await resp.text())

                        logger.error(
                            "Status failed with: %i",
                            resp.status,
                        )

    @cli_function(
        help="Add metadata to the apache solr metadata server.",
    )
    async def index(
        self,
        *,
        server: Annotated[
            str,
            cli_parameter(
                "-s",
                "--server",
                help="The <host>:<port> to the solr server",
                type=str,
            ),
        ] = None,
    ) -> None:
        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            for core in self.cores:
                async for chunk in self.get_metadata(core):
                    url = await self.solr_url(server, core)
                    # adopt the bbox field for Solr
                    values = [
                        {
                            k: (
                                f"ENVELOPE({v[0]}, {v[1]}, {v[3]}, {v[2]})"
                                if k == "bbox"
                                else v
                            )
                            for k, v in entry.items()
                        }
                        for entry in chunk
                    ]
                    logger.debug("Sending %i entries to %s", len(values), url)
                    async with session.post(url, json=values) as resp:
                        if resp.status not in (200, 201):
                            logger.error(await resp.text())
                            raise ValueError(
                                f"Solr Ingest failed with: {resp.status} in {core}"
                            )
