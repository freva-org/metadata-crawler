"""Collection of aync data ingest classes."""

from __future__ import annotations

import asyncio
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Annotated, Any, Dict, List, Optional, cast

import aiohttp
import orjson

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

    def _encode_payload(self, chunk: List[Dict[str, Any]]) -> bytes:
        """CPU-bound: convert docs and JSON-encode off the event loop."""
        return orjson.dumps([self._convert(x) for x in chunk])

    async def _post_chunk(
        self,
        session: aiohttp.ClientSession,
        url: str,
        body: bytes,
    ) -> None:
        """POST one batch with minimal overhead and simple retries."""
        status = 500
        t0 = time.perf_counter()
        try:
            async with session.post(
                url, data=body, headers={"Content-Type": "application/json"}
            ) as resp:
                status = resp.status
                await resp.read()

        except Exception as error:
            logger.log(
                logging.WARNING,
                error,
                exc_info=logger.level < logging.INFO,
            )
            return
        logger.debug(
            "POST %s -> %i (index time: %.3f)",
            url,
            status,
            time.perf_counter() - t0,
        )

    async def _index_core(
        self, server: str, core: str, suffix: str, http_workers: int = 8
    ) -> None:
        """Zero-copy-ish, backpressured, bounded-concurrency indexer.

        - No per-batch commit.
        - Bounded queue so tasks don't pile up.
        - Constant number of worker tasks (not O(batches)).
        """
        base_url = await self.solr_url(server, core + suffix)
        update_url = base_url.split("?", 1)[0]  # guard

        queue_max: int = 128
        encode_workers: int = 4

        timeout = aiohttp.ClientTimeout(
            connect=10, sock_connect=10, sock_read=180, total=None
        )
        connector = aiohttp.TCPConnector(
            limit_per_host=http_workers,
            ttl_dns_cache=300,
            enable_cleanup_closed=True,
        )

        loop = asyncio.get_running_loop()
        cpu_pool = ThreadPoolExecutor(max_workers=encode_workers)
        q: asyncio.Queue[Optional[bytes]] = asyncio.Queue(maxsize=queue_max)
        SENTINEL: Optional[bytes] = None

        async def producer() -> None:
            async for batch in self.get_metadata(core):
                body = await loop.run_in_executor(
                    cpu_pool, self._encode_payload, batch
                )
                await q.put(body)
            for _ in range(http_workers):
                await q.put(SENTINEL)

        async def consumer(
            worker_id: int, session: aiohttp.ClientSession
        ) -> None:
            while True:
                body = await q.get()
                if body is SENTINEL:
                    q.task_done()
                    break
                try:
                    await self._post_chunk(session, update_url, cast(bytes, body))
                finally:
                    q.task_done()

        async with aiohttp.ClientSession(
            timeout=timeout, connector=connector, raise_for_status=True
        ) as session:
            consumers = [
                asyncio.create_task(consumer(i, session))
                for i in range(http_workers)
            ]
            prod_task = asyncio.create_task(producer())
            await prod_task
            await q.join()
            await asyncio.gather(*consumers)

        commit_url = f"{update_url}?commit=true"
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                commit_url,
                data=b"[]",
                headers={"Content-Type": "application/json"},
            ) as resp:
                if resp.status >= 400:
                    text = await resp.text()
                    logger.warning(
                        "COMMIT %s -> %i: %s", commit_url, resp.status, text
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
        http_workers: Annotated[
            int,
            cli_parameter(
                "--http-workers", help="Number of ingestion threads.", type=int
            ),
        ] = 8,
    ) -> None:
        """Add metadata to the apache solr metadata server."""
        async with asyncio.TaskGroup() as tg:
            for core in self.index_names:
                tg.create_task(
                    self._index_core(
                        server or "",
                        core,
                        suffix=index_suffix or "",
                        http_workers=http_workers,
                    )
                )
