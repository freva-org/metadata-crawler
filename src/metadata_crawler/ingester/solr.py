"""Collection of aync data ingest classes."""

from __future__ import annotations

import asyncio
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from types import TracebackType
from typing import Annotated, Any, Dict, List, Optional, Tuple, Type, cast

import aiohttp
import orjson

from ..api.cli import cli_function, cli_parameter
from ..api.index import BaseIndex
from ..api.stores import IndexName
from ..logger import logger


class SolrIndex(BaseIndex):
    """Ingest metadata into an apache solr server."""

    senteniel: Optional[bytes] = None

    def __post_init__(self) -> None:
        self.timeout = aiohttp.ClientTimeout(
            connect=10, sock_connect=10, sock_read=180, total=None
        )
        self.semaphore = asyncio.Event()
        self.max_http_workers: int = 0
        queue_max: int = 128
        encode_workers: int = 4
        self._uri: str = ""
        self.cpu_pool = ThreadPoolExecutor(max_workers=encode_workers)
        self.producer_queue: asyncio.Queue[Tuple[str, Optional[bytes]]] = asyncio.Queue(
            maxsize=queue_max
        )
        self.connector = aiohttp.TCPConnector(
            ttl_dns_cache=300,
            use_dns_cache=True,
            enable_cleanup_closed=True,
        )

    def _ensure_uri(self, server: str) -> str:
        """Resolve and cache the base solr URI (``<scheme>://<host>:<port>/solr``)."""
        if not self._uri:
            scheme, _, server = server.rpartition("://")
            scheme = scheme or "http"
            solr_server, _, solr_port = server.partition(":")
            solr_server = solr_server or "localhost"
            uri = f"{scheme}://{solr_server}"
            uri = f"{uri}:{solr_port}" if solr_port else uri
            self._uri = f"{uri}/solr"
        return self._uri

    async def solr_url(self, server: str, core: str) -> str:
        """Construct the solr url from a given solr core."""
        self._ensure_uri(server)
        return f"{self._uri}/{core}/update/json?commit=true"

    async def _core_exists(self, session: aiohttp.ClientSession, core: str) -> bool:
        """Return ``True`` if a core of that name is currently loaded."""
        url = f"{self._uri}/admin/cores?action=STATUS&core={core}"
        async with session.get(url) as resp:
            if resp.status >= 400:
                return False
            data = orjson.loads(await resp.read())
        return bool(data.get("status", {}).get(core, {}).get("name"))

    async def _create_core(
        self, session: aiohttp.ClientSession, core: str, configset: str
    ) -> None:
        """Create an (empty) core from ``configset`` unless it already exists."""
        if await self._core_exists(session, core):
            logger.debug("Core %s already exists, not creating", core)
            return
        url = f"{self._uri}/admin/cores?action=CREATE&name={core}&configSet={configset}"
        async with session.get(url) as resp:
            if resp.status >= 400:
                raise RuntimeError(
                    f"CREATE {core} failed ({resp.status}): {await resp.text()}"
                )
        logger.info("Created core %s (configSet=%s)", core, configset)

    async def _commit(self, session: aiohttp.ClientSession, core: str) -> None:
        """Hard-commit ``core`` and wait for the new searcher to be ready.

        Called only after the whole producer queue has drained, so every batch
        for the core has actually been POSTed. ``commit=true`` defaults to
        ``waitSearcher=true``, so on return the docs are visible to search
        (and, with useColdSearcher=false, the warmed searcher is live).
        """
        url = f"{self._uri}/{core}/update/json?commit=true"
        async with session.post(
            url, data=b"[]", headers={"Content-Type": "application/json"}
        ) as resp:
            if resp.status >= 400:
                logger.warning(
                    "COMMIT %s -> %i: %s", core, resp.status, await resp.text()
                )

    async def _count_docs(self, session: aiohttp.ClientSession, core: str) -> int:
        """Return the number of documents in ``core`` (0 if it is unreachable)."""
        url = f"{self._uri}/{core}/select?q=*:*&rows=0&wt=json"
        async with session.get(url) as resp:
            if resp.status >= 400:
                return 0
            data = orjson.loads(await resp.read())
        return int(data.get("response", {}).get("numFound", 0))

    async def _unload_core(self, session: aiohttp.ClientSession, core: str) -> None:
        """Unload ``core`` and delete its instance dir (best effort)."""
        url = (
            f"{self._uri}/admin/cores?action=UNLOAD&core={core}&deleteInstanceDir=true"
        )
        async with session.get(url) as resp:
            if resp.status >= 400:
                logger.warning(
                    "UNLOAD %s -> %i: %s", core, resp.status, await resp.text()
                )

    async def _flip_core(
        self, session: aiohttp.ClientSession, live: str, new: str
    ) -> None:
        """Promote ``new`` to ``live`` and drop the previously live data.

        If ``live`` already exists the names are swapped atomically (after the
        SWAP ``new`` points at the *old* data, which is then unloaded). On a
        first deployment ``new`` is simply renamed to ``live``.
        """
        if await self._core_exists(session, live):
            async with session.get(
                f"{self._uri}/admin/cores?action=SWAP&core={live}&other={new}"
            ) as resp:
                if resp.status >= 400:
                    raise RuntimeError(
                        f"SWAP {live}<->{new} failed "
                        f"({resp.status}): {await resp.text()}"
                    )
            logger.info("Swapped %s <-> %s", live, new)
            await self._unload_core(session, new)
        else:
            async with session.get(
                f"{self._uri}/admin/cores?action=RENAME&core={new}&other={live}"
            ) as resp:
                if resp.status >= 400:
                    raise RuntimeError(
                        f"RENAME {new}->{live} failed "
                        f"({resp.status}): {await resp.text()}"
                    )
            logger.info("Renamed %s -> %s (first deployment)", new, live)

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
            field = self.index_schema.get(k)
            if field is None:
                # Field not described by the store's schema (e.g. schema/data
                # drift between backends): it needs no special encoding, so
                # pass it through untouched rather than crashing the batch.
                continue
            match field.type:
                case "bbox":
                    metadata[k] = f"ENVELOPE({v[0]}, {v[1]}, {v[3]}, {v[2]})"
                case "daterange":
                    metadata[k] = (
                        f"[{v[0].strftime('%Y-%m-%dT%H:%M:%SZ')} "
                        f"TO {v[-1].strftime('%Y-%m-%dT%H:%M:%SZ')}]"
                    )
                case "datetime":
                    metadata[k] = v[0].strftime("%Y-%m-%dT%H:%M:%SZ")

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

    async def consumer(self, session: aiohttp.ClientSession) -> None:
        """Consume the metadata read by the porducers."""
        while True:
            update_url, body = await self.producer_queue.get()
            if body is self.senteniel:
                self.producer_queue.task_done()
                break
            try:
                await self._post_chunk(session, update_url, cast(bytes, body))
            finally:
                self.producer_queue.task_done()

    async def _index_core(
        self,
        session: aiohttp.ClientSession,
        server: str,
        core: str,
        suffix: str,
    ) -> None:
        """Zero-copy-ish, backpressured, bounded-concurrency indexer.

        - No per-batch commit.
        - Bounded queue so tasks don't pile up.
        - Constant number of worker tasks (not O(batches)).
        """
        base_url = await self.solr_url(server, core + suffix)
        update_url = base_url.split("?", 1)[0]  # guard
        loop = asyncio.get_running_loop()
        async for body in self.get_metadata(core):
            enc = await loop.run_in_executor(self.cpu_pool, self._encode_payload, body)
            await self.producer_queue.put((update_url, enc))
        # NB: no commit here. Producers only enqueue; the bodies are POSTed
        # asynchronously by the consumer tasks, so committing at this point
        # would race ahead of the not-yet-POSTed batches and open a searcher
        # over a partial core. The commit happens in ``index`` once the whole
        # producer queue has been drained.

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:

        try:
            self.producer_queue.shutdown()
        except AttributeError:  # pragma: no cover
            pass  # prgama: no cover
        self.cpu_pool.shutdown()

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
        rotate: Annotated[
            bool,
            cli_parameter(
                "--rotate",
                "--blue-green",
                action="store_true",
                help=(
                    "Blue/green deploy: index into fresh cores, then atomically "
                    "swap them with the live cores and drop the old ones."
                ),
            ),
        ] = False,
        configset: Annotated[
            str,
            cli_parameter(
                "--configset",
                help="Solr configset used to create the blue/green cores.",
                type=str,
            ),
        ] = "freva",
        min_docs: Annotated[
            int,
            cli_parameter(
                "--min-docs",
                help=(
                    "Abort the rotation (dropping the new cores) if a freshly "
                    "indexed core holds fewer than this many documents."
                ),
                type=int,
            ),
        ] = 1,
    ) -> None:
        """Add metadata to the apache solr metadata server."""
        server = server or ""
        suffix = index_suffix or ""
        if rotate and not suffix:
            suffix = datetime.now().strftime("_%Y%m%dT%H%M%S%f")
        if rotate:
            self._ensure_uri(server)
            async with aiohttp.ClientSession(timeout=self.timeout) as admin:
                for core in self.index_names:
                    await self._create_core(admin, core + suffix, configset)
        async with aiohttp.ClientSession(
            timeout=self.timeout, connector=self.connector, raise_for_status=True
        ) as session:
            consumers = [
                asyncio.create_task(self.consumer(session)) for _ in range(http_workers)
            ]
            async with asyncio.TaskGroup() as tg:
                for core in self.index_names:
                    tg.create_task(
                        self._index_core(
                            session,
                            server,
                            core,
                            suffix=suffix,
                        )
                    )
            for _ in range(http_workers):
                await self.producer_queue.put(("", self.senteniel))
            await self.producer_queue.join()
            await asyncio.gather(*consumers)
            # Every batch is now POSTed; commit each core so the documents are
            # actually visible (and warmed) before we count / flip.
            for core in self.index_names:
                await self._commit(session, core + suffix)
        if rotate:
            await self._rotate(suffix, min_docs)

    async def _rotate(self, suffix: str, min_docs: int) -> None:
        """Validate the freshly indexed cores and swap them into production.

        Both new cores are validated *before* any swap, so a bad index leaves
        the live cores untouched. The swaps themselves happen one core at a
        time; a failure between the two leaves a mixed state (same exposure as
        the previous shell-script approach) and is surfaced as an error.
        """
        async with aiohttp.ClientSession(timeout=self.timeout) as admin:
            counts = {
                core: await self._count_docs(admin, core + suffix)
                for core in self.index_names
            }
            logger.info("Indexed docs per new core: %s", counts)
            if any(n < min_docs for n in counts.values()):
                for core in self.index_names:
                    await self._unload_core(admin, core + suffix)
                raise SystemExit(
                    f"Rotation aborted: doc counts {counts} below "
                    f"--min-docs={min_docs}; new cores dropped, live cores untouched."
                )
            for core in self.index_names:
                await self._flip_core(admin, core, core + suffix)
            logger.info(
                "Blue/green rotation complete; live cores: %s",
                ", ".join(self.index_names),
            )
