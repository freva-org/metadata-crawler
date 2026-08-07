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
        self.failed_batches: int = 0
        self.posted_batches: int = 0
        self.first_error: Optional[str] = None
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

    async def _core_status(self, session: aiohttp.ClientSession) -> Dict[str, Any]:
        """Return the STATUS map of *every* core known to the container."""
        url = f"{self._uri}/admin/cores?action=STATUS"
        async with session.get(url) as resp:
            if resp.status >= 400:
                raise RuntimeError(
                    f"Core STATUS failed ({resp.status}): {await resp.text()}"
                )
            data = orjson.loads(await resp.read())
        return cast(Dict[str, Any], data.get("status", {}))

    @staticmethod
    def _instance_dir_name(instance_dir: Any) -> str:
        """Basename of an ``instanceDir`` as reported by Solr."""
        return os.path.basename(str(instance_dir or "").rstrip("/\\"))

    @staticmethod
    def _dir_owner(status: Dict[str, Any], core: str) -> Optional[str]:
        """Name of the core occupying ``<solr.home>/<core>``, if any.

        Solr derives the instance directory of a new core from its *name*, so
        a directory can be occupied by a core that is registered under a
        different name. That is exactly what a completed rotation leaves
        behind: ``SWAP`` renames the cores but not their directories, so the
        live core ends up owning ``<live>_<suffix>``.
        """
        for name, info in status.items():
            if SolrIndex._instance_dir_name(info.get("instanceDir")) == core:
                return cast(str, info.get("name") or name)
        return None

    async def _core_exists(self, session: aiohttp.ClientSession, core: str) -> bool:
        """Return ``True`` if a core of that name is currently loaded."""
        status = await self._core_status(session)
        return bool(status.get(core, {}).get("name"))

    async def _create_core(
        self, session: aiohttp.ClientSession, core: str, configset: str
    ) -> bool:
        """Create an (empty) core from ``configset`` unless it already exists.

        Returns ``True`` if this call created the core, so the caller knows
        which cores it owns and has to clean up if the run aborts.

        Both the name and the instance directory are checked from a single
        STATUS response. Checking only the name (as a plain ``STATUS&core=``
        does) disagrees with what CREATE enforces and turns a reused suffix
        into an opaque HTTP 400 half an hour into the run.
        """
        status = await self._core_status(session)
        if status.get(core, {}).get("name"):
            logger.debug("Core %s already exists, not creating", core)
            return False
        owner = self._dir_owner(status, core)
        if owner is not None:
            raise RuntimeError(
                f"Refusing to create core {core!r}: its instance directory is "
                f"already owned by core {owner!r}. The index suffix has "
                "already been rotated into production; a suffix must be "
                "unique per rotation (use a fresh --index-suffix, or none at "
                "all and let one be generated)."
            )
        url = f"{self._uri}/admin/cores?action=CREATE&name={core}&configSet={configset}"
        async with session.get(url) as resp:
            if resp.status >= 400:
                raise RuntimeError(
                    f"CREATE {core} failed ({resp.status}): {await resp.text()}"
                )
        logger.info("Created core %s (configSet=%s)", core, configset)
        return True

    async def _drop_cores(self, cores: List[str]) -> None:
        """Unload cores this run created, skipping any that are already gone.

        Called when a run aborts after core creation. Without it a failed run
        leaves its instance directories behind, and the next run that reaches
        for the same suffix collides with them.
        """
        if not cores:
            return
        async with aiohttp.ClientSession(timeout=self.timeout) as admin:
            status = await self._core_status(admin)
            for core in cores:
                if status.get(core, {}).get("name"):
                    logger.info("Dropping core %s after a failed run", core)
                    await self._unload_core(admin, core)

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
                # A failed commit means the documents are not searchable and
                # the doc counts the rotation gate is about to read are
                # meaningless. Never continue past this.
                raise RuntimeError(
                    f"COMMIT {core} failed ({resp.status}): {await resp.text()}"
                )

    async def _count_docs(self, session: aiohttp.ClientSession, core: str) -> int:
        """Return the number of documents in ``core``.

        An unreachable core raises rather than counting as empty: "somebody
        swapped my core away" and "I indexed nothing" need different answers,
        and reporting both as ``0`` sends the rotation gate to blame the doc
        count for what is actually a missing core.
        """
        url = f"{self._uri}/{core}/select?q=*:*&rows=0&wt=json"
        async with session.get(url) as resp:
            if resp.status >= 400:
                raise RuntimeError(
                    f"Cannot count documents in {core} ({resp.status}): "
                    f"{await resp.text()}"
                )
            data = orjson.loads(await resp.read())
        return int(data.get("response", {}).get("numFound", 0))

    async def _unload_core(self, session: aiohttp.ClientSession, core: str) -> None:
        """Unload ``core`` and delete its instance dir (best effort)."""
        url = (
            f"{self._uri}/admin/cores?action=UNLOAD&core={core}&deleteInstanceDir=true"
        )
        async with session.get(url) as resp:
            if resp.status >= 400:
                # Not fatal on its own, but the instance directory is now an
                # orphan that will make a later CREATE of the same name fail.
                logger.error(
                    "UNLOAD %s -> %i: %s. Its instance directory may be left "
                    "behind and will collide with a future core of that name.",
                    core,
                    resp.status,
                    await resp.text(),
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
        """POST one batch, recording - never swallowing - any failure."""
        status = 500
        t0 = time.perf_counter()
        try:
            async with session.post(
                url, data=body, headers={"Content-Type": "application/json"}
            ) as resp:
                status = resp.status
                payload = await resp.read()
            if status >= 400:
                self._record_failure(
                    f"POST {url} -> {status}: {payload.decode('utf-8', 'replace')}"
                )
                return
        except Exception as error:
            self._record_failure(f"POST {url} raised {error!r}")
            logger.log(
                logging.WARNING,
                error,
                exc_info=logger.level < logging.INFO,
            )
            return
        self.posted_batches += 1
        logger.debug(
            "POST %s -> %i (index time: %.3f)",
            url,
            status,
            time.perf_counter() - t0,
        )

    def _record_failure(self, message: str) -> None:
        """Book a rejected batch so ``index`` can refuse to rotate."""
        self.failed_batches += 1
        if self.first_error is None:
            self.first_error = message
        logger.warning("Batch rejected by solr: %s", message)

    async def consumer(self, session: aiohttp.ClientSession) -> None:
        """Consume the metadata read by the porducers.

        A consumer only ever leaves this loop on its sentinel. Letting one die
        early would strand a sentinel in the queue and block the drain, and
        would deadlock the producers once the bounded queue fills up, so
        unexpected errors are booked as failed batches and the worker carries
        on.
        """
        while True:
            update_url, body = await self.producer_queue.get()
            if body is self.senteniel:
                self.producer_queue.task_done()
                break
            try:
                await self._post_chunk(session, update_url, cast(bytes, body))
            except Exception as error:  # pragma: no cover - defensive
                self._record_failure(f"consumer crashed on {update_url}: {error!r}")
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
        max_failed_batches: Annotated[
            int,
            cli_parameter(
                "--max-failed-batches",
                help=(
                    "Number of batches solr may reject before the run is "
                    "considered failed. The default of 0 means any rejected "
                    "batch aborts before anything is committed or rotated."
                ),
                type=int,
            ),
        ] = 0,
    ) -> None:
        """Add metadata to the apache solr metadata server."""
        server = server or ""
        suffix = index_suffix or ""
        if rotate and not suffix:
            suffix = datetime.now().strftime("_%Y%m%dT%H%M%S%f")
        if rotate and index_suffix:
            logger.warning(
                "Rotating into an explicitly given index suffix (%s). The "
                "suffix must be unique per rotation: reusing it after a "
                "successful rotation, or sharing it with a concurrent run "
                "against the same solr, will collide on the core instance "
                "directory.",
                index_suffix,
            )
        created: List[str] = []
        if rotate:
            self._ensure_uri(server)
            async with aiohttp.ClientSession(timeout=self.timeout) as admin:
                for core in self.index_names:
                    if await self._create_core(admin, core + suffix, configset):
                        created.append(core + suffix)
        try:
            await self._index(server, suffix, http_workers, max_failed_batches)
            if rotate:
                await self._rotate(suffix, min_docs)
        except BaseException:
            # Anything from here on leaves the live cores as they were, so the
            # only thing to tidy up is what this run created.
            await self._drop_cores(created)
            raise

    async def _index(
        self,
        server: str,
        suffix: str,
        http_workers: int,
        max_failed_batches: int,
    ) -> None:
        """Stream every store into ``<core><suffix>`` and commit the result."""
        async with aiohttp.ClientSession(
            timeout=self.timeout, connector=self.connector
        ) as session:
            # NB: no raise_for_status here. Every response is inspected
            # explicitly so solr's error body (which is what tells you about
            # schema drift) makes it into the log rather than being reduced to
            # an exception type.
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
            # Awaiting the consumers (rather than ``queue.join()``) drains the
            # queue just the same - a consumer only stops at its sentinel, and
            # the sentinels are queued behind every batch - but it cannot hang
            # if a worker went away.
            await asyncio.gather(*consumers)
            self._check_write_failures(max_failed_batches)
            # Every batch is now POSTed; commit each core so the documents are
            # actually visible (and warmed) before we count / flip.
            for core in self.index_names:
                await self._commit(session, core + suffix)
            async with aiohttp.ClientSession(timeout=self.timeout) as admin:
                for core in self.index_names:
                    logger.info(
                        "Core %s holds %i documents",
                        core + suffix,
                        await self._count_docs(admin, core + suffix),
                    )

    def _check_write_failures(self, max_failed_batches: int) -> None:
        """Abort if solr rejected more batches than we are willing to lose.

        The item counts reported while reading come from the metadata stores,
        not from solr, so without this gate a run in which every single POST
        was rejected still reports hundreds of thousands of "indexed" items
        and only shows up as an empty core at rotation time.
        """
        logger.info(
            "Posted %i batches, %i rejected", self.posted_batches, self.failed_batches
        )
        if self.failed_batches > max_failed_batches:
            raise RuntimeError(
                f"solr rejected {self.failed_batches} of "
                f"{self.failed_batches + self.posted_batches} batches "
                f"(--max-failed-batches={max_failed_batches}); nothing was "
                f"committed or rotated. First error: {self.first_error}"
            )

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
            flipped: List[str] = []
            try:
                for core in self.index_names:
                    await self._flip_core(admin, core, core + suffix)
                    flipped.append(core)
            except Exception:
                pending = [c for c in self.index_names if c not in flipped]
                logger.error(
                    "Rotation left a mixed state: %s now serve the new index, "
                    "%s still serve the previous one. Re-run with a fresh "
                    "--index-suffix once the cause is fixed.",
                    ", ".join(flipped) or "no cores",
                    ", ".join(pending),
                )
                raise
            logger.info(
                "Blue/green rotation complete; live cores: %s",
                ", ".join(self.index_names),
            )
