"""Blue/green rotation for Solr, exercised without a running Solr.

``fake_solr`` models the Core-Admin detail the rotation hinges on: a core is a
*name* pointing at an *instance directory*, ``SWAP`` exchanges the names but
not the directories, and ``CREATE`` derives the directory from the name. That
is enough to reproduce - and pin - the failure modes that only ever showed up
in production:

* a suffix that has already been rotated collides with the live core's
  instance directory, even though no core carries that *name* any more;
* every batch being rejected still reads the full store, so without a write
  gate the run reports hundreds of thousands of items and an empty core.

These tests need no external services, so they run in every environment.
"""

from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path
from typing import Any, Dict, Iterator, List, Tuple

import aiohttp
import pytest
from fake_solr import SolrState, serve

from metadata_crawler import delete, index

CATALOGUES = ("fs-cat.yml", "s3-cat.yml", "swift-cat.yml")
DATASETS = {"fs": "obs-fs", "s3": "obs-s3", "swift": "obs-swift"}
LIVE_DOC = {"file": "/live-before.nc", "uri": "file:///live-before.nc"}


# --------------------------------------------------------------------------- #
# fixtures
# --------------------------------------------------------------------------- #
@pytest.fixture()
def solr_state() -> SolrState:
    """A fake Solr holding two populated live cores."""
    state = SolrState()
    state.seed("latest", [dict(LIVE_DOC)])
    state.seed("files", [dict(LIVE_DOC)])
    return state


@pytest.fixture()
def empty_solr_state() -> SolrState:
    """A fake Solr with no cores at all (first deployment)."""
    return SolrState()


@pytest.fixture()
def fake_solr(solr_state: SolrState) -> Iterator[str]:
    yield from serve(solr_state)


@pytest.fixture()
def empty_fake_solr(empty_solr_state: SolrState) -> Iterator[str]:
    yield from serve(empty_solr_state)


@pytest.fixture()
def log_records() -> Iterator[List[logging.LogRecord]]:
    """Collect what the package logger emits.

    ``caplog`` cannot see it: the crawler's logger sets ``propagate = False``
    and manages its own handlers, so a handler has to be attached directly.
    """
    from metadata_crawler.logger import logger

    records: List[logging.LogRecord] = []

    class Collector(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    handler = Collector(level=logging.DEBUG)
    logger.addHandler(handler)
    try:
        yield records
    finally:
        logger.removeHandler(handler)


@pytest.fixture()
def in_mock_dir() -> Iterator[Path]:
    """The intake catalogues reference their data relatively."""
    cur_dir = os.getcwd()
    mock_dir = Path(__file__).parent / "mock_crawls"
    os.chdir(mock_dir)
    try:
        yield mock_dir
    finally:
        os.chdir(cur_dir)


def _rotate(server: str, *catalogues: str, **kwargs: Any) -> None:
    """``index --rotate`` with the arguments these tests share."""
    options: Dict[str, Any] = {
        "server": server,
        "batch_size": 20,
        "rotate": True,
        "configset": "freva",
        "min_docs": 1,
    }
    options.update(kwargs)
    index("solr", *(catalogues or ("fs-cat.yml",)), **options)


# --------------------------------------------------------------------------- #
# rotation
# --------------------------------------------------------------------------- #
class TestRotationCoversEveryStore:
    """One run over N stores must produce exactly one rotation."""

    def test_every_store_reaches_the_live_core(
        self, fake_solr: str, solr_state: SolrState, in_mock_dir: Path
    ) -> None:
        """All catalogues land in the same rotation, none is swapped away.

        Handing the stores to one ingester is what makes this true. Indexing
        them one at a time rotated after every store, so only the last one
        survived - when the run got that far at all.
        """
        _rotate(fake_solr, *CATALOGUES, index_suffix="_bg")

        live = solr_state.documents("latest")
        assert {doc["dataset"] for doc in live} == set(DATASETS.values())
        assert LIVE_DOC["file"] not in {doc["file"] for doc in live}

    def test_cores_are_created_once_each(
        self, fake_solr: str, solr_state: SolrState, in_mock_dir: Path
    ) -> None:
        """Three stores, still one CREATE and one SWAP per index name."""
        _rotate(fake_solr, *CATALOGUES, index_suffix="_bg")

        assert solr_state.created == ["latest_bg", "files_bg"]
        assert sorted(solr_state.cores) == ["files", "latest"]

    def test_swap_migrates_the_live_instance_dir(
        self, fake_solr: str, solr_state: SolrState, in_mock_dir: Path
    ) -> None:
        """After a SWAP the live core lives in the suffixed directory.

        This is not incidental - it is why reusing a suffix collides.
        """
        _rotate(fake_solr, "fs-cat.yml", index_suffix="_bg")

        assert solr_state.instance_dir("latest") == "latest_bg"
        assert solr_state.instance_dir("files") == "files_bg"


class TestReusedSuffix:
    """A suffix that was already rotated must be refused, and explained."""

    def test_second_rotation_with_the_same_suffix_is_refused(
        self, fake_solr: str, solr_state: SolrState, in_mock_dir: Path
    ) -> None:
        _rotate(fake_solr, "fs-cat.yml", index_suffix="_bg")

        with pytest.raises(RuntimeError, match="already owned by core 'latest'"):
            _rotate(fake_solr, "fs-cat.yml", index_suffix="_bg")

    def test_refusal_names_the_remedy(
        self, fake_solr: str, solr_state: SolrState, in_mock_dir: Path
    ) -> None:
        """The message has to point at the suffix, not at Solr internals."""
        _rotate(fake_solr, "fs-cat.yml", index_suffix="_bg")

        with pytest.raises(RuntimeError, match="--index-suffix"):
            _rotate(fake_solr, "fs-cat.yml", index_suffix="_bg")

    def test_refusal_leaves_the_live_cores_serving(
        self, fake_solr: str, solr_state: SolrState, in_mock_dir: Path
    ) -> None:
        _rotate(fake_solr, "fs-cat.yml", index_suffix="_bg")
        before = {core: solr_state.num_docs(core) for core in ("latest", "files")}

        with pytest.raises(RuntimeError):
            _rotate(fake_solr, "fs-cat.yml", index_suffix="_bg")

        after = {core: solr_state.num_docs(core) for core in ("latest", "files")}
        assert after == before
        assert sorted(solr_state.cores) == ["files", "latest"]

    def test_a_fresh_suffix_rotates_again(
        self, fake_solr: str, solr_state: SolrState, in_mock_dir: Path
    ) -> None:
        """Back-to-back rotations are fine as long as the suffix is new."""
        _rotate(fake_solr, "fs-cat.yml", index_suffix="_bg1")
        _rotate(fake_solr, "s3-cat.yml", index_suffix="_bg2")

        assert {doc["dataset"] for doc in solr_state.documents("latest")} == {"obs-s3"}
        assert solr_state.instance_dir("latest") == "latest_bg2"

    def test_generated_suffixes_do_not_collide(
        self, fake_solr: str, solr_state: SolrState, in_mock_dir: Path
    ) -> None:
        """Without --index-suffix the generated one is unique per run."""
        _rotate(fake_solr, "fs-cat.yml")
        _rotate(fake_solr, "fs-cat.yml")

        assert sorted(solr_state.cores) == ["files", "latest"]

    def test_explicit_suffix_warns(
        self,
        fake_solr: str,
        in_mock_dir: Path,
        log_records: List[logging.LogRecord],
    ) -> None:
        """Pinning a suffix is allowed but has to be flagged."""
        _rotate(fake_solr, "fs-cat.yml", index_suffix="_bg", verbosity=2)
        messages = [record.getMessage() for record in log_records]
        assert any("unique per rotation" in message for message in messages)


class TestFirstDeployment:
    """With no live cores the new ones are renamed rather than swapped."""

    def test_rename_promotes_the_new_cores(
        self, empty_fake_solr: str, empty_solr_state: SolrState, in_mock_dir: Path
    ) -> None:
        _rotate(empty_fake_solr, "fs-cat.yml", index_suffix="_bg")

        assert sorted(empty_solr_state.cores) == ["files", "latest"]
        assert empty_solr_state.num_docs("latest") > 0
        assert empty_solr_state.instance_dir("latest") == "latest_bg"


# --------------------------------------------------------------------------- #
# write failures
# --------------------------------------------------------------------------- #
class TestRejectedBatches:
    """Reading a store is not indexing it; rejected writes must abort."""

    def test_all_batches_rejected_aborts(
        self, fake_solr: str, solr_state: SolrState, in_mock_dir: Path
    ) -> None:
        """The run fails loudly instead of rotating an empty core in."""
        solr_state.reject_updates = 400

        with pytest.raises(RuntimeError, match="rejected"):
            _rotate(fake_solr, "fs-cat.yml", index_suffix="_bg")

    def test_abort_reports_solr_error_body(
        self, fake_solr: str, solr_state: SolrState, in_mock_dir: Path
    ) -> None:
        """Solr's message is what tells you it is schema drift."""
        solr_state.reject_updates = 400

        with pytest.raises(RuntimeError, match="unknown field"):
            _rotate(fake_solr, "fs-cat.yml", index_suffix="_bg")

    def test_abort_leaves_live_cores_untouched(
        self, fake_solr: str, solr_state: SolrState, in_mock_dir: Path
    ) -> None:
        solr_state.reject_updates = 400
        before = {core: solr_state.num_docs(core) for core in ("latest", "files")}

        with pytest.raises(RuntimeError):
            _rotate(fake_solr, "fs-cat.yml", index_suffix="_bg")

        after = {core: solr_state.num_docs(core) for core in ("latest", "files")}
        assert after == before

    def test_abort_drops_the_cores_it_created(
        self, fake_solr: str, solr_state: SolrState, in_mock_dir: Path
    ) -> None:
        """Otherwise a retry of the same suffix trips over the leftovers."""
        solr_state.reject_updates = 400

        with pytest.raises(RuntimeError):
            _rotate(fake_solr, "fs-cat.yml", index_suffix="_bg")

        assert sorted(solr_state.cores) == ["files", "latest"]

    def test_failures_within_tolerance_still_rotate(
        self, fake_solr: str, solr_state: SolrState, in_mock_dir: Path
    ) -> None:
        """--max-failed-batches is what makes a transient blip survivable."""
        solr_state.reject_updates = 400
        solr_state.reject_limit = 1

        _rotate(
            fake_solr, "fs-cat.yml", index_suffix="_bg", max_failed_batches=1
        )

        assert solr_state.num_docs("latest") > 0

    def test_failures_over_tolerance_abort(
        self, fake_solr: str, solr_state: SolrState, in_mock_dir: Path
    ) -> None:
        solr_state.reject_updates = 400
        solr_state.reject_limit = 2

        with pytest.raises(RuntimeError, match="max-failed-batches=1"):
            _rotate(
                fake_solr, "fs-cat.yml", index_suffix="_bg", max_failed_batches=1
            )


class TestMinDocsGate:
    """The doc-count gate keeps a thin index out of production."""

    def test_below_min_docs_aborts_and_keeps_live(
        self, fake_solr: str, solr_state: SolrState, in_mock_dir: Path
    ) -> None:
        before = {core: solr_state.num_docs(core) for core in ("latest", "files")}

        with pytest.raises(SystemExit, match="Rotation aborted"):
            _rotate(fake_solr, "fs-cat.yml", index_suffix="_bg", min_docs=10_000)

        after = {core: solr_state.num_docs(core) for core in ("latest", "files")}
        assert after == before
        assert sorted(solr_state.cores) == ["files", "latest"]


class TestMixedState:
    """A flip that fails between the two cores has to say so."""

    def test_partial_flip_names_both_sides(
        self,
        fake_solr: str,
        solr_state: SolrState,
        in_mock_dir: Path,
        log_records: List[logging.LogRecord],
    ) -> None:
        solr_state.fail_swap_after = 1  # first core flips, second one fails

        with pytest.raises(RuntimeError, match="SWAP"):
            _rotate(fake_solr, "fs-cat.yml", index_suffix="_bg", verbosity=2)

        mixed = [
            record.getMessage()
            for record in log_records
            if "mixed state" in record.getMessage()
        ]
        assert mixed, "a partial rotation must be reported"
        assert "latest" in mixed[0] and "files" in mixed[0]


# --------------------------------------------------------------------------- #
# plain indexing (no rotation)
# --------------------------------------------------------------------------- #
class TestAdminFailures:
    """Core-Admin calls that fail for reasons other than a reused suffix."""

    def test_create_failure_is_reported(
        self, fake_solr: str, solr_state: SolrState, in_mock_dir: Path
    ) -> None:
        """A broken configset must surface Solr's own message."""
        solr_state.fail_create = True

        with pytest.raises(RuntimeError, match="CREATE latest_bg failed"):
            _rotate(fake_solr, "fs-cat.yml", index_suffix="_bg")

    def test_rename_failure_is_reported(
        self, empty_fake_solr: str, empty_solr_state: SolrState, in_mock_dir: Path
    ) -> None:
        """The first-deployment promotion has no fallback either."""
        empty_solr_state.fail_rename = True

        with pytest.raises(RuntimeError, match="RENAME latest_bg->latest failed"):
            _rotate(empty_fake_solr, "fs-cat.yml", index_suffix="_bg")

    def test_failed_unload_does_not_undo_the_rotation(
        self,
        fake_solr: str,
        solr_state: SolrState,
        in_mock_dir: Path,
        log_records: List[logging.LogRecord],
    ) -> None:
        """The swap already happened, so this is loud but not fatal.

        It does leave an orphan instance dir, which is what the message has
        to say - that dir is what a later CREATE of the same name trips over.
        """
        solr_state.fail_unload = True

        _rotate(fake_solr, "fs-cat.yml", index_suffix="_bg", verbosity=2)

        assert solr_state.num_docs("latest") > 1  # the new index is live
        messages = [record.getMessage() for record in log_records]
        assert any("instance directory may be left behind" in m for m in messages)


class TestDeleteOffline:
    """``delete`` needs no live Solr to be exercised either."""

    def test_matching_documents_are_removed(
        self, fake_solr: str, solr_state: SolrState
    ) -> None:
        delete("solr", facets=[("project", "*")], server=fake_solr)
        assert solr_state.num_docs("latest") == 0
        assert solr_state.num_docs("files") == 0

    def test_file_facets_are_escaped(
        self, fake_solr: str, solr_state: SolrState
    ) -> None:
        """Leading separators and colons have to survive into the query."""
        delete("solr", facets=[("file", "/foo/*")], server=fake_solr)
        assert solr_state.num_docs("latest") == 0

    def test_missing_core_is_reported_not_raised(
        self,
        fake_solr: str,
        solr_state: SolrState,
        log_records: List[logging.LogRecord],
    ) -> None:
        """A delete against a core that is gone warns rather than crashing."""
        delete(
            "solr",
            facets=[("project", "*")],
            server=fake_solr,
            latest_version="gone",
            verbosity=2,
        )
        assert any(
            "no such core" in record.getMessage() for record in log_records
        )

    def test_verbosity_reaches_the_ingester(
        self,
        fake_solr: str,
        solr_state: SolrState,
        log_records: List[logging.LogRecord],
    ) -> None:
        """``delete`` has to honour ``verbosity`` like ``index`` does."""
        delete("solr", facets=[("project", "*")], server=fake_solr, verbosity=4)
        assert any(
            "Deleting entries matching" in record.getMessage()
            for record in log_records
        )


class TestLoggerLevelHandling:
    """``set_level`` has to actually change what the logger emits.

    The package logger is built directly rather than through
    ``logging.getLogger``, so it never lands in the manager's registry and
    ``setLevel``'s cache invalidation skips it. Without an explicit cache
    clear, the first level a process sees is the only one it ever honours -
    which silently swallows ``-vv`` on every run after the first.
    """

    def test_level_changes_take_effect(
        self, log_records: List[logging.LogRecord]
    ) -> None:
        from metadata_crawler.logger import logger

        previous = logger.level
        try:
            logger.set_level(logging.CRITICAL)
            logger.warning("swallowed")
            assert [r.getMessage() for r in log_records] == []

            logger.set_level(logging.WARNING)
            logger.warning("emitted")
            assert [r.getMessage() for r in log_records] == ["emitted"]
        finally:
            logger.set_level(previous)

    def test_error_below_info_attaches_the_traceback(
        self, log_records: List[logging.LogRecord]
    ) -> None:
        from metadata_crawler.logger import logger

        previous = logger.level
        try:
            logger.set_level(logging.DEBUG)
            try:
                raise ValueError("boom")
            except ValueError:
                logger.error("something broke")
        finally:
            logger.set_level(previous)
        assert log_records[-1].exc_info is not None

    def test_file_handles_are_replaced_not_stacked(self) -> None:
        """``apply_verbosity`` calls this on every run."""
        from logging.handlers import RotatingFileHandler

        from metadata_crawler.logger import logger

        before = [h for h in logger.handlers if isinstance(h, RotatingFileHandler)]
        logger.add_file_handle()
        logger.add_file_handle()
        after = [h for h in logger.handlers if isinstance(h, RotatingFileHandler)]
        assert len(after) <= max(len(before), 1)


class TestWithoutRotation:
    """``index`` without ``--rotate`` writes straight into the live cores."""

    def test_documents_are_added(
        self, fake_solr: str, solr_state: SolrState, in_mock_dir: Path
    ) -> None:
        before = solr_state.num_docs("latest")
        index("solr", "fs-cat.yml", server=fake_solr, batch_size=20)
        assert solr_state.num_docs("latest") > before

    def test_every_store_is_read(
        self, fake_solr: str, solr_state: SolrState, in_mock_dir: Path
    ) -> None:
        index("solr", *CATALOGUES, server=fake_solr, batch_size=20)
        datasets = {doc.get("dataset") for doc in solr_state.documents("latest")}
        assert set(DATASETS.values()) <= datasets

    def test_rejected_batches_abort(
        self, fake_solr: str, solr_state: SolrState, in_mock_dir: Path
    ) -> None:
        """The write gate is not tied to rotation."""
        solr_state.reject_updates = 400
        with pytest.raises(RuntimeError, match="rejected"):
            index("solr", "fs-cat.yml", server=fake_solr, batch_size=20)


# --------------------------------------------------------------------------- #
# helpers, against the fake's real HTTP surface
# --------------------------------------------------------------------------- #
def _run(fake_solr: str, coro_name: str, *args: Any) -> Any:
    """Call one ``SolrIndex`` helper against the fake server."""
    from metadata_crawler.ingester.solr import SolrIndex

    async def _main() -> Any:
        instance = SolrIndex.__new__(SolrIndex)
        instance.__post_init__()
        instance._ensure_uri(fake_solr)
        async with aiohttp.ClientSession(timeout=instance.timeout) as session:
            return await getattr(instance, coro_name)(session, *args)

    return asyncio.run(_main())


class TestCoreHelpers:
    """The primitives the rotation is built from."""

    def test_core_exists_only_for_loaded_names(
        self, fake_solr: str, solr_state: SolrState
    ) -> None:
        assert _run(fake_solr, "_core_exists", "latest") is True
        assert _run(fake_solr, "_core_exists", "nope") is False

    def test_create_is_idempotent_for_a_loaded_core(
        self, fake_solr: str, solr_state: SolrState
    ) -> None:
        assert _run(fake_solr, "_create_core", "latest", "freva") is False
        assert solr_state.created == []

    def test_create_reports_what_it_created(
        self, fake_solr: str, solr_state: SolrState
    ) -> None:
        assert _run(fake_solr, "_create_core", "latest_new", "freva") is True
        assert solr_state.created == ["latest_new"]

    def test_count_docs_returns_num_found(self, fake_solr: str) -> None:
        assert _run(fake_solr, "_count_docs", "latest") == 1

    def test_count_docs_raises_for_a_missing_core(self, fake_solr: str) -> None:
        """A vanished core must not be reported as an empty one."""
        with pytest.raises(RuntimeError, match="Cannot count documents"):
            _run(fake_solr, "_count_docs", "gone")

    def test_commit_raises_for_a_missing_core(self, fake_solr: str) -> None:
        with pytest.raises(RuntimeError, match="COMMIT gone failed"):
            _run(fake_solr, "_commit", "gone")

    def test_core_status_raises_on_a_broken_server(self) -> None:
        with pytest.raises(RuntimeError, match="Core STATUS failed"):
            state = SolrState()
            for base in serve(state):
                # /solr/admin/cores is the only admin path the fake answers;
                # point the helper somewhere else to force the error branch.
                from metadata_crawler.ingester.solr import SolrIndex

                async def _main(base: str = base) -> Any:
                    instance = SolrIndex.__new__(SolrIndex)
                    instance.__post_init__()
                    instance._uri = f"{base}/nowhere"
                    async with aiohttp.ClientSession(
                        timeout=instance.timeout
                    ) as session:
                        return await instance._core_status(session)

                asyncio.run(_main())

    @pytest.mark.parametrize(
        "instance_dir,expected",
        [
            ("/data/db/latest_bg/", "latest_bg"),
            ("/data/db/latest_bg", "latest_bg"),
            ("", ""),
            (None, ""),
        ],
    )
    def test_instance_dir_name(self, instance_dir: Any, expected: str) -> None:
        from metadata_crawler.ingester.solr import SolrIndex

        assert SolrIndex._instance_dir_name(instance_dir) == expected

    def test_dir_owner_finds_the_renamed_core(self) -> None:
        from metadata_crawler.ingester.solr import SolrIndex

        status = {"latest": {"name": "latest", "instanceDir": "/data/db/latest_bg/"}}
        assert SolrIndex._dir_owner(status, "latest_bg") == "latest"
        assert SolrIndex._dir_owner(status, "latest_other") is None


def _post(url: str, body: bytes = b'[{"file": "/a.nc"}]') -> Any:
    """POST one batch through ``_post_chunk`` and return the ingester."""
    from metadata_crawler.ingester.solr import SolrIndex

    async def _main() -> Any:
        # ``__post_init__`` builds a TCPConnector, which needs a running loop.
        instance = SolrIndex.__new__(SolrIndex)
        instance.__post_init__()
        async with aiohttp.ClientSession(timeout=instance.timeout) as session:
            await instance._post_chunk(session, url, body)
        return instance

    return asyncio.run(_main())


def _bookkeeper() -> Any:
    """An ingester built for pure bookkeeping assertions."""
    from metadata_crawler.ingester.solr import SolrIndex

    async def _main() -> Any:
        instance = SolrIndex.__new__(SolrIndex)
        instance.__post_init__()
        return instance

    return asyncio.run(_main())


class TestWriteBookkeeping:
    """``_post_chunk`` books what it cannot deliver."""

    def test_successful_post_counts(self, fake_solr: str) -> None:
        instance = _post(f"{fake_solr}/solr/latest/update/json")
        assert (instance.posted_batches, instance.failed_batches) == (1, 0)

    def test_rejected_post_is_booked(self, fake_solr: str) -> None:
        instance = _post(f"{fake_solr}/solr/gone/update/json")
        assert (instance.posted_batches, instance.failed_batches) == (0, 1)
        assert "no such core" in (instance.first_error or "")

    def test_unreachable_server_is_booked(self) -> None:
        """A connection error must not look like a successful batch."""
        instance = _post("http://127.0.0.1:1/solr/x/update/json")
        assert instance.failed_batches == 1

    def test_gate_passes_when_nothing_failed(self) -> None:
        instance = _bookkeeper()
        instance.posted_batches = 5
        instance._check_write_failures(0)  # must not raise

    def test_gate_trips_over_tolerance(self) -> None:
        instance = _bookkeeper()
        instance._record_failure("boom")
        instance._record_failure("boom again")
        instance._check_write_failures(2)  # exactly at tolerance
        with pytest.raises(RuntimeError, match="First error: boom"):
            instance._check_write_failures(1)

    def test_first_error_is_kept(self) -> None:
        instance = _bookkeeper()
        instance._record_failure("first")
        instance._record_failure("second")
        assert instance.first_error == "first"
        assert instance.failed_batches == 2


class TestConsumerResilience:
    """A worker that hits an unexpected error must not strand the drain."""

    def test_consumer_survives_and_drains(self) -> None:
        from metadata_crawler.ingester.solr import SolrIndex

        async def _main() -> Tuple[int, int]:
            instance = SolrIndex.__new__(SolrIndex)
            instance.__post_init__()

            calls: List[str] = []

            async def _boom(session: Any, url: str, body: Any) -> None:
                calls.append(url)
                raise ValueError("unexpected")

            instance._post_chunk = _boom  # type: ignore[method-assign]
            worker = asyncio.create_task(instance.consumer(None))
            for num in range(3):
                await instance.producer_queue.put((f"u{num}", b"[]"))
            await instance.producer_queue.put(("", instance.senteniel))
            # A dead worker would leave this hanging.
            await asyncio.wait_for(worker, timeout=5)
            return len(calls), instance.failed_batches

        assert asyncio.run(_main()) == (3, 3)
