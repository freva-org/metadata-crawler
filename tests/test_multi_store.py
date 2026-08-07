"""Reading several metadata stores into one index target.

``rglob_stores`` expands a directory or glob into N store uris. Those uris
have to reach *one* ingester instance: an ingester's set-up and tear-down
(creating cores, committing, rotating) is per *run*, not per store, so calling
it once per uri repeats all of it - which for a blue/green target means one
rotation per store, and every store but the last thrown away.
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import Any, AsyncIterator, Dict, List, Tuple

import mock
import pytest

from metadata_crawler import run as run_module
from metadata_crawler.api.index import BaseIndex
from metadata_crawler.utils import IndexProgress


class FakeStore:
    """A metadata store yielding canned batches."""

    def __init__(
        self,
        name: str,
        batches: Dict[str, List[List[Dict[str, Any]]]],
        index_names: Tuple[str, str] = ("latest", "files"),
    ) -> None:
        self.name = name
        self.batches = batches
        self.index_names = index_names
        self.schema: Dict[str, Any] = {}
        self.read_calls: List[str] = []

    async def read(self, index_name: str) -> AsyncIterator[List[Dict[str, Any]]]:
        self.read_calls.append(index_name)
        for batch in self.batches.get(index_name, []):
            yield batch


class Index(BaseIndex):
    """Concrete ``BaseIndex`` - the abstract methods are irrelevant here."""

    async def delete(self, **kwargs: Any) -> None: ...

    async def index(self, **kwargs: Any) -> None: ...


@pytest.fixture()
def stores(monkeypatch: pytest.MonkeyPatch) -> Dict[str, FakeStore]:
    """Map store uris onto ``FakeStore`` instances."""
    registry = {
        "store-a": FakeStore(
            "a", {"latest": [[{"n": 1}, {"n": 2}], [{"n": 3}]], "files": [[{"n": 9}]]}
        ),
        "store-b": FakeStore("b", {"latest": [[{"n": 4}]], "files": [[{"n": 8}]]}),
        "store-empty": FakeStore("empty", {}),
        "store-other-names": FakeStore(
            "other", {"latest": [[{"n": 5}]]}, index_names=("latest", "all")
        ),
    }

    class Reader:
        def __init__(self, store_url: str = "", **kwargs: Any) -> None:
            self.store = registry[store_url]

    monkeypatch.setattr("metadata_crawler.api.index.CatalogueReader", Reader)
    return registry


def _batches(index: BaseIndex, name: str) -> List[List[Dict[str, Any]]]:
    async def _main() -> List[List[Dict[str, Any]]]:
        return [batch async for batch in index.get_metadata(name)]

    return asyncio.run(_main())


# --------------------------------------------------------------------------- #
# uri handling
# --------------------------------------------------------------------------- #
class TestUriNormalisation:
    """``uri`` accepts one store or many."""

    @pytest.mark.parametrize(
        "value,expected",
        [
            (None, []),
            ("", []),
            ("a", ["a"]),
            (Path("/tmp/a.yml"), ["/tmp/a.yml"]),
            (["a", "b"], ["a", "b"]),
            (("a", "b"), ["a", "b"]),
            (["a", "", None], ["a"]),
        ],
    )
    def test_normalise(self, value: Any, expected: List[str]) -> None:
        assert BaseIndex._normalise_uris(value) == expected

    def test_single_uri_still_works(self, stores: Dict[str, FakeStore]) -> None:
        """The old single-store call signature is unchanged."""
        index = Index(uri="store-a")
        assert index._stores == [stores["store-a"]]
        assert index._store is stores["store-a"]

    def test_no_uri_yields_no_stores(self, stores: Dict[str, FakeStore]) -> None:
        index = Index()
        assert index._stores == []
        assert index._store is None
        assert _batches(index, "latest") == []


# --------------------------------------------------------------------------- #
# chaining
# --------------------------------------------------------------------------- #
class TestChaining:
    """``get_metadata`` presents N stores as one stream per index name."""

    def test_batches_of_every_store_are_yielded(
        self, stores: Dict[str, FakeStore]
    ) -> None:
        index = Index(uri=["store-a", "store-b"])
        assert _batches(index, "latest") == [
            [{"n": 1}, {"n": 2}],
            [{"n": 3}],
            [{"n": 4}],
        ]

    def test_store_order_is_preserved(self, stores: Dict[str, FakeStore]) -> None:
        index = Index(uri=["store-b", "store-a"])
        assert _batches(index, "latest") == [
            [{"n": 4}],
            [{"n": 1}, {"n": 2}],
            [{"n": 3}],
        ]

    def test_every_store_is_asked_for_the_index(
        self, stores: Dict[str, FakeStore]
    ) -> None:
        index = Index(uri=["store-a", "store-b"])
        _batches(index, "files")
        assert stores["store-a"].read_calls == ["files"]
        assert stores["store-b"].read_calls == ["files"]

    def test_an_empty_store_does_not_end_the_stream(
        self, stores: Dict[str, FakeStore]
    ) -> None:
        """A store with nothing to say must not hide the ones behind it."""
        index = Index(uri=["store-empty", "store-a"])
        assert _batches(index, "latest") == [[{"n": 1}, {"n": 2}], [{"n": 3}]]

    def test_progress_counts_every_store(self, stores: Dict[str, FakeStore]) -> None:
        seen: List[int] = []

        class Progress:
            def update(self, num: int) -> None:
                seen.append(num)

        index = Index(uri=["store-a", "store-b"], progress=Progress())  # type: ignore[arg-type]
        _batches(index, "latest")
        assert sum(seen) == 4


class TestIndexNames:
    """All stores of a run must agree on where their records belong."""

    def test_agreeing_stores(self, stores: Dict[str, FakeStore]) -> None:
        assert Index(uri=["store-a", "store-b"]).index_names == ("latest", "files")

    def test_disagreeing_stores_are_refused(self, stores: Dict[str, FakeStore]) -> None:
        index = Index(uri=["store-a", "store-other-names"])
        with pytest.raises(ValueError, match="disagree on their index names"):
            index.index_names

    def test_no_stores(self, stores: Dict[str, FakeStore]) -> None:
        assert Index().index_names == ("", "")


# --------------------------------------------------------------------------- #
# one ingester per run
# --------------------------------------------------------------------------- #
class TestAsyncCallUsesOneIngester:
    """``async_call`` must not repeat the ingester's lifecycle per store."""

    @staticmethod
    def _fake_backend() -> Tuple[Any, List[Dict[str, Any]], List[Dict[str, Any]]]:
        constructed: List[Dict[str, Any]] = []
        calls: List[Dict[str, Any]] = []

        class Fake:
            def __init__(self, **kwargs: Any) -> None:
                constructed.append(kwargs)

            async def __aenter__(self) -> "Fake":
                return self

            async def __aexit__(self, *exc: Any) -> None:
                return None

            async def index(self, **kwargs: Any) -> None:
                calls.append(kwargs)

        return Fake, constructed, calls

    def test_one_instance_for_many_stores(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        backend, constructed, calls = self._fake_backend()
        monkeypatch.setattr(run_module, "load_plugins", lambda group: {"fake": backend})

        asyncio.run(
            run_module.async_call(
                "fake", "index", uris=["a", "b", "c"], server="localhost"
            )
        )

        assert len(constructed) == 1, "one ingester, one create/commit/rotate cycle"
        assert len(calls) == 1

    def test_all_uris_reach_the_instance(self, monkeypatch: pytest.MonkeyPatch) -> None:
        backend, constructed, _ = self._fake_backend()
        monkeypatch.setattr(run_module, "load_plugins", lambda group: {"fake": backend})

        asyncio.run(run_module.async_call("fake", "index", uris=["a", "b", "c"]))

        assert constructed[0]["uri"] == ["a", "b", "c"]

    def test_no_uris_passes_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """``delete`` runs without any store; the ingester gets ``uri=None``."""
        backend, constructed, _ = self._fake_backend()
        monkeypatch.setattr(run_module, "load_plugins", lambda group: {"fake": backend})

        asyncio.run(run_module.async_call("fake", "index", uris=None))

        assert constructed[0]["uri"] is None

    def test_unknown_backend_is_reported(self, monkeypatch: pytest.MonkeyPatch) -> None:
        backend, _, _ = self._fake_backend()
        monkeypatch.setattr(run_module, "load_plugins", lambda group: {"fake": backend})

        with pytest.raises(ValueError, match="No such backend"):
            asyncio.run(run_module.async_call("nope", "index", uris=["a"]))

    def test_environment_is_restored_in_place(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``async_call`` must not rebind ``os.environ``.

        Replacing it with a plain dict drops the ``_Environ`` wrapper, so
        ``putenv`` stops firing and child processes stop seeing later changes.
        It also defeats every context manager holding the original object:
        anything set while the snapshot was taken leaks out of that context
        for the rest of the process.
        """
        backend, _, _ = self._fake_backend()
        monkeypatch.setattr(run_module, "load_plugins", lambda group: {"fake": backend})
        environ = os.environ

        with mock.patch.dict(os.environ, {"MDC_TEST_LEAK": "1"}, clear=True):
            asyncio.run(run_module.async_call("fake", "index", uris=["a"]))

        assert os.environ is environ, "os.environ was replaced, not restored"
        assert "MDC_TEST_LEAK" not in os.environ

    def test_internal_variables_do_not_survive_the_call(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        backend, _, _ = self._fake_backend()
        monkeypatch.setattr(run_module, "load_plugins", lambda group: {"fake": backend})

        asyncio.run(run_module.async_call("fake", "index", uris=["a"]))

        assert "MDC_LOG_INIT" not in os.environ


class TestProgressBeforeStart:
    """Counting must work whether or not the bar was started."""

    def test_interactive_update_without_start(self) -> None:
        """``_task`` does not exist until ``start`` adds it.

        ``BaseIndex`` builds a default ``IndexProgress`` and only
        ``async_call`` starts it, so any other caller - and any run where
        ``MDC_INTERACTIVE`` is set - would otherwise die on a task id that
        was never registered.
        """
        progress = IndexProgress(total=10, interactive=True)
        progress.update(3)  # must not raise
        assert progress._done == 3

    def test_update_after_stop(self) -> None:
        progress = IndexProgress(total=10, interactive=True)
        progress.start()
        progress.update(2)
        progress.stop()
        progress.update(2)  # the bar is gone; the count is not
        assert progress._done == 4
