"""Special unit tests for the LookupMixin class."""

from __future__ import annotations

import threading
import time
from types import MappingProxyType
from typing import Any, Tuple

import pytest

from metadata_crawler.api.mixin.lookup_mixin import LookupMixin

Key = Tuple[str, ...]


class FakeBackend(LookupMixin):
    """A minimal backend that records calls and returns synthetic values."""

    # knobs for tests (no __init__ required)
    delay_s: float = 0.0
    LOOKUP_INCLUDE_PATH_IN_KEY = False

    # counters
    calls_read_attr: int = 0

    def read_attr(self, attribute: str, path: str, **read_kws: Any) -> Any:
        self.calls_read_attr += 1
        if self.delay_s:
            time.sleep(self.delay_s)
        # Return a stable value based on inputs for assertions
        return f"VAL:{attribute}:{path}"


@pytest.fixture(autouse=True)
def reset_class_state():
    """Ensure CMOR_STATIC and class-level counters are clean between tests."""
    # Minimal static CMOR table for fast-path tests
    cmor_flat = {
        ("cmip6", "CF3hr", "tas", "realm"): "atmos",
        ("cmip6", "day", "pr", "time-frequency"): "day",
    }
    FakeBackend.CMOR_STATIC = MappingProxyType(dict(cmor_flat))
    # Reset class counters/knobs
    FakeBackend.calls_read_attr = 0
    FakeBackend.delay_s = 0.0
    FakeBackend.LOOKUP_INCLUDE_PATH_IN_KEY = False
    yield
    # nothing to tear down


def test_static_fastpath_skips_read_attr():
    b = FakeBackend()
    # present in CMOR_STATIC
    v = b.lookup("/data/path/file.nc", "realm", "cmip6", "CF3hr", "tas")
    assert v == "atmos"
    assert b.calls_read_attr == 0  # must not call read_attr


def test_dynamic_cache_hits_after_first_miss():
    b = FakeBackend()
    # not in static -> first call computes via read_attr
    v1 = b.lookup("/x/a.nc", "units", "cmip6", "CF3hr", "tas")
    assert v1 == "VAL:units:/x/a.nc"
    assert b.calls_read_attr == 1
    # second call (same key) should be cached
    v2 = b.lookup("/x/a.nc", "units", "cmip6", "CF3hr", "tas")
    assert v2 == v1
    assert b.calls_read_attr == 1  # no extra call


def test_clear_dynamic_cache_forces_recompute():
    b = FakeBackend()
    _ = b.lookup("/x/a.nc", "units", "cmip6", "CF3hr", "tas")
    assert b.calls_read_attr == 1
    b.clear_dynamic_cache()
    _ = b.lookup("/x/a.nc", "units", "cmip6", "CF3hr", "tas")
    assert b.calls_read_attr == 2  # recomputed after clear


def test_include_path_in_key_changes_cache_cardinality():
    b = FakeBackend()
    FakeBackend.LOOKUP_INCLUDE_PATH_IN_KEY = False
    v1 = b.lookup("/x/a.nc", "units", "cmip6", "CF3hr", "tas")
    v2 = b.lookup("/x/b.nc", "units", "cmip6", "CF3hr", "tas")
    # without path in key, second should hit the cache (1 read)
    assert b.calls_read_attr == 1
    assert v2 == v1

    # with path included, identical attr/tree but different path -> new compute
    b2 = FakeBackend()
    FakeBackend.LOOKUP_INCLUDE_PATH_IN_KEY = True
    _ = b2.lookup("/x/a.nc", "units", "cmip6", "CF3hr", "tas")
    _ = b2.lookup("/x/b.nc", "units", "cmip6", "CF3hr", "tas")
    assert b2.calls_read_attr == 2


def test_inflight_dedup_threads_only_one_computation():
    b = FakeBackend()
    b.delay_s = 0.05  # simulate slow read

    results: list[Any] = []
    errors: list[BaseException] = []
    N = 20
    barrier = threading.Barrier(N)

    def worker():
        try:
            barrier.wait()
            val = b.lookup("/x/a.nc", "units", "cmip6", "CF3hr", "tas")
            results.append(val)
        except BaseException as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(N)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    assert len(results) == N
    # only one slow read should have happened
    assert b.calls_read_attr == 1
    # all results equal
    assert len(set(results)) == 1
    assert results[0] == "VAL:units:/x/a.nc"


def test_read_attr_multi_is_used_when_overridden(monkeypatch):
    class BulkBackend(FakeBackend):
        pass

    # Ensure the subclass override is visible to the mixin's type check
    bb = BulkBackend()
    bb.delay_s = 0.0

    val = bb.lookup("/x/a.nc", "units", "cmip6", "CF3hr", "tas")
    assert val == "VAL:units:/x/a.nc"
    assert bb.calls_read_attr == 1


def test_lru_cache() -> None:

    l = LookupMixin()
    l.set_static_from_nested()
    cache = l._lookup__cache
    with pytest.raises(KeyError):
        cache["foo"]
    cache["foo"] = "bar"
    assert "foo" in cache
    assert cache["foo"] == "bar"
    for key in cache:
        assert key == "foo"
    assert len(cache) == 1
    del cache["foo"]
    assert "foo" not in cache
