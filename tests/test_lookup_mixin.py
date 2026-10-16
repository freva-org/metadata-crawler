"""Special unit tests for the LookupMixin class."""

import importlib
import os
import time


def _reload_with_cache_dir(tmp_path):
    os.environ["MDC_LOOKUP_CACHE_DIR"] = str(tmp_path)
    import metadata_crawler.api.mixin.lookup_mixin as lm  # noqa

    return importlib.reload(lm)


def test_static_cmor_fastpath(monkeypatch, tmp_path):
    cache_dir = tmp_path / "lookup"
    lm = _reload_with_cache_dir(cache_dir)
    assert lm._DC is None
    assert not cache_dir.exists()

    class Fake(lm.LookupMixin):
        calls = 0

        def read_attr(self, attribute: str, path: str, **k):
            Fake.calls += 1
            return f"{attribute}@{path}"

    # provide a static entry; read_attr must NOT be called
    lm.LookupMixin.CMOR_STATIC = {("tier",): "STATIC"}

    f = Fake()
    out = f.lookup("/p/file.nc", "attr", "tier")
    assert out == "STATIC"
    assert Fake.calls == 0
    assert lm._DC is None
    assert not cache_dir.exists()


def test_diskcache_reuse_across_instances(monkeypatch, tmp_path):
    lm = _reload_with_cache_dir(tmp_path)

    class Fake(lm.LookupMixin):
        calls = 0

        def read_attr(self, attribute: str, path: str, **k):
            Fake.calls += 1
            # tiny sleep to make any races more likely (still deterministic serially)
            time.sleep(0.01)
            return f"{attribute}@{path}"

    # ensure no static entries interfere
    lm.LookupMixin.CMOR_STATIC = {}

    a = Fake()
    b = Fake()

    # first call: miss -> compute -> add
    v1 = a.lookup("/p/file.nc", "units", "cmip6", "day", "tas")
    assert v1 == "units@/p/file.nc"
    assert Fake.calls == 1

    # second call (new instance): hit from diskcache, no new compute
    v2 = b.lookup("/p/file.nc", "units", "cmip6", "day", "tas")
    assert v2 == v1
    assert Fake.calls == 1  # unchanged

    # clean up cache
    lm._get_cache().clear()


def test_lookup_uses_value_cached_by_another_process(monkeypatch, tmp_path):
    """A concurrent writer can win the race between get() and add()."""
    lm = _reload_with_cache_dir(tmp_path)
    lm.LookupMixin.CMOR_STATIC = {}

    class Cache:
        def __init__(self):
            self.get_calls = 0

        def get(self, tree):
            self.get_calls += 1
            return None if self.get_calls == 1 else "cached value"

        def add(self, tree, value):
            return False

    class Fake(lm.LookupMixin):
        def read_attr(self, attribute: str, path: str, **k):
            return "locally read value"

    monkeypatch.setattr(lm, "_get_cache", lambda: Cache())

    assert Fake().lookup("/p/file.nc", "units", "cmip6", "day", "tas") == "cached value"
