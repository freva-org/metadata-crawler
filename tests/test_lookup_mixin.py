"""Special unit tests for the LookupMixin class."""

import importlib
import os
import time


def _reload_with_cache_dir(tmp_path):
    os.environ["MDC_LOOKUP_CACHE_DIR"] = str(tmp_path)
    import metadata_crawler.api.mixin.lookup_mixin as lm  # noqa

    return importlib.reload(lm)


def test_static_cmor_fastpath(monkeypatch, tmp_path):
    lm = _reload_with_cache_dir(tmp_path)

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

    # clean up cache
    lm._DC.clear()


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
    lm._DC.clear()
