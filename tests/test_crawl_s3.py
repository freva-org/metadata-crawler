"""Test crawling s3 stores."""

from typing import Any, Dict, List, Mapping, Tuple, cast

import pytest
from s3fs import S3FileSystem

from metadata_crawler.api.storage_backend import MetadataType
from metadata_crawler.backends.s3 import S3Path


from pathlib import Path

import intake

from metadata_crawler import add


class FakeS3Client:
    """Stand-in for the private async API of :class:`s3fs.S3FileSystem`.

    Prefix handling is reproduced faithfully -- including the trailing slash
    quirk of ``split_path`` -- so these tests fail if the normalisation in
    :class:`~metadata_crawler.backends.s3.S3Path` is dropped again.
    """

    def __init__(self, keys: Mapping[str, int]) -> None:
        self.keys = dict(keys)
        self.lsdir_calls: List[str] = []

    @staticmethod
    def _split(path: str) -> Tuple[str, str]:
        """Mirror ``S3FileSystem.split_path`` (``key += trail``)."""
        trail = path[len(path.rstrip("/")) :]
        stripped = S3FileSystem._strip_protocol(path).lstrip("/")
        bucket, _, key = stripped.partition("/")
        return bucket, key + trail

    def _list_prefix(self, path: str) -> str:
        bucket, key = self._split(path)
        return f"{bucket}/{key}/" if key else f"{bucket}/"

    async def _lsdir(
        self, path: str, delimiter: str = "/", prefix: str = "", **_: Any
    ) -> List[Dict[str, Any]]:
        self.lsdir_calls.append(path)
        full = self._list_prefix(path) + prefix
        out: List[Dict[str, Any]] = []
        seen = set()
        for name, size in sorted(self.keys.items()):
            if not name.startswith(full):
                continue
            rest = name[len(full) :]
            if delimiter and delimiter in rest:
                common = full + rest.split(delimiter, 1)[0]
                if common not in seen:
                    seen.add(common)
                    out.append({"name": common, "type": "directory", "size": 0})
            else:
                out.append({"name": name, "type": "file", "size": size})
        return out

    async def _isfile(self, path: str) -> bool:
        bucket, key = self._split(path)
        return f"{bucket}/{key}" in self.keys

    async def _isdir(self, path: str) -> bool:
        bucket, key = self._split(path)
        return any(k.startswith(f"{bucket}/{key}/") for k in self.keys)


# A bucket laid out like the waterpark era5land hub: two zarr stores, each
# holding a realistic number of chunk objects, plus a stray netcdf file.
ZARR_BUCKET: Dict[str, int] = {
    "reanalysis/healpix/era5land/PT1H/era5land_hp10.zarr/.zmetadata": 2048,
    "reanalysis/healpix/era5land/P1M/era5land_hp10.zarr/.zmetadata": 2048,
    "reanalysis/healpix/era5land/legacy/era5land_2020.nc": 4096,
    "reanalysis/healpix/era5land/README.txt": 12,
}
for _freq in ("PT1H", "P1M"):
    for _var in ("t2m", "tp"):
        for _chunk in range(64):
            ZARR_BUCKET[
                f"reanalysis/healpix/era5land/{_freq}/"
                f"era5land_hp10.zarr/{_var}/0.{_chunk}.0"
            ] = 1024


@pytest.fixture
def store() -> S3Path:
    """An ``S3Path`` wired to the fake client, no endpoint (plain s3:// uris).

    ``_get_client`` returns ``self._client`` when it is already set, so
    injecting the fake here is enough -- no session is ever opened.
    """
    path = S3Path()
    path._client = FakeS3Client(ZARR_BUCKET)  # type: ignore[assignment]
    return path


@pytest.fixture
def client(store: S3Path) -> FakeS3Client:
    """The fake client behind ``store``, for asserting on listing calls."""
    return cast(FakeS3Client, store._client)


async def collect(store: S3Path, path: str, pattern: str = "*") -> List[str]:
    """Drain ``rglob`` into a sorted list of paths."""
    out: List[MetadataType] = [item async for item in store.rglob(path, pattern)]
    return sorted(item["path"] for item in out)


def test_crawl_s3_obs(
    drs_config_path: Path,
    storage_options: Dict[str, str],
) -> None:
    """Test crawling s3."""
    cat_file = "s3://test/metadata_crawler/tests/data.yml"
    endpoint_url = storage_options["endpoint_url"]
    add(
        drs_config_path,
        store=cat_file,
        data_store_prefix="s3://test/metadata_crawler/tests/metadata",
        batch_size=3,
        n_procs=1,
        data_set=["obs-s3"],
        storage_options=storage_options,
        verbosity=5,
    )
    cat = intake.open_catalog(cat_file, storage_options=storage_options)
    df = cat.latest.read()
    assert len(df) > 0
    # path/uri must be stable, endpoint independent values -
    # not presigned/endpoint URLs that expire or hide the bucket.
    assert df[0]["file"].startswith(endpoint_url)
    assert df[0]["uri"].startswith("s3://test/")
    assert "X-Amz" not in df[0]["uri"]


def test_crawl_s3_dir(
    drs_config_path: Path,
    storage_options: Dict[str, str],
) -> None:
    """Test crawling a flat directory."""
    cat_file = "s3://test/metadata_crawler/tests/data-flat.yml"
    inp_dir = (
        "s3://test/data/obs/observations/grid/CPC/CPC/cmorph/"
        "30min/atmos/30min/r1i1p1/v20210618/pr"
    )
    add(
        drs_config_path,
        store=cat_file,
        data_store_prefix="s3://test/metadata_crawler/tests/metadata",
        batch_size=3,
        n_procs=1,
        data_object=[inp_dir],
        storage_options=storage_options,
    )
    cat = intake.open_catalog(cat_file, storage_options=storage_options)
    assert len(cat.latest.read()) > 0


def test_crawl_s3_single_file(
    drs_config_path: Path,
    storage_options: Dict[str, str],
) -> None:
    """Test crawling a flat directory."""
    cat_file = "s3://test/metadata_crawler/tests/data-flat.yml"
    inp_file = (
        "s3://test/data/obs/observations/grid/CPC/CPC/cmorph"
        "/30min/atmos/30min/r1i1p1/v20210618/pr"
        "/pr_30min_CPC_cmorph_r1i1p1_201609020000-201609020030.nc"
    )
    add(
        drs_config_path,
        store=cat_file,
        data_store_prefix="s3://test/metadata_crawler/tests/metadata",
        batch_size=3,
        n_procs=1,
        data_object=[inp_file],
        storage_options=storage_options,
    )
    cat = intake.open_catalog(cat_file, storage_options=storage_options)
    assert len(cat.latest.read()) > 0


def test_crawl_s3_cmip6(drs_config_path: Path, storage_options: Dict[str, str]) -> None:
    cat_file = "s3://test/metadata_crawler/tests/cmip6-s3.yml"
    endpoint_url = storage_options["endpoint_url"]
    add(
        drs_config_path,
        store=cat_file,
        data_store_prefix="s3://test/metadata_crawler/tests/cmip6-s3",
        batch_size=3,
        n_procs=1,
        data_set=["cmip6-s3"],
        verbosity=5,
        storage_options=storage_options,
    )
    cat = intake.open_catalog(cat_file, storage_options=storage_options)
    df = cat.latest.read()
    assert len(df) > 0
    # There are versioned datasets so latest should not have all the entries
    assert len(df) < len(cat.files.read())
    assert df[0]["file"].startswith(endpoint_url)
    assert df[0]["uri"].startswith("s3://test/")


def test_crawl_single_s3_file(
    drs_config_path: Path,
    cat_file: Path,
) -> None:
    """Test if we can crawl a single file on s3."""
    file = (
        "s3://test/data/model/global/cmip6/CMIP6/CMIP/CSIRO-ARCCSS/"
        "ACCESS-CM2/amip/r1i1p1f1/Amon/ua/gn/v20191108/"
        "ua_Amon_ACCESS-CM2_amip_r1i1p1f1_gn_197001-201512.nc"
    )
    add(
        drs_config_path,
        n_procs=1,
        store=cat_file,
        batch_size=3,
        data_object=[file],
    )
    assert cat_file.is_file()
    cat = intake.open_catalog(cat_file)
    assert len(cat.latest.read()) == len(cat.files.read()) == 1


def test_crawl_s3_zarr(drs_config_path: Path, cat_file: Path) -> None:
    """Zarr stores are directories but are treated as files."""

    add(
        drs_config_path,
        data_set=["nextgems-s3"],
        store=cat_file,
        n_procs=1,
        batch_size=3,
        verbosity=5,
    )
    assert cat_file.is_file()
    cat = intake.open_catalog(cat_file)
    assert len(cat.latest.read()) > 0


async def test_access_uri() -> None:
    """Test for the access uri."""
    from metadata_crawler.backends.s3 import S3Path

    s3_path_1 = S3Path(endpoint_url="https://s3.foo.bar")
    s3_path_2 = S3Path(client_kwargs={"endpoint_url": "https://s3.foo.bar"})
    s3_path_3 = S3Path()
    s3_path_4 = S3Path(endpoint_url="https://amazonaws.com.cn")

    assert (await s3_path_1._access_uri("/foo/bar.nc")).startswith("http")
    assert (await s3_path_1._access_uri("/foo/bar.nc")) == (
        await s3_path_2._access_uri("/foo/bar.nc")
    )

    assert (await s3_path_3._access_uri("/foo/bar.nc")).startswith("s3")
    assert (await s3_path_3._access_uri("/foo/bar.nc")) == (
        await s3_path_4._access_uri("/foo/bar.nc")
    )


class TestNorm:
    """``_norm`` is what keeps the private s3fs methods usable."""

    @pytest.mark.parametrize(
        "given",
        [
            "s3://reanalysis/healpix/era5land",
            "s3://reanalysis/healpix/era5land/",
            "s3://reanalysis/healpix/era5land///",
            "reanalysis/healpix/era5land/",
        ],
    )
    def test_normalises_to_same_key(self, given: str) -> None:
        assert S3Path._norm(given) == "reanalysis/healpix/era5land"

    def test_normalised_path_survives_split_path(self) -> None:
        """The regression itself: no doubled slash in the listing prefix."""
        raw = "s3://reanalysis/healpix/era5land/"
        assert FakeS3Client._split(raw)[1].endswith("/")
        assert not FakeS3Client._split(S3Path._norm(raw))[1].endswith("/")

    def test_is_idempotent(self) -> None:
        once = S3Path._norm("s3://bucket/pre/")
        assert S3Path._norm(once) == once


class TestTrailingSlash:
    """A trailing slash must not silently empty the listing."""

    async def test_iterdir_ignores_trailing_slash(self, store: S3Path) -> None:
        without = [d async for d in store.iterdir("reanalysis/healpix/era5land")]
        with_slash = [d async for d in store.iterdir("reanalysis/healpix/era5land/")]
        assert without == with_slash
        assert without  # and it is not just two empty listings

    async def test_iterdir_ignores_protocol(self, store: S3Path) -> None:
        bare = [d async for d in store.iterdir("reanalysis/healpix/era5land")]
        proto = [d async for d in store.iterdir("s3://reanalysis/healpix/era5land/")]
        assert bare == proto

    async def test_rglob_ignores_trailing_slash(self, store: S3Path) -> None:
        without = await collect(store, "reanalysis/healpix/era5land")
        with_slash = await collect(store, "reanalysis/healpix/era5land/")
        assert without == with_slash
        assert without

    async def test_is_dir_and_is_file_ignore_trailing_slash(
        self, store: S3Path
    ) -> None:
        assert await store.is_dir("s3://reanalysis/healpix/era5land/") is True
        assert (
            await store.is_file("reanalysis/healpix/era5land/legacy/era5land_2020.nc/")
            is True
        )

    async def test_iterdir_strips_trailing_slash_from_names(self) -> None:
        """Placeholder objects come back with a trailing slash; drop it."""

        class Placeholders(FakeS3Client):
            async def _lsdir(self, path: str, **_: Any) -> List[Dict[str, Any]]:
                return [{"name": "bucket/pre/sub/", "type": "directory", "size": 0}]

            async def _isfile(self, path: str) -> bool:
                return False

        store = S3Path()
        store._client = Placeholders({})  # type: ignore[assignment]
        assert [d async for d in store.iterdir("bucket/pre")] == ["bucket/pre/sub"]


class TestIterdirOnFile:
    """``iterdir`` handed a concrete object yields that object, not a listing.

    ``_iter_content`` calls ``iterdir`` on whatever the walk turned up, so it
    has to tolerate a plain key.  s3 has no real directories: a listing of an
    object prefix would come back empty and the file would be dropped
    silently.
    """

    NC = "reanalysis/healpix/era5land/legacy/era5land_2020.nc"

    async def test_yields_the_file_itself(self, store: S3Path) -> None:
        assert [d async for d in store.iterdir(self.NC)] == [f"s3://{self.NC}"]

    async def test_does_not_list(self, store: S3Path, client: FakeS3Client) -> None:
        [d async for d in store.iterdir(self.NC)]
        assert client.lsdir_calls == []

    @pytest.mark.parametrize("suffix", ["", "/", "//"])
    async def test_normalised_before_the_file_check(
        self, store: S3Path, suffix: str
    ) -> None:
        """A trailing slash must not turn the object into a missing prefix."""
        found = [d async for d in store.iterdir(f"s3://{self.NC}{suffix}")]
        assert found == [f"s3://{self.NC}"]

    async def test_uri_is_used_for_a_custom_endpoint(self) -> None:
        """The file branch yields a ``uri``, which stays endpoint independent."""
        store = S3Path(endpoint_url="https://s3.waterpark.dkrz.de")
        store._client = FakeS3Client(ZARR_BUCKET)  # type: ignore[assignment]
        assert [d async for d in store.iterdir(self.NC)] == [f"s3://{self.NC}"]

    async def test_zarr_store_is_not_treated_as_a_file(self, store: S3Path) -> None:
        """A zarr store is a prefix, so ``iterdir`` still lists its members."""
        zarr = "reanalysis/healpix/era5land/PT1H/era5land_hp10.zarr"
        found = [d async for d in store.iterdir(zarr)]
        assert found == [
            f"{zarr}/.zmetadata",
            f"{zarr}/t2m",
            f"{zarr}/tp",
        ]

    async def test_missing_path_lists_nothing(self, store: S3Path) -> None:
        assert [d async for d in store.iterdir("reanalysis/nope.nc")] == []


class TestPruning:
    """The walk must stop at a data store, not descend into it."""

    async def test_zarr_root_short_circuits(
        self, store: S3Path, client: FakeS3Client
    ) -> None:
        """Handed a zarr store, ``rglob`` yields it without listing anything.

        This is the hang: ``_find`` on this prefix would enumerate all 128
        chunk objects (and, on a real store, millions) before returning.
        """
        zarr = "reanalysis/healpix/era5land/PT1H/era5land_hp10.zarr"
        assert await collect(store, zarr) == [f"s3://{zarr}"]
        assert client.lsdir_calls == []

    async def test_zarr_root_short_circuits_with_protocol_and_slash(
        self, store: S3Path, client: FakeS3Client
    ) -> None:
        zarr = "reanalysis/healpix/era5land/PT1H/era5land_hp10.zarr"
        assert await collect(store, f"s3://{zarr}/") == [f"s3://{zarr}"]
        assert client.lsdir_calls == []

    async def test_walk_does_not_descend_into_zarr(
        self, store: S3Path, client: FakeS3Client
    ) -> None:
        await collect(store, "reanalysis/healpix/era5land")
        assert not any(".zarr" in call for call in client.lsdir_calls)

    async def test_chunk_objects_are_never_yielded(self, store: S3Path) -> None:
        found = await collect(store, "reanalysis/healpix/era5land")
        assert not any("/0." in path or ".zmetadata" in path for path in found)

    async def test_finds_nested_stores(self, store: S3Path) -> None:
        assert await collect(store, "reanalysis/healpix/era5land") == [
            "s3://reanalysis/healpix/era5land/P1M/era5land_hp10.zarr",
            "s3://reanalysis/healpix/era5land/PT1H/era5land_hp10.zarr",
            "s3://reanalysis/healpix/era5land/legacy/era5land_2020.nc",
        ]

    async def test_unknown_suffixes_are_skipped(self, store: S3Path) -> None:
        found = await collect(store, "reanalysis/healpix/era5land")
        assert not any(path.endswith("README.txt") for path in found)

    async def test_single_file_root(self, store: S3Path) -> None:
        nc = "reanalysis/healpix/era5land/legacy/era5land_2020.nc"
        assert await collect(store, nc) == [f"s3://{nc}"]

    async def test_empty_prefix_yields_nothing(self, store: S3Path) -> None:
        assert await collect(store, "reanalysis/healpix/nowhere") == []

    async def test_custom_suffixes_prune_at_that_boundary(self) -> None:
        """Pruning follows ``suffixes``, not a hard-coded ``.zarr``."""
        store = S3Path(suffixes=[".nc"])
        store._client = FakeS3Client(ZARR_BUCKET)  # type: ignore[assignment]
        found = await collect(store, "reanalysis/healpix/era5land")
        assert found == ["s3://reanalysis/healpix/era5land/legacy/era5land_2020.nc"]


class TestDegenerateListings:
    """Guards against what real buckets put in a listing.

    A placeholder object (``pre/``, size 0) makes s3fs return an entry whose
    name, once the trailing slash is stripped, is the prefix being listed.
    Without the self-reference guard the walk pushes that back onto the stack
    and never terminates.
    """

    class SelfReferencing(FakeS3Client):
        async def _lsdir(self, path: str, **_: Any) -> List[Dict[str, Any]]:
            self.lsdir_calls.append(path)
            return [
                {"name": "bucket/pre/", "type": "directory", "size": 0},
                {"name": "", "type": "directory", "size": 0},
                {"name": "bucket/pre/store.zarr", "type": "directory", "size": 0},
            ]

        async def _isfile(self, path: str) -> bool:
            return False

    async def test_self_reference_does_not_loop(self) -> None:
        store = S3Path()
        store._client = self.SelfReferencing({})  # type: ignore[assignment]
        found = [item async for item in store.rglob("bucket/pre")]
        assert [item["path"] for item in found] == ["s3://bucket/pre/store.zarr"]

    async def test_self_reference_is_listed_once(self) -> None:
        store = S3Path()
        client = self.SelfReferencing({})
        store._client = client  # type: ignore[assignment]
        [item async for item in store.rglob("bucket/pre")]
        assert client.lsdir_calls == ["bucket/pre"]

    async def test_iterdir_skips_empty_non_directory_entries(self) -> None:
        """Zero byte file entries are placeholders, not data."""

        class Placeholder(FakeS3Client):
            async def _lsdir(self, path: str, **_: Any) -> List[Dict[str, Any]]:
                return [
                    {"name": "bucket/pre/marker", "type": "file", "size": 0},
                    {"name": "bucket/pre/data.nc", "type": "file", "size": 12},
                ]

            async def _isfile(self, path: str) -> bool:
                return False

        store = S3Path()
        store._client = Placeholder({})  # type: ignore[assignment]
        assert [d async for d in store.iterdir("bucket/pre")] == ["bucket/pre/data.nc"]

class TestGlobPattern:
    """``glob_pattern`` is matched relative to the ``rglob`` root."""

    async def test_star_matches_everything(self, store: S3Path) -> None:
        assert len(await collect(store, "reanalysis/healpix/era5land", "*")) == 3

    async def test_suffix_pattern(self, store: S3Path) -> None:
        found = await collect(store, "reanalysis/healpix/era5land", "*.zarr")
        assert len(found) == 2
        assert all(path.endswith(".zarr") for path in found)

    async def test_relative_path_pattern(self, store: S3Path) -> None:
        found = await collect(store, "reanalysis/healpix/era5land", "PT1H/*.zarr")
        assert found == ["s3://reanalysis/healpix/era5land/PT1H/era5land_hp10.zarr"]

    async def test_pipe_separated_patterns(self, store: S3Path) -> None:
        found = await collect(store, "reanalysis/healpix/era5land", "*.zarr|*.nc")
        assert len(found) == 3

    async def test_non_matching_pattern_yields_nothing(self, store: S3Path) -> None:
        assert await collect(store, "reanalysis/healpix/era5land", "*.grb") == []

    async def test_pattern_is_ignored_for_a_store_root(self, store: S3Path) -> None:
        """A root that *is* a dataset is returned regardless of the pattern.

        ``_ingest_dir`` hands ``rglob`` the store directory itself, so the
        pattern would otherwise be matched against an empty relative path.
        The posix backend short-circuits the same way.
        """
        zarr = "reanalysis/healpix/era5land/PT1H/era5land_hp10.zarr"
        assert await collect(store, zarr, "*/*.zarr") == [f"s3://{zarr}"]


class TestAccessUri:
    """Discovered paths keep their endpoint-dependent shape."""

    async def test_endpoint_urls_for_custom_endpoint(self) -> None:
        store = S3Path(endpoint_url="https://s3.waterpark.dkrz.de")
        store._client = FakeS3Client(ZARR_BUCKET)  # type: ignore[assignment]
        found = await collect(store, "reanalysis/healpix/era5land")
        assert all(
            path.startswith("https://s3.waterpark.dkrz.de/reanalysis/")
            for path in found
        )

    async def test_s3_uris_without_endpoint(self, store: S3Path) -> None:
        found = await collect(store, "reanalysis/healpix/era5land")
        assert all(path.startswith("s3://reanalysis/") for path in found)

    async def test_no_duplicate_slashes(self, store: S3Path) -> None:
        found = await collect(store, "reanalysis/healpix/era5land")
        assert all("//" not in path.partition("://")[-1] for path in found)
