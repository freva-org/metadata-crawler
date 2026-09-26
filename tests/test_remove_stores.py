"""Functional tests: remove entries from real sources of truth.

Every test crawls the 24 observation files into a fresh store (intake
catalogue, PostgreSQL and MongoDB) and removes entries from it. The
database backends need the services from ``docker-compose.yaml``.

All observation records share the same facets apart from the file name,
which encodes a half-hour time step, e.g.
``pr_30min_CPC_cmorph_r1i1p1_201609020000-201609020030.nc``. Each file is
present in both the ``latest`` and the ``files`` index.
"""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List

import intake
import pytest

import metadata_crawler.cli as mc_cli
from metadata_crawler import add, remove
from metadata_crawler.api.metadata_stores import CatalogueReader
from metadata_crawler.api.stores import PostgreSQL
from metadata_crawler.api.stores.base import Facet

FILES_PER_INDEX = 24
HOUR_00 = ("file", "*_201609020000-*")
HOUR_01 = ("file", "*_201609020100-*")
HOURS_00_TO_09 = ("file", "*_201609020?00-*")  # ten files


@dataclass
class PopulatedStore:
    """A freshly crawled source of truth."""

    backend: str
    url: str
    storage_options: Dict[str, Any] = field(default_factory=dict)

    def reader(self) -> CatalogueReader:
        return CatalogueReader(self.url, storage_options=dict(self.storage_options))

    async def remove(self, *facets: Facet, dry_run: bool = False) -> int:
        return await self.reader().store.remove(*facets, dry_run=dry_run)

    async def files(self) -> Dict[str, List[str]]:
        """Sorted file names per index."""
        store = self.reader().store
        out: Dict[str, List[str]] = {}
        for name in store.index_names:
            out[name] = sorted(
                [rec["file"] async for batch in store.read(name) for rec in batch]
            )
        return out

    async def counts(self) -> List[int]:
        return [len(files) for files in (await self.files()).values()]

    def cli_options(self) -> List[str]:
        opts: List[str] = []
        for key, value in self.storage_options.items():
            opts += ["-s", key, str(value)]
        return opts


def _crawl_intake(drs_config_path: Path, data_dir: Path, cat_file: Path) -> None:
    add(
        drs_config_path,
        store=cat_file,
        n_procs=1,
        batch_size=3,
        catalogue_backend="intake",
        data_object=[data_dir / "observations"],
    )


@pytest.fixture(params=["intake", "postgresql", "mongodb"])
def populated_store(
    request: pytest.FixtureRequest,
    monkeypatch: pytest.MonkeyPatch,
    drs_config_path: Path,
    data_dir: Path,
    cat_file: Path,
    db_storage_options: Dict[str, str],
) -> PopulatedStore:
    """Crawl the observations into the requested backend."""
    backend: str = request.param
    if backend == "intake":
        monkeypatch.chdir(cat_file.parent)
        _crawl_intake(drs_config_path, data_dir, cat_file)
        return PopulatedStore(backend, str(cat_file))

    if backend == "postgresql":
        request.getfixturevalue("pg_cursor")
        monkeypatch.setattr(PostgreSQL, "_CATALOGUE_TABLE", "test_catalogue")
    else:
        request.getfixturevalue("mongo_client")
    add(
        drs_config_path,
        store="localhost",
        n_procs=1,
        batch_size=3,
        backend=backend,
        data_object=[data_dir / "observations"],
        storage_options=db_storage_options,
    )
    return PopulatedStore(backend, f"{backend}://localhost", dict(db_storage_options))


# ---------------------------------------------------------------------------
# Behaviour every backend has to share
# ---------------------------------------------------------------------------


class TestRemoveFromStore:
    """The same facets must remove the same entries on every backend."""

    async def test_crawl_sanity(self, populated_store: PopulatedStore) -> None:
        assert await populated_store.counts() == [FILES_PER_INDEX] * 2

    async def test_dry_run_counts_without_removing(
        self, populated_store: PopulatedStore
    ) -> None:
        before = await populated_store.files()
        assert await populated_store.remove(HOURS_00_TO_09, dry_run=True) == 20
        assert await populated_store.files() == before

    async def test_remove_glob(self, populated_store: PopulatedStore) -> None:
        before = await populated_store.files()
        assert await populated_store.remove(HOURS_00_TO_09) == 20
        after = await populated_store.files()
        for name, files in before.items():
            kept = [
                f for f in files if not any(f"_201609020{h}00-" in f for h in range(10))
            ]
            assert after[name] == kept
            assert len(kept) == FILES_PER_INDEX - 10

    async def test_values_of_one_key_are_or_ed(
        self, populated_store: PopulatedStore
    ) -> None:
        assert await populated_store.remove(HOUR_00, HOUR_01) == 4
        assert await populated_store.counts() == [FILES_PER_INDEX - 2] * 2

    @pytest.mark.parametrize("variable, expected", [("pr", 2), ("ua", 0)])
    async def test_keys_are_and_ed(
        self, populated_store: PopulatedStore, variable: str, expected: int
    ) -> None:
        assert await populated_store.remove(HOUR_00, ("variable", variable)) == expected

    @pytest.mark.parametrize(
        "facet, expected",
        [
            (("project", "obs*"), 2 * FILES_PER_INDEX),  # glob on a list field
            (("project", "observations"), 2 * FILES_PER_INDEX),  # exact on a list
            (("project", "obs"), 0),  # exact means exact, not prefix
            (("dataset", "obs-fs"), 2 * FILES_PER_INDEX),  # scalar field
            (("variable", "PR"), 2 * FILES_PER_INDEX),  # case-insensitive
            (("file", "*_2016090200??-*"), 2),  # '?' is a single character
        ],
    )
    async def test_matching_semantics(
        self, populated_store: PopulatedStore, facet: Facet, expected: int
    ) -> None:
        assert await populated_store.remove(facet, dry_run=True) == expected

    async def test_no_match_removes_nothing(
        self, populated_store: PopulatedStore
    ) -> None:
        assert await populated_store.remove(("variable", "ua")) == 0
        assert await populated_store.counts() == [FILES_PER_INDEX] * 2

    async def test_invalid_facets_remove_nothing(
        self, populated_store: PopulatedStore
    ) -> None:
        """Only invalid facets must never degrade into 'remove everything'."""
        assert await populated_store.remove(("nope", "x"), ("time", "2016")) == 0
        assert await populated_store.counts() == [FILES_PER_INDEX] * 2

    async def test_removal_is_idempotent(self, populated_store: PopulatedStore) -> None:
        assert await populated_store.remove(HOUR_00) == 2
        assert await populated_store.remove(HOUR_00) == 0
        assert await populated_store.counts() == [FILES_PER_INDEX - 1] * 2


class TestRemoveApi:
    """The public ``remove`` function and the CLI."""

    def test_remove_updates_total_objects(
        self, populated_store: PopulatedStore
    ) -> None:
        assert populated_store.reader().metadata["total_objects"] == 48
        remove(
            store=populated_store.url,
            storage_options=dict(populated_store.storage_options),
            facets=[HOURS_00_TO_09],
        )
        assert populated_store.reader().metadata["total_objects"] == 48 - 20

    def test_dry_run_keeps_total_objects(self, populated_store: PopulatedStore) -> None:
        remove(
            store=populated_store.url,
            storage_options=dict(populated_store.storage_options),
            facets=[HOURS_00_TO_09],
            dry_run=True,
        )
        assert populated_store.reader().metadata["total_objects"] == 48

    def test_cli(self, populated_store: PopulatedStore) -> None:
        mc_cli.cli(
            ["remove", populated_store.url, "-f", *HOUR_00]
            + populated_store.cli_options()
        )
        assert asyncio.run(populated_store.counts()) == [FILES_PER_INDEX - 1] * 2

    def test_cli_dry_run(self, populated_store: PopulatedStore) -> None:
        mc_cli.cli(
            ["remove", populated_store.url, "-f", *HOUR_00, "--dry-run"]
            + populated_store.cli_options()
        )
        assert asyncio.run(populated_store.counts()) == [FILES_PER_INDEX] * 2


# ---------------------------------------------------------------------------
# Intake specifics: the catalogue files are rewritten on disk
# ---------------------------------------------------------------------------


@pytest.fixture()
def intake_store(
    monkeypatch: pytest.MonkeyPatch,
    drs_config_path: Path,
    data_dir: Path,
    cat_file: Path,
) -> PopulatedStore:
    monkeypatch.chdir(cat_file.parent)
    _crawl_intake(drs_config_path, data_dir, cat_file)
    return PopulatedStore("intake", str(cat_file))


def _catalogue_files(store: PopulatedStore) -> List[Path]:
    idx_store = store.reader().store
    paths = [Path(idx_store._fs._strip_protocol(s.path)) for s in idx_store._paths]
    assert all(p.is_file() for p in paths)
    return paths


class TestRemoveFromIntake:
    """Rewriting the gzipped JSONLines files."""

    async def test_survivors_are_unchanged(self, intake_store: PopulatedStore) -> None:
        store = intake_store.reader().store

        async def records(name: str) -> List[Dict[str, Any]]:
            return [
                rec
                async for batch in store.read(name, parse_timestamps=False)
                for rec in batch
            ]

        before = {name: await records(name) for name in store.index_names}
        await intake_store.remove(HOURS_00_TO_09)
        for name in store.index_names:
            expected = [
                rec
                for rec in before[name]
                if not any(f"_201609020{h}00-" in rec["file"] for h in range(10))
            ]
            assert await records(name) == expected

    async def test_catalogue_is_readable_by_intake(
        self, intake_store: PopulatedStore
    ) -> None:
        await intake_store.remove(HOURS_00_TO_09)
        cat = intake.open_catalog(intake_store.url)
        assert len(cat.latest.read()) == FILES_PER_INDEX - 10
        assert len(cat.files.read()) == FILES_PER_INDEX - 10

    async def test_no_temporary_files_are_left(
        self, intake_store: PopulatedStore
    ) -> None:
        cat_dir = Path(intake_store.url).parent
        before = set(os.listdir(cat_dir))
        await intake_store.remove(HOURS_00_TO_09, dry_run=True)
        await intake_store.remove(HOURS_00_TO_09)
        await intake_store.remove(("variable", "ua"))
        assert set(os.listdir(cat_dir)) == before

    async def test_dry_run_and_no_match_do_not_rewrite(
        self, intake_store: PopulatedStore
    ) -> None:
        paths = _catalogue_files(intake_store)
        mtimes = [p.stat().st_mtime_ns for p in paths]
        await intake_store.remove(HOURS_00_TO_09, dry_run=True)
        await intake_store.remove(("variable", "ua"))
        assert [p.stat().st_mtime_ns for p in paths] == mtimes

    async def test_remove_from_another_working_directory(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        intake_store: PopulatedStore,
    ) -> None:
        """``mdc remove path/to/cat.yml`` must not depend on the cwd."""
        other = tmp_path / "elsewhere"
        other.mkdir()
        monkeypatch.chdir(other)
        assert await intake_store.remove(HOUR_00) == 2
        assert await intake_store.counts() == [FILES_PER_INDEX - 1] * 2
