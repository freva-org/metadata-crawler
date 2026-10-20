"""Unit tests for removing entries from the source of truth.

None of these tests need a running service: they cover facet validation,
glob translation and the backend specific query building.
"""

from __future__ import annotations

import re
from typing import Dict, List
from unittest.mock import AsyncMock

import pytest

from metadata_crawler.api.stores import MongoDB, PostgreSQL
from metadata_crawler.api.stores.base import (
    FacetValue,
    IndexStore,
    glob_to_like,
    glob_to_regex,
)
from metadata_crawler.api.stores.jsonlines import JSONLines

# ---------------------------------------------------------------------------
# Glob translation
# ---------------------------------------------------------------------------


class TestGlobToRegex:
    """``glob_to_regex`` only treats ``*`` and ``?`` as wildcards."""

    @pytest.mark.parametrize(
        "glob, value, expected",
        [
            ("next*", "nextgems", True),
            ("next*", "anextgems", False),  # anchored at the start
            ("*gems", "nextgems-s3", False),  # anchored at the end
            ("cmip?", "cmip6", True),
            ("cmip?", "cmip", False),
            ("cmip?", "cmip66", False),
            ("a.b", "a.b", True),
            ("a.b", "axb", False),  # dots are literal
            ("[ab]", "[ab]", True),  # brackets are literal
            ("[ab]", "a", False),
            ("*", "", True),
            ("/data/*/v1/*.nc", "/data/cmip6/v1/tas.nc", True),
        ],
    )
    def test_matching(self, glob: str, value: str, expected: bool) -> None:
        assert bool(re.match(glob_to_regex(glob), value)) is expected


class TestGlobToLike:
    """``glob_to_like`` translates wildcards and escapes LIKE specials."""

    @pytest.mark.parametrize(
        "glob, expected",
        [
            ("next*", "next%"),
            ("cmip?", "cmip_"),
            ("100%", "100\\%"),
            ("grid_label", "grid\\_label"),
            ("back\\slash", "back\\\\slash"),
            ("*_?%", "%\\__\\%"),
            ("plain", "plain"),
        ],
    )
    def test_translation(self, glob: str, expected: str) -> None:
        assert glob_to_like(glob) == expected


# ---------------------------------------------------------------------------
# Facet validation and value splitting
# ---------------------------------------------------------------------------


class TestSanitizeFacets:
    """Facets are validated against the schema and grouped by key."""

    def test_groups_values_by_key(self, jsonl_store: JSONLines) -> None:
        grouped = jsonl_store.sanitize_facets(
            [("project", "a"), ("dataset", "x"), ("project", "b")]
        )
        assert grouped == {"project": ["a", "b"], "dataset": ["x"]}

    def test_drops_unknown_keys(self, jsonl_store: JSONLines) -> None:
        grouped = jsonl_store.sanitize_facets([("nope", "a"), ("project", "b")])
        assert grouped == {"project": ["b"]}

    @pytest.mark.parametrize("key", ["time", "bbox", "created"])
    def test_drops_unfilterable_types(self, jsonl_store: JSONLines, key: str) -> None:
        grouped = jsonl_store.sanitize_facets([(key, "x"), ("project", "b")])
        assert grouped == {"project": ["b"]}

    def test_all_invalid_gives_empty(self, jsonl_store: JSONLines) -> None:
        assert jsonl_store.sanitize_facets([("nope", "a"), ("bbox", "0")]) == {}


class TestSplitFacetValues:
    """Values are split into typed exact values and glob patterns."""

    def test_strings(self, jsonl_store: JSONLines) -> None:
        exact, globs = jsonl_store.split_facet_values(
            "project", ["cmip6", "next*", "obs?", "a.b"]
        )
        assert exact == ["cmip6", "a.b"]
        assert globs == ["next*", "obs?"]

    def test_numbers_are_cast_to_the_field_type(self, jsonl_store: JSONLines) -> None:
        assert jsonl_store.split_facet_values("level", ["3", 4]) == ([3, 4], [])
        assert jsonl_store.split_facet_values("height", ["1.5", 2]) == (
            [1.5, 2.0],
            [],
        )

    def test_invalid_numbers_are_dropped(self, jsonl_store: JSONLines) -> None:
        assert jsonl_store.split_facet_values("level", ["abc", "1.5"]) == ([], [])

    def test_globs_are_not_supported_for_numbers(self, jsonl_store: JSONLines) -> None:
        assert jsonl_store.split_facet_values("level", ["1*"]) == ([], [])


# ---------------------------------------------------------------------------
# The remove template method
# ---------------------------------------------------------------------------


class TestRemoveGuards:
    """``IndexStore.remove`` validates before delegating to ``_remove``."""

    @pytest.mark.parametrize("mode", ["w", "a"])
    async def test_refuses_write_modes(
        self, jsonl_store: JSONLines, monkeypatch: pytest.MonkeyPatch, mode: str
    ) -> None:
        backend = AsyncMock(return_value=1)
        monkeypatch.setattr(jsonl_store, "_remove", backend)
        monkeypatch.setattr(jsonl_store, "mode", mode)
        with pytest.raises(RuntimeError):
            await jsonl_store.remove(("project", "a"))
        backend.assert_not_awaited()

    async def test_no_facets_removes_nothing(
        self, jsonl_store: JSONLines, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        backend = AsyncMock(return_value=1)
        monkeypatch.setattr(jsonl_store, "_remove", backend)
        assert await jsonl_store.remove() == 0
        backend.assert_not_awaited()

    async def test_only_invalid_facets_removes_nothing(
        self, jsonl_store: JSONLines, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An empty filter must never turn into 'match everything'."""
        backend = AsyncMock(return_value=1)
        monkeypatch.setattr(jsonl_store, "_remove", backend)
        assert await jsonl_store.remove(("nope", "a"), ("time", "2000")) == 0
        backend.assert_not_awaited()

    async def test_delegates_grouped_facets(
        self, jsonl_store: JSONLines, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        backend = AsyncMock(return_value=7)
        monkeypatch.setattr(jsonl_store, "_remove", backend)
        removed = await jsonl_store.remove(
            ("project", "a"), ("project", "b"), ("nope", "x"), dry_run=True
        )
        assert removed == 7
        backend.assert_awaited_once_with({"project": ["a", "b"]}, True)

    @pytest.mark.parametrize("cls", [JSONLines, MongoDB, PostgreSQL])
    def test_backends_implement_remove(self, cls: type) -> None:
        assert cls._remove is not IndexStore._remove


# ---------------------------------------------------------------------------
# Backend specific filters
# ---------------------------------------------------------------------------


RECORDS: List[Dict[str, object]] = [
    {"file": "/a/1.nc", "project": ["cmip6"], "dataset": "fs", "level": 1},
    {"file": "/a/2.nc", "project": ["cmip6", "obs"], "dataset": "s3", "level": 2},
    {"file": "/b/3.nc", "project": ["nextgems"], "dataset": "fs", "level": 3},
    {"file": "/b/4.nc", "project": None, "dataset": "fs", "level": None},
]


class TestJsonLinesPredicate:
    """The in-memory predicate used for intake catalogues."""

    @staticmethod
    def _matching(
        store: JSONLines, grouped: Dict[str, List[FacetValue]]
    ) -> List[object]:
        predicate = store._make_predicate(grouped)
        return [rec["file"] for rec in RECORDS if predicate(rec)]

    @pytest.mark.parametrize(
        "grouped, expected",
        [
            ({"dataset": ["fs"]}, ["/a/1.nc", "/b/3.nc", "/b/4.nc"]),
            ({"project": ["obs"]}, ["/a/2.nc"]),  # any element of a list
            ({"project": ["cmip6", "nextgems"]}, ["/a/1.nc", "/a/2.nc", "/b/3.nc"]),
            ({"project": ["cmip6"], "dataset": ["fs"]}, ["/a/1.nc"]),  # AND
            ({"file": ["/a/*"]}, ["/a/1.nc", "/a/2.nc"]),
            ({"file": ["/?/3.nc"]}, ["/b/3.nc"]),
            ({"project": ["next"]}, []),  # exact, not a prefix
            ({"project": ["*"]}, ["/a/1.nc", "/a/2.nc", "/b/3.nc"]),  # None never
            ({"level": ["2", "3"]}, ["/a/2.nc", "/b/3.nc"]),
            ({"level": [2.0]}, ["/a/2.nc"]),
        ],
    )
    def test_matching(
        self,
        jsonl_store: JSONLines,
        grouped: Dict[str, List[FacetValue]],
        expected: List[str],
    ) -> None:
        assert self._matching(jsonl_store, grouped) == expected


class TestMongoQuery:
    """Facets translate into ``$in`` queries."""

    def test_exact_and_glob_values(self, mongo_store: MongoDB) -> None:
        query = mongo_store._build_query({"project": ["cmip6", "next*"]})
        values = query["project"]["$in"]
        assert values[0] == "cmip6"
        assert isinstance(values[1], re.Pattern)
        assert values[1].pattern == glob_to_regex("next*")

    def test_keys_are_combined_with_and(self, mongo_store: MongoDB) -> None:
        query = mongo_store._build_query({"project": ["a"], "dataset": ["b"]})
        assert query == {"project": {"$in": ["a"]}, "dataset": {"$in": ["b"]}}

    def test_numbers_are_typed(self, mongo_store: MongoDB) -> None:
        assert mongo_store._build_query({"level": ["3"]}) == {"level": {"$in": [3]}}

    def test_invalid_values_match_nothing(self, mongo_store: MongoDB) -> None:
        assert mongo_store._build_query({"level": ["abc"]}) == {"level": {"$in": []}}


class TestPostgresWhere:
    """Facets translate into parametrised WHERE clauses."""

    def test_scalar_exact(self, pg_store: PostgreSQL) -> None:
        where, params = pg_store._build_where({"dataset": ["fs", "s3"]})
        assert where == '("dataset" = ANY(CAST(:e0 AS text[])))'
        assert params == {"e0": ["fs", "s3"]}

    def test_scalar_glob(self, pg_store: PostgreSQL) -> None:
        where, params = pg_store._build_where({"file": ["/a/*"]})
        assert where == '("file" LIKE :g0_0)'
        assert params == {"g0_0": "/a/%"}

    def test_array_exact_uses_overlap(self, pg_store: PostgreSQL) -> None:
        where, params = pg_store._build_where({"project": ["cmip6"]})
        assert where == '("project" && CAST(:e0 AS text[]))'
        assert params == {"e0": ["cmip6"]}

    def test_array_glob_uses_unnest(self, pg_store: PostgreSQL) -> None:
        where, params = pg_store._build_where({"project": ["next*"]})
        assert 'unnest("project")' in where
        assert "LIKE :g0_0" in where
        assert params == {"g0_0": "next%"}

    def test_or_within_and_across_keys(self, pg_store: PostgreSQL) -> None:
        where, params = pg_store._build_where({"dataset": ["fs", "s*"], "level": ["3"]})
        assert where == (
            '("dataset" = ANY(CAST(:e0 AS text[])) OR "dataset" LIKE :g0_0)'
            ' AND ("level" = ANY(CAST(:e1 AS bigint[])))'
        )
        assert params == {"e0": ["fs"], "g0_0": "s%", "e1": [3]}

    def test_invalid_values_match_nothing(self, pg_store: PostgreSQL) -> None:
        where, params = pg_store._build_where({"level": ["abc"]})
        assert where == "FALSE"
        assert params == {}

    def test_values_are_never_inlined(self, pg_store: PostgreSQL) -> None:
        where, _ = pg_store._build_where({"dataset": ["x'); DROP TABLE latest; --"]})
        assert "DROP" not in where
