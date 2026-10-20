"""Unit tests for reading metadata from the source of truth.

None of these tests need a running service. They cover the facet
validation in ``IndexStore.read``, reading real gzipped JSONLines files,
and how the MongoDB backend streams its collections. Applying the facet
filter in the backends is not implemented yet, so it isn't tested here.
"""

from __future__ import annotations

import gzip
from datetime import datetime
from pathlib import Path
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple

import orjson
import pymongo
import pytest

from metadata_crawler.api.stores import MongoDB, PostgreSQL
from metadata_crawler.api.stores.base import MetadataRecord
from metadata_crawler.api.stores.jsonlines import JSONLines


def _record(n: int, project: Optional[List[str]], dataset: str) -> Dict[str, Any]:
    return {
        "file": f"/data/{n}.nc",
        "project": project,
        "dataset": dataset,
        "level": n,
        "time": ["2000-01-01T00:00:00", "2000-12-31T23:59:00"],
    }


RECORDS: List[Dict[str, Any]] = [
    _record(0, ["cmip6"], "fs"),
    _record(1, ["cmip6", "obs"], "s3"),
    _record(2, ["nextgems"], "fs"),
    _record(3, None, "fs"),
    _record(4, ["obs"], "s3"),
]


async def _collect(batches: AsyncIterator[List[MetadataRecord]]) -> List[List[Any]]:
    """Return the file names per batch."""
    return [[rec["file"] for rec in batch] async for batch in batches]


def _files(batches: List[List[Any]]) -> List[Any]:
    return [name for batch in batches for name in batch]


# ---------------------------------------------------------------------------
# The read template method
# ---------------------------------------------------------------------------


class _Recorder:
    """Replacement for ``_read`` that records how it was called."""

    def __init__(self) -> None:
        self.calls: List[Tuple[str, Tuple[Any, ...], Dict[str, Any]]] = []

    async def __call__(
        self, index_name: str, *args: Any, **kwargs: Any
    ) -> AsyncIterator[List[MetadataRecord]]:
        self.calls.append((index_name, args, kwargs))
        yield [{"file": "a"}]


@pytest.fixture()
def recorder(jsonl_store: JSONLines, monkeypatch: pytest.MonkeyPatch) -> _Recorder:
    rec = _Recorder()
    monkeypatch.setattr(jsonl_store, "_read", rec)
    return rec


class TestReadTemplate:
    """``IndexStore.read`` validates facets before delegating to ``_read``."""

    @pytest.mark.parametrize("facets", [[], []])
    async def test_without_facets(
        self, jsonl_store: JSONLines, recorder: _Recorder, facets: Any
    ) -> None:
        batches = [b async for b in jsonl_store.read("latest", *facets)]
        assert batches == [[{"file": "a"}]]
        assert [call[0] for call in recorder.calls] == ["latest"]

    async def test_valid_facets(
        self, jsonl_store: JSONLines, recorder: _Recorder
    ) -> None:
        facets = [("project", "a"), ("dataset", "x"), ("project", "b")]
        batches = [b async for b in jsonl_store.read("files", *facets)]
        assert batches == [[{"file": "a"}]]
        assert [call[0] for call in recorder.calls] == ["files"]

    async def test_partly_invalid_facets_still_read(
        self, jsonl_store: JSONLines, recorder: _Recorder
    ) -> None:
        facets = [("nope", "x"), ("time", "2000"), ("project", "a")]
        _ = [b async for b in jsonl_store.read("latest", *facets)]
        assert len(recorder.calls) == 1

    @pytest.mark.parametrize(
        "facets",
        [
            [("nope", "x")],  # unknown key
            [("time", "2000"), ("bbox", "0")],  # unfilterable types
            [("created", "2000"), ("nope", "x")],
        ],
    )
    async def test_only_invalid_facets_raise(
        self, jsonl_store: JSONLines, recorder: _Recorder, facets: Any
    ) -> None:
        """A rejected filter must never turn into 'read everything'."""
        with pytest.raises(ValueError):
            _ = [b async for b in jsonl_store.read("latest", *facets)]
        assert recorder.calls == []

    @pytest.mark.parametrize("parse_timestamps", [True, False])
    async def test_parse_timestamps_is_forwarded(
        self, jsonl_store: JSONLines, recorder: _Recorder, parse_timestamps: bool
    ) -> None:
        _ = [
            b
            async for b in jsonl_store.read("latest", parse_timestamps=parse_timestamps)
        ]
        assert recorder.calls[0][2]["parse_timestamps"] is parse_timestamps

    @pytest.mark.parametrize("cls", [JSONLines, MongoDB, PostgreSQL])
    def test_backends_implement_read(self, cls: type) -> None:
        assert "_read" in cls.__dict__


# ---------------------------------------------------------------------------
# JSONLines: reading real gzipped files
# ---------------------------------------------------------------------------


def _path(store: JSONLines, index_name: str) -> Path:
    return Path(store._fs._strip_protocol(store.get_path(index_name)))


def _write(
    store: JSONLines,
    records: List[Dict[str, Any]],
    index_name: str = "latest",
    members: int = 1,
    blank_lines: bool = False,
) -> None:
    """Write *records* like the catalogue writer does: gzip member per chunk."""
    lines = [orjson.dumps(rec) + b"\n" for rec in records]
    if blank_lines:
        lines = [line + b"\n" for line in lines]
    size = max(1, -(-len(lines) // members))
    path = _path(store, index_name)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as stream:
        for start in range(0, len(lines), size):
            stream.write(gzip.compress(b"".join(lines[start : start + size])))


class TestJsonLinesRead:
    """Streaming, batching and filtering of intake catalogue files."""

    async def test_reads_everything_in_order(self, jsonl_store: JSONLines) -> None:
        _write(jsonl_store, RECORDS)
        batches = await _collect(jsonl_store.read("latest"))
        assert _files(batches) == [rec["file"] for rec in RECORDS]

    @pytest.mark.parametrize(
        "batch_size, sizes",
        [(2, [2, 2, 1]), (5, [5]), (1, [1, 1, 1, 1, 1]), (100, [5])],
    )
    async def test_batching(
        self,
        jsonl_store: JSONLines,
        monkeypatch: pytest.MonkeyPatch,
        batch_size: int,
        sizes: List[int],
    ) -> None:
        """A size that divides the file evenly must not yield an empty batch."""
        monkeypatch.setattr(jsonl_store, "batch_size", batch_size)
        _write(jsonl_store, RECORDS)
        batches = await _collect(jsonl_store.read("latest"))
        assert [len(b) for b in batches] == sizes

    async def test_multi_member_gzip(self, jsonl_store: JSONLines) -> None:
        _write(jsonl_store, RECORDS, members=3)
        batches = await _collect(jsonl_store.read("latest"))
        assert len(_files(batches)) == len(RECORDS)

    async def test_blank_lines_are_skipped(self, jsonl_store: JSONLines) -> None:
        _write(jsonl_store, RECORDS, blank_lines=True)
        batches = await _collect(jsonl_store.read("latest"))
        assert _files(batches) == [rec["file"] for rec in RECORDS]

    async def test_timestamps(self, jsonl_store: JSONLines) -> None:
        _write(jsonl_store, RECORDS[:1])
        parsed = [r async for b in jsonl_store.read("latest") for r in b]
        raw = [
            r
            async for b in jsonl_store.read("latest", parse_timestamps=False)
            for r in b
        ]
        assert all(isinstance(t, datetime) for t in parsed[0]["time"])
        assert raw[0]["time"] == RECORDS[0]["time"]

    async def test_early_exit_closes_cleanly(
        self, jsonl_store: JSONLines, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(jsonl_store, "batch_size", 1)
        _write(jsonl_store, RECORDS)
        stream = jsonl_store.read("latest")
        first = await stream.__anext__()
        await stream.aclose()  # a prefetch is in flight here
        assert [r["file"] for r in first] == ["/data/0.nc"]
        # The file can be read again afterwards.
        assert len(_files(await _collect(jsonl_store.read("latest")))) == 5

    async def test_indexes_are_separate(self, jsonl_store: JSONLines) -> None:
        _write(jsonl_store, RECORDS[:2], index_name="latest")
        _write(jsonl_store, RECORDS, index_name="files")
        assert len(_files(await _collect(jsonl_store.read("latest")))) == 2
        assert len(_files(await _collect(jsonl_store.read("files")))) == 5

    async def test_missing_file_raises(self, jsonl_store: JSONLines) -> None:
        with pytest.raises(FileNotFoundError):
            await _collect(jsonl_store.read("latest"))


# ---------------------------------------------------------------------------
# MongoDB: streaming a collection
# ---------------------------------------------------------------------------


class _FakeCollection:
    def __init__(self, docs: List[Dict[str, Any]]) -> None:
        self.docs = docs
        self.find_calls: List[Tuple[Any, ...]] = []

    def find(self, query: Any, projection: Any, batch_size: int) -> Any:
        self.find_calls.append((query, projection, batch_size))
        docs = self.docs

        class _Cursor:
            async def __aiter__(self) -> AsyncIterator[Dict[str, Any]]:
                for doc in docs:
                    yield dict(doc)

        return _Cursor()


class _FakeAsyncClient:
    """Minimal stand-in for ``pymongo.AsyncMongoClient``."""

    collection = _FakeCollection([])
    closed = False

    def __init__(self, uri: str) -> None:
        self.uri = uri

    async def __aenter__(self) -> "_FakeAsyncClient":
        return self

    async def __aexit__(self, *_: Any) -> None:
        type(self).closed = True

    def get_default_database(self, default: str) -> Dict[str, _FakeCollection]:
        return {"latest": self.collection, "files": self.collection}


@pytest.fixture()
def fake_mongo(monkeypatch: pytest.MonkeyPatch) -> _FakeCollection:
    collection = _FakeCollection(RECORDS)
    monkeypatch.setattr(_FakeAsyncClient, "collection", collection)
    monkeypatch.setattr(_FakeAsyncClient, "closed", False)
    monkeypatch.setattr(pymongo, "AsyncMongoClient", _FakeAsyncClient)
    return collection


class TestMongoRead:
    async def test_reads_the_whole_collection(
        self, mongo_store: MongoDB, fake_mongo: _FakeCollection
    ) -> None:
        batches = await _collect(mongo_store.read("latest"))
        assert len(_files(batches)) == len(RECORDS)
        query, projection, _ = fake_mongo.find_calls[0]
        assert query == {}
        assert projection == {"_id": 0, mongo_store._epoch_key: 0}

    async def test_batching(
        self,
        mongo_store: MongoDB,
        fake_mongo: _FakeCollection,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(mongo_store, "batch_size", 2)
        batches = await _collect(mongo_store.read("latest"))
        assert [len(b) for b in batches] == [2, 2, 1]
        assert fake_mongo.find_calls[0][2] == 2

    async def test_client_is_closed(
        self, mongo_store: MongoDB, fake_mongo: _FakeCollection
    ) -> None:
        await _collect(mongo_store.read("latest"))
        assert _FakeAsyncClient.closed
