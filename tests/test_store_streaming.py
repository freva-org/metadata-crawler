"""The PostgreSQL store must stream its table, not materialise it.

The driver buffers the whole result set client-side unless the connection is
put into server-side-cursor mode. With a table of tens of millions of records
that is a multi-gigabyte allocation before the first batch is even yielded -
``batch_size`` then only controls how the already-resident rows are handed
out, which is exactly the shape of an out-of-memory kill early in a run.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, Dict, Iterator, List

import pytest
import sqlalchemy as sa

from metadata_crawler.api.stores.postgresql import PostgreSQL

ROWS = 250


@pytest.fixture()
def sqlite_url(tmp_path: Path) -> Iterator[str]:
    """A table standing in for a crawled metadata table."""
    url = f"sqlite:///{tmp_path / 'store.db'}"
    engine = sa.create_engine(url)
    meta = sa.MetaData()
    sa.Table(
        "latest",
        meta,
        sa.Column("file", sa.String, primary_key=True),
        sa.Column("project", sa.String),
        sa.Column("_epoch", sa.Float),
    )
    meta.create_all(engine)
    with engine.begin() as conn:
        conn.execute(
            sa.text("INSERT INTO latest VALUES (:file, :project, :_epoch)"),
            [
                {"file": f"/f{n}.nc", "project": "CMIP6", "_epoch": 1.0}
                for n in range(ROWS)
            ],
        )
    engine.dispose()
    try:
        yield url
    finally:
        engine.dispose()


@pytest.fixture()
def store(sqlite_url: str, monkeypatch: pytest.MonkeyPatch) -> PostgreSQL:
    """A ``PostgreSQL`` store pointed at the sqlite stand-in.

    Only ``read`` is under test, so the attributes it touches are set
    directly rather than going through the postgres-specific constructor.
    """
    instance = PostgreSQL.__new__(PostgreSQL)
    instance._url = sqlite_url  # type: ignore[attr-defined]
    instance._db_schema = None  # type: ignore[attr-defined,assignment]
    instance._epoch_key = "_epoch"  # type: ignore[attr-defined]
    instance.batch_size = 100  # type: ignore[attr-defined]
    return instance


def _read(store: PostgreSQL, index_name: str) -> List[List[Dict[str, Any]]]:
    async def _main() -> List[List[Dict[str, Any]]]:
        return [batch async for batch in store.read(index_name)]

    return asyncio.run(_main())


class TestStreamingRead:
    def test_every_row_is_returned(self, store: PostgreSQL) -> None:
        batches = _read(store, "latest")
        assert sum(len(batch) for batch in batches) == ROWS

    def test_batches_honour_batch_size(self, store: PostgreSQL) -> None:
        """The last batch is short; none may exceed the requested size."""
        batches = _read(store, "latest")
        assert [len(batch) for batch in batches] == [100, 100, 50]

    def test_epoch_column_is_stripped(self, store: PostgreSQL) -> None:
        batches = _read(store, "latest")
        assert all("_epoch" not in record for record in batches[0])
        assert batches[0][0]["project"] == "CMIP6"

    def test_a_server_side_cursor_is_requested(
        self, store: PostgreSQL, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Without ``stream_results`` the whole table lands in memory first.

        Pinning the execution option is the only way to catch a regression
        here - a buffered read returns exactly the same batches, it just
        allocates the entire table before yielding the first one.
        """
        seen: List[Dict[str, Any]] = []
        original = sa.Connection.execution_options

        def _spy(self: sa.Connection, **options: Any) -> sa.Connection:
            seen.append(options)
            return original(self, **options)

        monkeypatch.setattr(sa.Connection, "execution_options", _spy)
        _read(store, "latest")

        assert seen, "read() never set any execution options"
        assert seen[0].get("stream_results") is True
        assert seen[0].get("max_row_buffer") == store.batch_size

    def test_rows_are_fetched_in_chunks(
        self, store: PostgreSQL, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``fetchmany`` keeps the resident set to one batch."""
        sizes: List[Any] = []
        original = sa.MappingResult.fetchmany

        def _spy(self: sa.MappingResult, size: Any = None) -> Any:
            sizes.append(size)
            return original(self, size)

        monkeypatch.setattr(sa.MappingResult, "fetchmany", _spy)
        _read(store, "latest")

        assert sizes and set(sizes) == {store.batch_size}

    def test_missing_table_yields_nothing(self, store: PostgreSQL) -> None:
        assert _read(store, "does_not_exist") == []
