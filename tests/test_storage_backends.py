"""Test storage backends other than intake."""

from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import patch

import psycopg
import pymongo
import pytest

from metadata_crawler import add
from metadata_crawler.api.config import BaseType, SchemaField
from metadata_crawler.api.metadata_stores import CatalogueWriter
from metadata_crawler.api.stores import IndexName, MongoDB, PostgreSQL
from metadata_crawler.api.stores.mongodb import _sanitise_uri
from metadata_crawler.api.stores.postgresql import _sa_column_type


@pytest.fixture()
def populated_pg(
    pg_cursor: psycopg.Cursor,
    drs_config_path: Path,
    data_dir: Path,
    db_storage_options: Dict[str, str],
) -> psycopg.Cursor:
    """Crawl observations into PostgreSQL and return the cursor."""
    with patch.object(PostgreSQL, "_CATALOGUE_TABLE", "test_catalogue"):
        add(
            drs_config_path,
            store="localhost",
            n_procs=1,
            batch_size=3,
            backend="postgresql",
            data_object=[data_dir / "observations"],
            storage_options=db_storage_options,
        )
    return pg_cursor


@pytest.fixture()
def populated_mongo(
    mongo_client: pymongo.MongoClient,
    drs_config_path: Path,
    data_dir: Path,
    db_storage_options: Dict[str, str],
) -> pymongo.MongoClient:
    """Crawl observations into MongoDB and return the client."""
    add(
        drs_config_path,
        store="localhost",
        n_procs=1,
        batch_size=3,
        backend="mongodb",
        data_object=[data_dir / "observations"],
        storage_options=db_storage_options,
    )
    return mongo_client


def _make_field(
    base: str, multi: bool = False, length: int | None = None
) -> SchemaField:
    """Build a SchemaField with controlled attributes."""
    f = SchemaField.__new__(SchemaField)
    for attr, val in {
        "key": "test",
        "type": base,
        "base_type": BaseType(base),
        "multi_valued": multi,
        "length": length,
        "required": False,
        "unique": False,
        "indexed": True,
        "default": None,
        "name": "test",
    }.items():
        object.__setattr__(f, attr, val)
    return f


@patch.object(PostgreSQL, "_CATALOGUE_TABLE", "test_catalogue")
class TestPostgresQL:
    """Test writing metadata to postgresQL."""

    def test_crawl_local_obs(
        self,
        pg_cursor: psycopg.Cursor,
        drs_config_path: Path,
        data_dir: Path,
        db_storage_options: dict[str, str],
    ) -> None:
        """Test crawling the local observations."""
        pg_cursor.execute("SELECT COUNT(*) FROM latest")
        assert pg_cursor.fetchone()[0] == 0
        add(
            drs_config_path,
            store="localhost",
            n_procs=1,
            batch_size=3,
            backend="postgresql",
            data_object=[data_dir / "observations"],
            storage_options=db_storage_options,
            verbosity=6,
        )
        pg_cursor.execute("SELECT COUNT(*) FROM latest")
        assert pg_cursor.fetchone()[0] > 0

    def test_swept_away(
        self,
        pg_cursor: psycopg.Cursor,
        drs_config_path: Path,
        data_dir: Path,
        db_storage_options: dict[str, str],
    ) -> None:
        """Test crawling the local observations."""
        pg_cursor.execute("SELECT COUNT(*) FROM latest")
        assert pg_cursor.fetchone()[0] == 0
        epoch = time.time() + 86400 * 10
        with patch(
            "metadata_crawler.data_collector.CatalogueWriter._get_epoch",
            return_value=epoch,
        ):
            with patch(
                "metadata_crawler.data_collector.CatalogueWriter.confirm_sweep",
                return_value=True,
            ):
                add(
                    drs_config_path,
                    store="localhost",
                    n_procs=1,
                    batch_size=3,
                    backend="postgresql",
                    data_object=[data_dir / "observations"],
                    storage_options=db_storage_options,
                    verbosity=6,
                )
        pg_cursor.execute("SELECT COUNT(*) FROM latest")
        assert pg_cursor.fetchone()[0] == 0


class TestMongoDB:
    """Test writing metadata to MongoDB."""

    def test_crawl_local_obs(
        self,
        mongo_client: pymongo.MongoClient[dict[str, Any]],
        drs_config_path: Path,
        data_dir: Path,
        db_storage_options: dict[str, str],
    ) -> None:
        """Test crawling the local observations."""
        assert mongo_client["metadata"]["latest"].count_documents({}) == 0
        add(
            drs_config_path,
            store="localhost",
            n_procs=1,
            batch_size=3,
            backend="mongodb",
            data_object=[data_dir / "observations"],
            storage_options=db_storage_options,
            verbosity=6,
        )
        assert mongo_client["metadata"]["latest"].count_documents({}) > 0

    def test_swept_away(
        self,
        mongo_client: pymongo.MongoClient[dict[str, Any]],
        drs_config_path: Path,
        data_dir: Path,
        db_storage_options: dict[str, str],
    ) -> None:
        """Test crawling the local observations."""
        assert mongo_client["metadata"]["latest"].count_documents({}) == 0
        epoch = time.time() + 86400 * 10
        with patch(
            "metadata_crawler.data_collector.CatalogueWriter._get_epoch",
            return_value=epoch,
        ):
            with patch(
                "metadata_crawler.data_collector.CatalogueWriter.confirm_sweep",
                return_value=True,
            ):
                add(
                    drs_config_path,
                    store="localhost",
                    n_procs=1,
                    batch_size=3,
                    backend="mongodb",
                    data_object=[data_dir / "observations"],
                    storage_options=db_storage_options,
                )
        assert mongo_client["metadata"]["latest"].count_documents({}) == 0


class TestMetaDataGlance:
    """Test reading the metadata."""

    @staticmethod
    def _create_entry(
        config_path: Path, store: str, data_dir: Path, **storage_options: Any
    ) -> None:
        add(
            config_path,
            store=store,
            n_procs=1,
            batch_size=3,
            data_object=[data_dir / "observations"],
            storage_options=storage_options,
        )

    def test_glance_intake_metadata(
        self, drs_config_path: Path, cat_file: Path, data_dir: Path
    ) -> None:
        """Test glancing the intake metadata."""
        from metadata_crawler import glance_metadata

        self._create_entry(drs_config_path, str(cat_file), data_dir)
        assert cat_file.is_file()
        metadata = glance_metadata(cat_file)
        assert isinstance(metadata, dict)
        assert "schema" in metadata
        assert "indexed_objects" in metadata
        assert metadata["indexed_objects"] > 0
        assert metadata == glance_metadata(cat_file, backend="intake")

    def test_glance_mongo_metadata(
        self,
        drs_config_path: Path,
        data_dir: Path,
        mongo_client: pymongo.MongoClient,
        db_storage_options: dict[str, str],
    ) -> None:
        """Test glancing the intake metadata."""
        from metadata_crawler import glance_metadata

        self._create_entry(
            drs_config_path, "mongodb://localhost", data_dir, **db_storage_options
        )
        metadata = glance_metadata("mongodb://localhost", **db_storage_options)
        assert isinstance(metadata, dict)
        assert "schema" in metadata
        assert "indexed_objects" in metadata
        assert metadata["indexed_objects"] > 0
        assert metadata == glance_metadata(
            "localhost", backend="mongodb", **db_storage_options
        )

    def test_glance_postgres_metadata(
        self,
        drs_config_path: Path,
        data_dir: Path,
        pg_cursor: psycopg.Cursor,
        db_storage_options: dict[str, str],
    ) -> None:
        """Test glancing the intake metadata."""
        from metadata_crawler import glance_metadata

        self._create_entry(
            drs_config_path, "postgresql://localhost", data_dir, **db_storage_options
        )
        metadata = glance_metadata("postgresql://localhost", **db_storage_options)
        assert isinstance(metadata, dict)
        assert "schema" in metadata
        assert "indexed_objects" in metadata
        assert metadata["indexed_objects"] > 0
        assert metadata == glance_metadata(
            "localhost", backend="postgresql", **db_storage_options
        )

    @pytest.mark.parametrize(
        "store_cls,attr,url",
        [
            (PostgreSQL, "_CATALOGUE_TABLE", "postgresql://localhost"),
            (MongoDB, "_CATALOGUE_COLLECTION", "mongodb://localhost:27017"),
        ],
    )
    def test_glance_metadata_failed(
        self, store_cls, attr, url, db_storage_options
    ) -> None:

        from metadata_crawler import glance_metadata

        with patch.object(store_cls, attr, "foo"):
            with pytest.raises(ValueError):
                glance_metadata(url, **db_storage_options)


class TestGenericMonogDB:
    """Cover remaining MongoDB gaps."""

    def test_sanitise_uri_no_credentials(self) -> None:
        """netloc without credentials."""
        uri = _sanitise_uri("mongodb://myhost:27017")
        assert "myhost:27017" in uri
        assert "@" not in uri.split("?")[0]

    def test_resolve_unique_key_fallback(self) -> None:
        """no unique field in schema falls back to 'file'."""
        schema: Dict[str, SchemaField] = {
            "project": SchemaField(key="project", type="string", unique=False),
            "variable": SchemaField(key="variable", type="string", unique=False),
        }
        store = MongoDB(
            "mongodb://localhost:27017",
            IndexName(),
            schema,
            mode="r",
        )
        assert store._unique_key == "file"


class TestPostgreSQLColumnTypes:
    """Cover _sa_column_type branches."""

    def test_integer_scalar(self) -> None:
        """BigInteger."""
        import sqlalchemy as sa

        assert isinstance(_sa_column_type(_make_field("integer")), sa.BigInteger)

    def test_integer_multi(self) -> None:
        """ARRAY(BigInteger)."""
        import sqlalchemy as sa

        assert isinstance(_sa_column_type(_make_field("integer", multi=True)), sa.ARRAY)

    def test_float_scalar(self) -> None:
        """Float."""
        import sqlalchemy as sa

        assert isinstance(_sa_column_type(_make_field("float")), sa.Float)

    def test_timestamp_scalar(self) -> None:
        """DateTime."""
        import sqlalchemy as sa

        assert isinstance(_sa_column_type(_make_field("timestamp")), sa.DateTime)


class TestGenericPostgreSQL:
    """Cover remaining PostgreSQL gaps."""

    def test_resolve_unique_key_fallback(self) -> None:
        """no unique field falls back to 'file'."""
        schema: Dict[str, SchemaField] = {
            "project": SchemaField(key="project", type="string", unique=False),
        }
        store = PostgreSQL(
            "postgresql://localhost/metadata",
            IndexName(),
            schema,
            mode="r",
        )
        assert store._unique_key == "file"

    def test_read_nonexistent_table(
        self,
        db_storage_options: Dict[str, str],
    ) -> None:
        """read() on a missing table returns empty."""
        schema: Dict[str, SchemaField] = {
            "file": SchemaField(key="file", type="string", unique=True),
        }
        store = PostgreSQL(
            "postgresql://localhost/metadata",
            IndexName(latest="nonexistent_table", all="also_missing"),
            schema,
            mode="r",
            storage_options=db_storage_options,
        )

        async def _collect() -> List[Dict[str, Any]]:
            records: List[Dict[str, Any]] = []
            async for batch in store.read("nonexistent_table"):
                records.extend(batch)
            return records

        result = asyncio.run(_collect())
        assert result == []


class TestGetEpoch:
    """Test the grace-period epoch calculation."""

    def test_default_grace_period(self) -> None:
        from metadata_crawler.api.metadata_stores import CatalogueWriter

        now = time.time()
        epoch = CatalogueWriter._get_epoch()
        # Default grace is 5 days = 432000 seconds
        assert now - epoch == pytest.approx(5 * 86400, abs=5)

    def test_grace_period_from_env(self) -> None:
        from metadata_crawler.api.metadata_stores import CatalogueWriter

        with patch.dict("os.environ", {"MDC_GRACE_DAYS": "10"}):
            now = time.time()
            epoch = CatalogueWriter._get_epoch()
            assert now - epoch == pytest.approx(10 * 86400, abs=5)

    def test_invalid_grace_falls_back(self) -> None:
        from metadata_crawler.api.metadata_stores import CatalogueWriter

        with patch.dict("os.environ", {"MDC_GRACE_DAYS": "abc"}):
            now = time.time()
            epoch = CatalogueWriter._get_epoch()
            assert now - epoch == pytest.approx(5 * 86400, abs=5)


@patch.object(PostgreSQL, "_CATALOGUE_TABLE", "test_catalogue")
class TestTotalObjectsPostgreSQL:
    """Test total_objects for PostgreSQL."""

    def test_zero_when_empty(
        self,
        pg_cursor: psycopg.Cursor,
        db_storage_options: Dict[str, str],
    ) -> None:
        from metadata_crawler.api.config import SchemaField
        from metadata_crawler.api.stores import IndexName

        schema = {"file": SchemaField(key="file", type="string", unique=True)}
        store = PostgreSQL(
            "postgresql://localhost/metadata",
            IndexName(),
            schema,
            mode="r",
            storage_options=db_storage_options,
        )
        assert store.total_objects == 0

    def test_nonzero_after_crawl(
        self,
        populated_pg: psycopg.Cursor,
        db_storage_options: Dict[str, str],
    ) -> None:
        meta = PostgreSQL.read_catalogue_metadata(
            "postgresql://localhost", **db_storage_options
        )
        from metadata_crawler.api.config import SchemaField
        from metadata_crawler.api.stores import IndexName

        schema = {s["key"]: SchemaField(**s) for _, s in meta["schema"].items()}
        index_name = IndexName(**meta["index_names"])
        store = PostgreSQL(
            "postgresql://localhost/metadata",
            index_name,
            schema,
            mode="r",
            storage_options=db_storage_options,
        )
        assert store.total_objects > 0


class TestTotalObjectsMongoDB:
    """Test total_objects for MongoDB."""

    def test_nonzero_after_crawl(
        self,
        populated_mongo: pymongo.MongoClient,
        db_storage_options: Dict[str, str],
    ) -> None:
        meta = MongoDB.read_catalogue_metadata(
            "mongodb://localhost:27017", **db_storage_options
        )
        from metadata_crawler.api.config import SchemaField
        from metadata_crawler.api.stores import IndexName

        schema = {s["key"]: SchemaField(**s) for _, s in meta["schema"].items()}
        index_name = IndexName(**meta["index_names"])
        store = MongoDB(
            "mongodb://localhost:27017",
            index_name,
            schema,
            mode="r",
            storage_options=db_storage_options,
        )
        assert store.total_objects > 0


@patch.object(PostgreSQL, "_CATALOGUE_TABLE", "test_catalogue")
class TestCountStalePostgreSQL:
    """Test stale object counting for PostgreSQL."""

    def test_all_stale_with_future_epoch(
        self,
        populated_pg: psycopg.Cursor,
        db_storage_options: Dict[str, str],
    ) -> None:
        meta = PostgreSQL.read_catalogue_metadata(
            "postgresql://localhost", **db_storage_options
        )
        from metadata_crawler.api.config import SchemaField
        from metadata_crawler.api.stores import IndexName

        schema = {s["key"]: SchemaField(**s) for _, s in meta["schema"].items()}
        index_name = IndexName(**meta["index_names"])
        store = PostgreSQL(
            "postgresql://localhost/metadata",
            index_name,
            schema,
            mode="r",
            storage_options=db_storage_options,
        )
        future_epoch = time.time() + 86400 * 30
        stale = store.count_stale_objects(future_epoch)
        assert stale == store.total_objects

    def test_none_stale_with_past_epoch(
        self,
        populated_pg: psycopg.Cursor,
        db_storage_options: Dict[str, str],
    ) -> None:
        meta = PostgreSQL.read_catalogue_metadata(
            "postgresql://localhost", **db_storage_options
        )
        from metadata_crawler.api.config import SchemaField
        from metadata_crawler.api.stores import IndexName

        schema = {s["key"]: SchemaField(**s) for _, s in meta["schema"].items()}
        index_name = IndexName(**meta["index_names"])
        store = PostgreSQL(
            "postgresql://localhost/metadata",
            index_name,
            schema,
            mode="r",
            storage_options=db_storage_options,
        )
        past_epoch = 0.0
        stale = store.count_stale_objects(past_epoch)
        assert stale == 0


@patch.object(PostgreSQL, "_CATALOGUE_TABLE", "test_catalogue")
class TestSweepPostgreSQL:
    """Test sweep for PostgreSQL."""

    def test_sweep_removes_all_with_future_epoch(
        self,
        populated_pg: psycopg.Cursor,
        db_storage_options: Dict[str, str],
    ) -> None:
        meta = PostgreSQL.read_catalogue_metadata(
            "postgresql://localhost", **db_storage_options
        )
        from metadata_crawler.api.config import SchemaField
        from metadata_crawler.api.stores import IndexName

        schema = {s["key"]: SchemaField(**s) for _, s in meta["schema"].items()}
        index_name = IndexName(**meta["index_names"])
        store = PostgreSQL(
            "postgresql://localhost/metadata",
            index_name,
            schema,
            mode="r",
            storage_options=db_storage_options,
        )
        assert store.total_objects > 0
        store.sweep(time.time() + 86400 * 30)
        assert store.total_objects == 0

    def test_sweep_keeps_recent_with_past_epoch(
        self,
        populated_pg: psycopg.Cursor,
        db_storage_options: Dict[str, str],
    ) -> None:
        meta = PostgreSQL.read_catalogue_metadata(
            "postgresql://localhost", **db_storage_options
        )
        from metadata_crawler.api.config import SchemaField
        from metadata_crawler.api.stores import IndexName

        schema = {s["key"]: SchemaField(**s) for _, s in meta["schema"].items()}
        index_name = IndexName(**meta["index_names"])
        store = PostgreSQL(
            "postgresql://localhost/metadata",
            index_name,
            schema,
            mode="r",
            storage_options=db_storage_options,
        )
        before = store.total_objects
        store.sweep(0.0)
        assert store.total_objects == before


class TestSweepMongoDB:
    """Test sweep for MongoDB."""

    def test_sweep_removes_all_with_future_epoch(
        self,
        populated_mongo: pymongo.MongoClient,
        db_storage_options: Dict[str, str],
    ) -> None:
        meta = MongoDB.read_catalogue_metadata(
            "mongodb://localhost:27017", **db_storage_options
        )
        from metadata_crawler.api.config import SchemaField
        from metadata_crawler.api.stores import IndexName

        schema = {s["key"]: SchemaField(**s) for _, s in meta["schema"].items()}
        index_name = IndexName(**meta["index_names"])
        store = MongoDB(
            "mongodb://localhost:27017",
            index_name,
            schema,
            mode="r",
            storage_options=db_storage_options,
        )
        assert store.total_objects > 0
        store.sweep(time.time() + 86400 * 30)
        assert store.total_objects == 0


class TestConfirmSweep:
    """Test the interactive sweep confirmation prompt."""

    def test_non_interactive_returns_false(self) -> None:
        """Non-TTY stdin aborts immediately."""
        with patch.object(sys.stdin, "isatty", return_value=False):
            assert CatalogueWriter.confirm_sweep(900, 1000) is False

    def test_user_confirms_yes(self) -> None:
        """Interactive user types 'y' within timeout."""
        with (
            patch.object(sys.stdin, "isatty", return_value=True),
            patch("metadata_crawler.api.metadata_stores.select") as mock_select,
            patch.object(sys.stdin, "readline", return_value="y\n"),
        ):
            mock_select.select.return_value = ([sys.stdin], [], [])
            assert CatalogueWriter.confirm_sweep(900, 1000, timeout=1) is True

    def test_user_declines_no(self) -> None:
        """Interactive user types 'n'."""
        with (
            patch.object(sys.stdin, "isatty", return_value=True),
            patch("metadata_crawler.api.metadata_stores.select") as mock_select,
            patch.object(sys.stdin, "readline", return_value="n\n"),
        ):
            mock_select.select.return_value = ([sys.stdin], [], [])
            assert CatalogueWriter.confirm_sweep(900, 1000, timeout=1) is False

    def test_timeout_returns_false(self) -> None:
        """No input within timeout aborts."""
        with (
            patch.object(sys.stdin, "isatty", return_value=True),
            patch("metadata_crawler.api.metadata_stores.select") as mock_select,
        ):
            mock_select.select.return_value = ([], [], [])
            assert CatalogueWriter.confirm_sweep(900, 1000, timeout=1) is False


@patch.object(PostgreSQL, "_CATALOGUE_TABLE", "test_catalogue")
class TestSweepThresholdIntegration:
    """Test that the 75% threshold guard works end-to-end."""

    def test_large_sweep_aborts_non_interactive(
        self,
        pg_cursor: psycopg.Cursor,
        drs_config_path: Path,
        data_dir: Path,
        db_storage_options: Dict[str, str],
    ) -> None:
        """A bad crawl with future epoch triggers >75% sweep,
        which aborts in non-interactive mode."""
        future_epoch = time.time() + 86400 * 30
        with (
            patch(
                "metadata_crawler.data_collector.CatalogueWriter._get_epoch",
                return_value=future_epoch,
            ),
            patch.object(sys.stdin, "isatty", return_value=False),
            pytest.raises(SystemExit),
        ):
            # First crawl to populate
            add(
                drs_config_path,
                store="localhost",
                n_procs=1,
                batch_size=3,
                backend="postgresql",
                data_object=[data_dir / "observations"],
                storage_options=db_storage_options,
            )
            # Second crawl with future epoch — all records are stale
            add(
                drs_config_path,
                store="localhost",
                n_procs=1,
                batch_size=3,
                backend="postgresql",
                data_object=[data_dir / "observations"],
                storage_options=db_storage_options,
            )
