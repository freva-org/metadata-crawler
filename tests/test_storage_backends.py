"""Test storage backends other than intake."""

import time
from typing import Any
from pathlib import Path
from metadata_crawler import add
from unittest.mock import patch
import psycopg
import pytest
import pymongo


from metadata_crawler.api.stores import PostgreSQL, MongoDB


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
            add(
                drs_config_path,
                store="localhost",
                n_procs=1,
                batch_size=3,
                backend="postgresql",
                data_object=[data_dir / "observations"],
                storage_options=db_storage_options,
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
            add(
                drs_config_path,
                store="localhost",
                n_procs=1,
                batch_size=3,
                backend="postgresql",
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
