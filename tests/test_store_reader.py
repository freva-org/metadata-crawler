"""Tests for turning store arguments (paths, URLs, connections) into URIs."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Tuple

import pytest
import yaml

import metadata_crawler
from metadata_crawler import glance_metadata, init_config
from metadata_crawler.api.metadata_stores import CatalogueReader
from metadata_crawler.api.stores.jsonlines import IntakeConnection
from metadata_crawler.api.stores.mongodb import MongoConnection
from metadata_crawler.api.stores.postgresql import PostgresConnection
from metadata_crawler.run import (
    StoreReader,
    _get_num_of_indexed_objects,
    _norm_files,
    _Uri,
)

# ---------------------------------------------------------------------------
# StoreReader
# ---------------------------------------------------------------------------


class TestStoreReaderPaths:
    @pytest.mark.parametrize(
        "store, expected",
        [
            ("cat.yml", "cat.yml"),
            ("/work/cat.yml", "/work/cat.yml"),
            ("~/cat.yml", "{home}/cat.yml"),
            ("$CATS/cat.yml", "/cats/cat.yml"),
            ("file:///work/cat.yml", "/work/cat.yml"),
            ("s3://bucket/cat.yml", "s3://bucket/cat.yml"),
            (Path("/work/cat.yml"), "/work/cat.yml"),
            (Path("~/cat.yml"), "{home}/cat.yml"),
            (Path("cat.yml"), "{cwd}/cat.yml"),
        ],
    )
    def test_uri(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        store: Any,
        expected: str,
    ) -> None:
        home, cwd = tmp_path / "home", tmp_path / "cwd"
        cwd.mkdir()
        monkeypatch.setenv("HOME", str(home))
        monkeypatch.setenv("CATS", "/cats")
        monkeypatch.chdir(cwd)
        assert StoreReader(store).uri == expected.format(home=home, cwd=cwd)

    def test_never_a_none_scheme(self) -> None:
        for store in ("cat.yml", "/a/b.yml", Path("c.yml"), None):
            assert not StoreReader(store).uri.startswith("None")

    def test_default(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.chdir(tmp_path)
        assert StoreReader(None).uri == str(tmp_path / "data.yml")

    def test_paths_have_no_storage_options(self) -> None:
        assert StoreReader("/work/cat.yml").storage_options(None) == {}
        assert StoreReader("/work/cat.yml").storage_options({"anon": True}) == {
            "anon": True
        }


class TestStoreReaderConnections:
    def test_postgres(self) -> None:
        conn = PostgresConnection.from_url("postgresql://u:pw@h:6543/db")
        reader = StoreReader(conn)
        assert reader.uri == "postgresql+psycopg://h:6543/db"
        options = reader.storage_options(None)
        assert options["username"] == "u"
        assert options["password"] == "pw", "the backends need the plain value"

    def test_mongo(self) -> None:
        reader = StoreReader(MongoConnection.from_url("mongodb://u:pw@h/db"))
        assert reader.uri == "mongodb://h/db"
        assert reader.storage_options(None)["password"] == "pw"

    def test_intake_on_s3(self) -> None:
        conn = IntakeConnection.from_url(
            "s3://bucket/cat.yml", endpoint_url="https://s3", key="k", secret="s"
        )
        reader = StoreReader(conn)
        assert reader.uri == "s3://bucket/cat.yml"
        assert reader.storage_options(None) == {
            "endpoint_url": "https://s3",
            "key": "k",
            "secret": "s",
        }

    def test_command_line_options_win(self) -> None:
        conn = PostgresConnection.from_url("postgresql://u:pw@h/db", db_schema="a")
        reader = StoreReader(conn)
        options = reader.storage_options({"db_schema": "b", "extra": 1})
        assert options["db_schema"] == "b" and options["extra"] == 1
        assert options["username"] == "u"

    def test_merging_does_not_leak_between_calls(self) -> None:
        reader = StoreReader(PostgresConnection.from_url("postgresql://h/db"))
        reader.storage_options({"db_schema": "changed"})
        assert reader.storage_options(None)["db_schema"] == "metadata_crawler"


# ---------------------------------------------------------------------------
# _norm_files and _get_num_of_indexed_objects
# ---------------------------------------------------------------------------


@pytest.fixture()
def catalogues(tmp_path: Path) -> Tuple[Path, List[Path]]:
    """A directory with two catalogue files and one unrelated file."""
    directory = tmp_path / "cats"
    directory.mkdir()
    cats = [directory / "a.yml", directory / "b.yml"]
    for cat in cats:
        cat.write_text(yaml.safe_dump({"metadata": {"indexed_objects": 2}}))
    (directory / "notes.txt").write_text("x")
    return directory, cats


class TestNormFiles:
    def test_none(self) -> None:
        assert [u.uri for u in _norm_files(None)] == [""]

    def test_single_path(self, catalogues: Tuple[Path, List[Path]]) -> None:
        _, cats = catalogues
        assert [u.uri for u in _norm_files(str(cats[0]))] == [str(cats[0])]

    def test_directory_is_expanded(self, catalogues: Tuple[Path, List[Path]]) -> None:
        directory, cats = catalogues
        result = _norm_files(str(directory), backend="intake")
        assert sorted(u.uri for u in result) == sorted(map(str, cats))

    def test_mixed_sequence(self, catalogues: Tuple[Path, List[Path]]) -> None:
        _, cats = catalogues
        conn = PostgresConnection.from_url("postgresql://u:pw@h/db")
        result = _norm_files([str(cats[0]), conn, cats[1]], anon=True)
        assert [u.uri for u in result] == [
            str(cats[0]),
            "postgresql+psycopg://h:5432/db",
            str(cats[1]),
        ]
        assert result[0].storage_options == {"anon": True}
        assert result[1].storage_options["password"] == "pw"
        assert result[1].storage_options["anon"] is True

    def test_single_connection(self) -> None:
        conn = MongoConnection.from_url("mongodb://h/db")
        assert [u.uri for u in _norm_files(conn)] == ["mongodb://h/db"]

    def test_options_are_not_shown_in_repr(self) -> None:
        uri = _Uri(uri="postgresql://h/db", storage_options={"password": "pw"})
        assert "pw" not in repr(uri)


class TestNumOfIndexedObjects:
    def test_counts_catalogues(self, catalogues: Tuple[Path, List[Path]]) -> None:
        directory, _ = catalogues
        stores = _norm_files(str(directory), backend="intake")
        assert _get_num_of_indexed_objects(stores, backend="intake") == 4

    def test_missing_catalogue_is_skipped(self, tmp_path: Path) -> None:
        stores = (_Uri(uri=str(tmp_path / "missing.yml")),)
        assert _get_num_of_indexed_objects(stores) == 0

    def test_uses_each_stores_options(self, monkeypatch: pytest.MonkeyPatch) -> None:
        calls: List[Tuple[str, Dict[str, Any]]] = []

        def read(uri: str, backend: Any = None, **options: Any) -> Dict[str, Any]:
            calls.append((uri, options))
            return {"indexed_objects": 5}

        monkeypatch.setattr(CatalogueReader, "read_catalogue_metadata", read)
        stores = (
            _Uri(uri="a", storage_options={"password": "1"}),
            _Uri(uri="b", storage_options={"password": "2"}),
        )
        assert _get_num_of_indexed_objects(stores) == 10
        assert calls == [("a", {"password": "1"}), ("b", {"password": "2"})]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


class TestGlanceMetadata:
    @pytest.fixture()
    def calls(self, monkeypatch: pytest.MonkeyPatch) -> List[Dict[str, Any]]:
        recorded: List[Dict[str, Any]] = []

        def read(uri: str, backend: Any = None, **options: Any) -> Dict[str, Any]:
            recorded.append({"uri": uri, "backend": backend, "options": options})
            return {"ok": True}

        monkeypatch.setattr(CatalogueReader, "read_catalogue_metadata", read)
        return recorded

    def test_path(self, calls: List[Dict[str, Any]]) -> None:
        assert glance_metadata("/work/cat.yml", anon=True) == {"ok": True}
        assert calls == [
            {"uri": "/work/cat.yml", "backend": None, "options": {"anon": True}}
        ]

    def test_connection(self, calls: List[Dict[str, Any]]) -> None:
        conn = PostgresConnection.from_url("postgresql://u:pw@h/db")
        glance_metadata(conn, backend="postgresql")
        assert calls[0]["uri"] == "postgresql+psycopg://h:5432/db"
        assert calls[0]["backend"] == "postgresql"
        assert calls[0]["options"]["password"] == "pw"


class TestInitConfig:
    def test_creates_both(
        self, config_home: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        init_config()
        assert (config_home / "connections.toml").is_file()
        assert (config_home / "secrets.toml").is_file()
        assert capsys.readouterr().out.count("Created") == 2

    def test_skips_existing(
        self, config_home: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        init_config()
        capsys.readouterr()
        (config_home / "secrets.toml").write_text("mine")
        init_config()
        assert capsys.readouterr().out.count("Skipped") == 2
        assert (config_home / "secrets.toml").read_text() == "mine"

    def test_force(self, config_home: Path, capsys: pytest.CaptureFixture[str]) -> None:
        init_config()
        (config_home / "secrets.toml").write_text("mine")
        init_config(force=True)
        assert (config_home / "secrets.toml").read_text() != "mine"

    def test_is_public(self) -> None:
        assert "init_config" in metadata_crawler.__all__
