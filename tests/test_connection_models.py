"""Tests for the connection models, their registry and the URIs they build."""

from __future__ import annotations

import warnings
from types import SimpleNamespace
from typing import Any, Dict, Iterator, List
from urllib.parse import parse_qs, urlsplit

import pydantic
import pytest

import metadata_crawler.api.stores.base as base
from metadata_crawler.api.stores.base import (
    CREDENTIAL_KEYS,
    BaseConnection,
    Credentials,
    IndexStore,
    S3Options,
)
from metadata_crawler.api.stores.jsonlines import IntakeConnection
from metadata_crawler.api.stores.mongodb import MongoConnection, _sanitise_uri
from metadata_crawler.api.stores.postgresql import PostgresConnection


@pytest.fixture()
def registry() -> Iterator[Dict[str, Any]]:
    """Restore the global registry after tests that define backends."""
    saved = dict(BaseConnection._registry)
    try:
        yield BaseConnection._registry
    finally:
        BaseConnection._registry.clear()
        BaseConnection._registry.update(saved)


# ---------------------------------------------------------------------------
# Option blocks
# ---------------------------------------------------------------------------


class TestS3Options:
    def test_key_and_secret_together(self) -> None:
        with pytest.raises(pydantic.ValidationError, match="together"):
            S3Options(key="k")
        with pytest.raises(pydantic.ValidationError, match="together"):
            S3Options(secret="s")
        S3Options(key="k", secret="s")

    def test_options(self) -> None:
        opts = S3Options.model_validate(
            {"endpoint_url": "https://s3", "key": "k", "secret": "s", "retries": 3}
        )
        masked = opts.options()
        assert isinstance(masked["key"], pydantic.SecretStr)
        assert "anon" not in masked, "unset options are dropped"
        assert opts.options(reveal=True) == {
            "endpoint_url": "https://s3",
            "key": "k",
            "secret": "s",
            "retries": 3,
        }


class TestCredentials:
    @pytest.mark.parametrize(
        "data",
        [
            {"username": "u", "password": "s3cr3t"},
            {"user": "u", "passwd": "s3cr3t"},
        ],
    )
    def test_aliases(self, data: Dict[str, str]) -> None:
        creds = Credentials.model_validate(data)
        assert creds.username == "u"
        assert creds.password is not None
        assert creds.password.get_secret_value() == "s3cr3t"
        assert "s3cr3t" not in repr(creds)


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


class TestRegistry:
    def test_builtins(self) -> None:
        assert {"intake", "postgresql", "mongodb"} <= set(BaseConnection.registered())

    def test_subclass_registers_itself(self, registry: Dict[str, Any]) -> None:
        class SqliteConnection(BaseConnection):
            backend = "sqlite-test"
            schemes = frozenset({"sqlite"})

            @property
            def store_uri(self) -> str:
                return self.url

        assert BaseConnection.for_backend("sqlite-test") is SqliteConnection
        assert BaseConnection.for_url("sqlite:///x.db") is SqliteConnection

    def test_intermediate_classes_do_not_register(
        self, registry: Dict[str, Any]
    ) -> None:
        before = set(registry)

        class SQLBase(BaseConnection):
            pass

        assert set(registry) == before

    def test_duplicate_backend_name(self, registry: Dict[str, Any]) -> None:
        with pytest.raises(TypeError, match="already registered"):

            class Other(BaseConnection):
                backend = "postgresql"

    def test_redefining_the_same_class_is_allowed(
        self, registry: Dict[str, Any]
    ) -> None:
        def define() -> type:
            class Reloaded(BaseConnection):
                backend = "reloaded-test"

            return Reloaded

        define()
        latest = define()  # e.g. a module reload in a notebook or test run
        assert BaseConnection.for_backend("reloaded-test") is latest

    def test_unknown_backend(self) -> None:
        with pytest.raises(ValueError, match="known: .*intake"):
            BaseConnection.for_backend("nope")

    @pytest.mark.parametrize(
        "url, model",
        [
            ("postgresql://h/db", PostgresConnection),
            ("postgres://h/db", PostgresConnection),
            ("mongodb://h/db", MongoConnection),
            ("mongodb+srv://cluster/db", MongoConnection),
            ("s3://bucket/cat.yml", IntakeConnection),
            ("/work/cat.yml", IntakeConnection),
            ("cat.yml", IntakeConnection),
        ],
    )
    def test_for_url(self, url: str, model: type) -> None:
        assert BaseConnection.for_url(url) is model

    def test_scheme_claimed_twice(self, registry: Dict[str, Any]) -> None:
        class Shadow(BaseConnection):
            backend = "shadow-test"
            schemes = frozenset({"postgresql"})

        with pytest.raises(ValueError, match="claimed by several backends"):
            BaseConnection.for_url("postgresql://h/db")

    def test_fallback_must_be_unique(self, registry: Dict[str, Any]) -> None:
        class SecondFallback(BaseConnection):
            backend = "fallback-test"
            fallback = True

        with pytest.raises(ValueError, match="no backend handles"):
            BaseConnection.for_url("ftp://somewhere/x")

    def test_plugins_are_loaded_once(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Index plugins define connection models; catalogue stores are built in."""
        groups: List[str] = []

        class EntryPoint:
            name = "plugin"

            def load(self) -> None:
                groups.append("loaded")

        def entry_points(group: str) -> List[EntryPoint]:
            groups.append(group)
            return [EntryPoint()]

        import importlib.metadata

        monkeypatch.setattr(importlib.metadata, "entry_points", entry_points)
        monkeypatch.setattr(base, "_plugins_loaded", False)
        BaseConnection.registered()
        BaseConnection.registered()
        assert groups == ["metadata_crawler.ingester", "loaded"]

    def test_broken_index_plugin_is_skipped(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An index plugin with missing dependencies must not break the rest."""

        class EntryPoint:
            name = "broken"

            def load(self) -> None:
                raise ImportError("No module named 'nothing'")

        import importlib.metadata

        monkeypatch.setattr(
            importlib.metadata, "entry_points", lambda group: [EntryPoint()]
        )
        monkeypatch.setattr(base, "_plugins_loaded", False)
        assert "postgresql" in BaseConnection.registered()


# ---------------------------------------------------------------------------
# Common connection behaviour
# ---------------------------------------------------------------------------


class TestBaseConnection:
    def test_from_url(self) -> None:
        conn = PostgresConnection.from_url("postgresql://u:p@h:1234/db")
        assert conn.name == conn.url == "postgresql://h:1234/db"
        assert (conn.host, conn.port, conn.database, conn.username) == (
            "h",
            1234,
            "db",
            "u",
        )

    def test_uri_and_url_are_accepted(self) -> None:
        a = PostgresConnection.model_validate({"name": "a", "uri": "postgresql://h/x"})
        b = PostgresConnection.model_validate({"name": "b", "url": "postgresql://h/x"})
        assert a.url == b.url and a.database == b.database == "x"

    def test_explicit_values_win_over_the_url(self) -> None:
        conn = PostgresConnection.model_validate(
            {
                "name": "p",
                "uri": "postgresql://urluser:urlpw@urlhost:1/urldb",
                "host": "h",
                "port": 2,
                "database": "d",
                "user": "u",
                "passwd": "pw",
            }
        )
        assert (conn.host, conn.port, conn.database, conn.username) == (
            "h",
            2,
            "d",
            "u",
        )
        assert conn.password is not None
        assert conn.password.get_secret_value() == "pw"

    def test_frozen(self) -> None:
        conn = PostgresConnection.from_url("postgresql://h/db")
        with pytest.raises(pydantic.ValidationError):
            conn.host = "other"  # type: ignore[misc]

    def test_unknown_keys_are_rejected(self) -> None:
        with pytest.raises(pydantic.ValidationError, match="typo"):
            PostgresConnection.model_validate(
                {"name": "p", "uri": "postgresql://h/db", "typo": 1}
            )

    def test_catalogue_options_drop_secrets(self) -> None:
        conn = PostgresConnection.from_url("postgresql://u:pw@h/db")
        assert "password" in conn.storage_options()
        assert "password" not in conn.catalogue_options()
        assert conn.catalogue_options()["username"] == "u"

    def test_secrets_are_masked(self) -> None:
        conn = PostgresConnection.from_url("postgresql://u:topsecret@h/db")
        assert "topsecret" not in str(conn.storage_options())
        assert conn.storage_options(reveal=True)["password"] == "topsecret"

    @pytest.mark.parametrize(
        "model, url",
        [
            (PostgresConnection, "postgresql://u:topsecret@h/db"),
            (MongoConnection, "mongodb://u:topsecret@h/db"),
        ],
    )
    def test_credentials_in_the_url_are_not_kept(self, model: Any, url: str) -> None:
        """A password in the URI must only live in the masked ``password``."""
        conn = model.from_url(url)
        assert conn.password.get_secret_value() == "topsecret"
        assert "topsecret" not in repr(conn)
        assert "topsecret" not in conn.url and "topsecret" not in conn.name
        assert conn.username == "u"


# ---------------------------------------------------------------------------
# Intake
# ---------------------------------------------------------------------------


class TestIntakeConnection:
    def test_s3_options_are_collected(self) -> None:
        conn = IntakeConnection.model_validate(
            {
                "name": "wp",
                "uri": "s3://bucket/cat.yml",
                "endpoint_url": "https://s3",
                "key": "k",
                "secret": "s",
                "retries": 3,
            }
        )
        assert conn.s3.endpoint_url == "https://s3"
        assert conn.s3.model_extra == {"retries": 3}
        assert conn.storage_options(reveal=True)["key"] == "k"

    def test_explicit_s3_block_is_merged(self) -> None:
        conn = IntakeConnection.model_validate(
            {"name": "x", "uri": "s3://b/c.yml", "s3": {"anon": True}, "retries": 1}
        )
        assert conn.s3.anon is True and conn.s3.model_extra == {"retries": 1}

    @pytest.mark.parametrize(
        "url, remote",
        [
            ("s3://b/c.yml", True),
            ("/work/c.yml", False),
            ("c.yml", False),
            ("file:///work/c.yml", False),
        ],
    )
    def test_is_remote(self, url: str, remote: bool) -> None:
        assert IntakeConnection.from_url(url).is_remote is remote

    def test_local_storage_options_are_empty(self) -> None:
        conn = IntakeConnection.from_url("/work/c.yml", endpoint_url="https://s3")
        assert conn.storage_options() == {}

    @pytest.mark.parametrize(
        "url, expected",
        [
            ("/work/c.yml", "/work/c.yml"),
            ("~/c.yml", "{home}/c.yml"),
            ("$CATS/c.yml", "/cats/c.yml"),
            ("file:///work/c.yml", "/work/c.yml"),
            ("s3://bucket/c.yml", "s3://bucket/c.yml"),
        ],
    )
    def test_store_uri(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Any, url: str, expected: str
    ) -> None:
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.setenv("CATS", "/cats")
        uri = IntakeConnection.from_url(url).store_uri
        assert uri == expected.format(home=tmp_path)

    def test_filesystem(self, tmp_path: Any) -> None:
        fs, path = IntakeConnection.from_url(str(tmp_path / "c.yml")).filesystem()
        assert fs.protocol in ("file", ("file", "local"))
        assert path.endswith("c.yml")


# ---------------------------------------------------------------------------
# MongoDB
# ---------------------------------------------------------------------------


class TestMongoConnection:
    def test_url_parts(self) -> None:
        conn = MongoConnection.from_url("mongodb://u:p@mongo.example.org:27018/meta")
        assert (conn.host, conn.port, conn.database, conn.username) == (
            "mongo.example.org",
            27018,
            "meta",
            "u",
        )

    def test_defaults(self) -> None:
        conn = MongoConnection.from_url("mongodb://h")
        assert (conn.port, conn.database) == (None, "metadata")

    def test_uri_options(self) -> None:
        conn = MongoConnection.from_url(
            "mongodb://h/db", tls=True, retryWrites=False, replicaSet="rs0"
        )
        assert conn.uri_options() == {
            "tls": "true",
            "retryWrites": "false",
            "replicaSet": "rs0",
        }

    def test_store_uri_has_no_credentials(self) -> None:
        conn = MongoConnection.from_url("mongodb://u:secret@h:27018/db", tls=True)
        assert conn.store_uri == "mongodb://h:27018/db?tls=true"
        assert "secret" not in conn.store_uri

    def test_store_uri_keeps_the_scheme(self) -> None:
        conn = MongoConnection.from_url("mongodb+srv://cluster.example.org/db")
        assert conn.store_uri == "mongodb+srv://cluster.example.org/db"

    def test_storage_options(self) -> None:
        conn = MongoConnection.from_url("mongodb://u:p@h:1/db", tls=True)
        assert conn.storage_options(reveal=True) == {
            "host": "h",
            "port": 1,
            "database": "db",
            "username": "u",
            "password": "p",
            "tls": True,
        }
        assert "port" not in MongoConnection.from_url("mongodb://h").storage_options()


class TestSanitiseUri:
    """The URI the stores actually connect with."""

    def test_credentials_come_from_options(self) -> None:
        conn = MongoConnection.from_url("mongodb://u:p@h:27018/db")
        uri = _sanitise_uri(conn.store_uri, **conn.storage_options(reveal=True))
        parts = urlsplit(uri)
        assert (parts.username, parts.password, parts.hostname, parts.port) == (
            "u",
            "p",
            "h",
            27018,
        )
        assert parts.path == "/db"

    def test_only_uri_options_end_up_in_the_query(self) -> None:
        conn = MongoConnection.from_url("mongodb://u:p@h/db", tls=True)
        uri = _sanitise_uri(conn.store_uri, **conn.storage_options(reveal=True))
        query = parse_qs(urlsplit(uri).query)
        assert set(query) == {"tls", "authSource", "timeoutMS"}
        assert query["tls"] == ["true"]

    @pytest.mark.parametrize("value, rendered", [(True, "true"), (False, "false")])
    def test_booleans_are_lowercase(self, value: bool, rendered: str) -> None:
        uri = _sanitise_uri("mongodb://h/db", tls=value)
        assert parse_qs(urlsplit(uri).query)["tls"] == [rendered]

    def test_pymongo_accepts_the_uri(self) -> None:
        pymongo = pytest.importorskip("pymongo")
        conn = MongoConnection.from_url("mongodb://u:p@h/db", tls=False)
        uri = _sanitise_uri(conn.store_uri, **conn.storage_options(reveal=True))
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            pymongo.MongoClient(uri, connect=False).close()

    def test_srv_uris_stay_srv(self) -> None:
        """``mongodb+srv`` must keep its scheme and must not get a port."""
        conn = MongoConnection.from_url("mongodb+srv://cluster.example.org/db")
        uri = _sanitise_uri(conn.store_uri, **conn.storage_options(reveal=True))
        parts = urlsplit(uri)
        assert parts.scheme == "mongodb+srv"
        assert parts.port is None

    def test_user_without_password(self) -> None:
        parts = urlsplit(_sanitise_uri("mongodb://h/db", username="u"))
        assert (parts.username, parts.password) == ("u", None)

    def test_query_options_in_the_uri_are_kept(self) -> None:
        uri = _sanitise_uri("mongodb://h/db?authSource=db&timeoutMS=10")
        query = parse_qs(urlsplit(uri).query)
        assert query["authSource"] == ["db"] and query["timeoutMS"] == ["10"]
        assert {k.lower() for k in query} == {"authsource", "timeoutms"}


@pytest.mark.parametrize(
    "model, url",
    [
        (MongoConnection, "mongodb://u:p@h/db"),
        (PostgresConnection, "postgresql://u:p@h/db"),
        (IntakeConnection, "s3://bucket/cat.yml"),
    ],
)
def test_validating_an_instance_returns_it(model: Any, url: str) -> None:
    """The ``before`` validators leave anything that isn't a mapping alone."""
    conn = model.from_url(url)
    assert model.model_validate(conn) is conn


def test_base_storage_options_are_empty(registry: Dict[str, Any]) -> None:
    class Plain(BaseConnection):
        backend = "plain-test"

        @property
        def store_uri(self) -> str:
            return self.url

    conn = Plain.from_url("plain://x")
    assert conn.storage_options() == {} and conn.storage_options(reveal=True) == {}


# ---------------------------------------------------------------------------
# PostgreSQL
# ---------------------------------------------------------------------------


class TestPostgresConnection:
    def test_defaults(self) -> None:
        conn = PostgresConnection.from_url("postgresql://h")
        assert (conn.port, conn.database, conn.db_schema) == (
            5432,
            "metadata",
            "metadata_crawler",
        )

    def test_store_uri_has_no_credentials(self) -> None:
        conn = PostgresConnection.from_url("postgresql://u:secret@h:6543/db")
        assert conn.store_uri == "postgresql+psycopg://h:6543/db"

    def test_storage_options(self) -> None:
        conn = PostgresConnection.from_url("postgresql://u:p@h/db", db_schema="s")
        assert conn.storage_options(reveal=True) == {
            "host": "h",
            "port": 5432,
            "database": "db",
            "db_schema": "s",
            "username": "u",
            "password": "p",
        }

    def test_engine_url_gets_the_credentials(self) -> None:
        pytest.importorskip("sqlalchemy")
        from metadata_crawler.api.stores.postgresql import _get_storage_url

        conn = PostgresConnection.from_url("postgresql://u:p%40ss@h/db")
        url = _get_storage_url(conn.store_uri, **conn.storage_options(reveal=True))
        import sqlalchemy as sa

        parsed = sa.engine.make_url(url)
        assert (parsed.username, parsed.password, parsed.host, parsed.database) == (
            "u",
            "p@ss",
            "h",
            "db",
        )


# ---------------------------------------------------------------------------
# What gets written into catalogues
# ---------------------------------------------------------------------------


def _catalogue_options(
    options: Dict[str, Any], shadow: Any = (), db: bool = False, path: str = ""
) -> Dict[str, Any]:
    store = SimpleNamespace(
        has_catalogue_storage=db,
        storage_options=dict(options),
        _shadow_options=list(shadow),
    )
    return IndexStore.catalogue_storage_options(store, path)  # type: ignore[arg-type]


class TestCatalogueStorageOptions:
    @pytest.mark.parametrize("key", sorted(CREDENTIAL_KEYS))
    def test_credentials_never_end_up_in_catalogues(self, key: str) -> None:
        assert key not in _catalogue_options({key: "x", "endpoint_url": "e"})

    def test_other_options_are_kept(self) -> None:
        assert _catalogue_options({"endpoint_url": "e", "retries": 3}) == {
            "endpoint_url": "e",
            "retries": 3,
        }

    def test_shadowed_options(self) -> None:
        opts = _catalogue_options({"endpoint_url": "e", "region": "r"}, ["region"])
        assert opts == {"endpoint_url": "e"}

    def test_anonymous_s3_without_credentials(self) -> None:
        assert _catalogue_options({}, path="s3://b/c.yml") == {"anon": True}

    def test_not_anonymous_with_credentials(self) -> None:
        opts = _catalogue_options({"key": "k", "secret": "s"}, path="s3://b/c.yml")
        assert opts == {}

    def test_database_stores_store_nothing(self) -> None:
        assert _catalogue_options({"endpoint_url": "e"}, db=True) == {}
