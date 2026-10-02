"""Tests for reading and merging the connections and secrets files."""

from __future__ import annotations

import re
import stat
from importlib.resources import files
from pathlib import Path
from typing import Any, Callable, Dict

import pytest
import rtoml

from metadata_crawler.api.stores.jsonlines import IntakeConnection
from metadata_crawler.api.stores.mongodb import MongoConnection
from metadata_crawler.api.stores.postgresql import PostgresConnection
from metadata_crawler.connections import (
    ConfigFiles,
    _read,
    default_path,
    init_template,
    merge_secrets,
    read_configfiles,
    resolve_relative_locations,
)
from metadata_crawler.types import StoresConfig

WriteToml = Callable[..., Path]


# ---------------------------------------------------------------------------
# default_path
# ---------------------------------------------------------------------------


class TestDefaultPath:
    """Each file has its own environment variable and user config default."""

    @pytest.mark.parametrize("name", ["connections", "secrets"])
    def test_user_config_dir(self, config_home: Path, name: Any) -> None:
        assert default_path(name) == config_home / f"{name}.toml"

    @pytest.mark.parametrize(
        "name, env",
        [("connections", "MDC_CONFIG_PATH"), ("secrets", "MDC_SECRETS_PATH")],
    )
    def test_environment_variable(
        self,
        config_home: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        name: Any,
        env: str,
    ) -> None:
        monkeypatch.setenv(env, str(tmp_path / "custom.toml"))
        assert default_path(name) == tmp_path / "custom.toml"

    def test_variables_are_independent(
        self, config_home: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Setting the config variable must not move the secrets file."""
        monkeypatch.setenv("MDC_CONFIG_PATH", str(tmp_path / "c.toml"))
        assert default_path("secrets") == config_home / "secrets.toml"

    def test_tilde_is_expanded(
        self, config_home: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("MDC_CONFIG_PATH", "~/conf.toml")
        assert default_path("connections") == Path.home() / "conf.toml"


# ---------------------------------------------------------------------------
# init_template
# ---------------------------------------------------------------------------


class TestInitTemplate:
    """Templates are only written on request and never silently replaced."""

    @pytest.mark.parametrize("name, mode", [("connections", 0o640), ("secrets", 0o600)])
    def test_creates_with_permissions(
        self, config_home: Path, name: Any, mode: int
    ) -> None:
        path = init_template(name)
        assert path == config_home / f"{name}.toml"
        assert stat.S_IMODE(path.stat().st_mode) == mode
        template = files("metadata_crawler").joinpath(f"connections/{name}.toml")
        assert path.read_bytes() == template.read_bytes()

    def test_refuses_to_overwrite(self, config_home: Path) -> None:
        path = init_template("secrets")
        path.write_text("[mine]\nkey = 'value'\n")
        with pytest.raises(FileExistsError, match="already exists"):
            init_template("secrets")
        assert "mine" in path.read_text()

    def test_force_replaces_and_fixes_permissions(self, config_home: Path) -> None:
        path = init_template("secrets")
        path.write_text("changed")
        path.chmod(0o644)
        init_template("secrets", force=True)
        assert "changed" not in path.read_text()
        assert stat.S_IMODE(path.stat().st_mode) == 0o600

    def test_explicit_target(self, config_home: Path, tmp_path: Path) -> None:
        target = tmp_path / "deep" / "dir" / "c.toml"
        assert init_template("connections", target=target) == target
        assert target.is_file()
        assert not (config_home / "connections.toml").exists()


# ---------------------------------------------------------------------------
# resolve_relative_locations
# ---------------------------------------------------------------------------


class TestResolveRelativeLocations:
    """Local locations become independent of the working directory."""

    def test_relative_paths(self, tmp_path: Path) -> None:
        config: Dict[str, Any] = {
            "a": {"uri": "cats/cat.yml"},
            "b": {"url": "../up.yml"},
            "c": {"path": "./c.yml"},
        }
        resolve_relative_locations(config, tmp_path / "conf")
        assert config["a"]["uri"] == str(tmp_path / "conf" / "cats" / "cat.yml")
        assert config["b"]["url"] == str(tmp_path / "up.yml")
        assert config["c"]["path"] == str(tmp_path / "conf" / "c.yml")

    @pytest.mark.parametrize(
        "value",
        [
            "/abs/cat.yml",
            "~/cat.yml",
            "$HOME/cat.yml",
            "s3://bucket/cat.yml",
            "postgresql://host/db",
            "file:///abs/cat.yml",
        ],
    )
    def test_left_alone(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, value: str
    ) -> None:
        monkeypatch.setenv("HOME", "/home/someone")
        config: Dict[str, Any] = {"x": {"uri": value}}
        resolve_relative_locations(config, tmp_path)
        assert config["x"]["uri"] == value

    def test_ignores_other_shapes(self, tmp_path: Path) -> None:
        config: Dict[str, Any] = {
            "scalar": 1,
            "no_location": {"description": "x"},
            "not_a_string": {"uri": 42},
        }
        resolve_relative_locations(config, tmp_path)
        assert config == {
            "scalar": 1,
            "no_location": {"description": "x"},
            "not_a_string": {"uri": 42},
        }


# ---------------------------------------------------------------------------
# merge_secrets
# ---------------------------------------------------------------------------


class TestMergeSecrets:
    """Credentials are added to stores; secrets tables never become stores."""

    @staticmethod
    def _config() -> Dict[str, Any]:
        return {
            "prod": {"uri": "postgresql://h/db"},
            "dev-pg": {"uri": "postgresql://localhost/m", "secrets": "dev"},
            "dev-mongo": {"uri": "mongodb://localhost/m", "secrets": "dev"},
            "scratch": {"uri": "/work/cat.yml"},
        }

    SECRETS: Dict[str, Any] = {
        "prod": {"username": "ab1234", "password": "pw"},
        "dev": {"username": "metadata", "password": "secret"},
        "swift-dkrz": {"os_password": "for a data source"},
    }

    def test_by_name(self) -> None:
        config = self._config()
        merge_secrets(config, self.SECRETS)
        assert config["prod"]["password"] == "pw"

    def test_by_reference(self) -> None:
        config = self._config()
        merge_secrets(config, self.SECRETS)
        assert config["dev-pg"]["username"] == "metadata"
        assert config["dev-mongo"]["password"] == "secret"

    def test_secrets_tables_are_not_stores(self) -> None:
        config = self._config()
        merge_secrets(config, self.SECRETS)
        assert set(config) == {"prod", "dev-pg", "dev-mongo", "scratch"}

    def test_store_without_secrets(self) -> None:
        config = self._config()
        merge_secrets(config, self.SECRETS)
        assert config["scratch"] == {"uri": "/work/cat.yml"}

    def test_reference_wins_over_own_name(self) -> None:
        config: StoresConfig = {"prod": {"uri": "postgresql://h/db", "secrets": "dev"}}
        merge_secrets(config, self.SECRETS)
        assert config["prod"]["username"] == "metadata"

    def test_secrets_win_over_the_connections_file(self) -> None:
        config: StoresConfig = {"prod": {"uri": "postgresql://h/db", "username": "old"}}
        merge_secrets(config, self.SECRETS)
        assert config["prod"]["username"] == "ab1234"

    def test_missing_reference_is_an_error(self) -> None:
        with pytest.raises(ValueError, match="'nope'"):
            merge_secrets({"x": {"uri": "a", "secrets": "nope"}}, {})

    def test_non_table_entries_are_skipped(self) -> None:
        config: Dict[str, Any] = {"version": 1}
        merge_secrets(config, {"version": {"x": 1}})
        assert config == {"version": 1}


# ---------------------------------------------------------------------------
# _read
# ---------------------------------------------------------------------------


class TestRead:
    """Finding, parsing and merging the two files."""

    def test_nothing_configured(self, config_home: Path) -> None:
        assert _read() == {}
        assert not config_home.exists(), "reading must not create files"

    def test_default_locations(self, config_home: Path) -> None:
        config_home.mkdir(parents=True)
        (config_home / "connections.toml").write_text(
            '[prod]\nuri = "postgresql://h/db"\n'
        )
        secrets = config_home / "secrets.toml"
        secrets.write_text('[prod]\npassword = "pw"\n')
        secrets.chmod(0o600)
        assert _read() == {"prod": {"uri": "postgresql://h/db", "password": "pw"}}

    def test_environment_variables(
        self,
        config_home: Path,
        write_toml: WriteToml,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        conns = write_toml("c.toml", '[prod]\nuri = "postgresql://h/db"\n', 0o644)
        secrets = write_toml("s.toml", '[prod]\nusername = "u"\n')
        monkeypatch.setenv("MDC_CONFIG_PATH", str(conns))
        assert _read() == {"prod": {"uri": "postgresql://h/db"}}
        monkeypatch.setenv("MDC_SECRETS_PATH", str(secrets))
        assert _read() == {"prod": {"uri": "postgresql://h/db", "username": "u"}}

    def test_explicit_paths_win(
        self,
        config_home: Path,
        write_toml: WriteToml,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        env = write_toml("env.toml", '[env]\nuri = "/env.yml"\n', 0o644)
        explicit = write_toml("explicit.toml", '[explicit]\nuri = "/e.yml"\n', 0o644)
        monkeypatch.setenv("MDC_CONFIG_PATH", str(env))
        assert set(_read(store_path=explicit)) == {"explicit"}

    @pytest.mark.parametrize("which", ["store_path", "secrets_path"])
    def test_explicit_missing_file_is_an_error(
        self, config_home: Path, tmp_path: Path, which: str
    ) -> None:
        with pytest.raises(FileNotFoundError, match="missing.toml"):
            _read(**{which: tmp_path / "missing.toml"})

    def test_invalid_toml(self, config_home: Path, write_toml: WriteToml) -> None:
        broken = write_toml("c.toml", "[prod\nuri = 1\n", 0o644)
        with pytest.raises(ValueError, match="c.toml"):
            _read(store_path=broken)

    def test_relative_locations_follow_the_config_file(
        self,
        config_home: Path,
        write_toml: WriteToml,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        conns = write_toml("conf/c.toml", '[local]\nuri = "cats/cat.yml"\n', 0o644)
        elsewhere = tmp_path / "elsewhere"
        elsewhere.mkdir()
        monkeypatch.chdir(elsewhere)
        config = _read(store_path=conns)
        assert config["local"]["uri"] == str(tmp_path / "conf" / "cats" / "cat.yml")

    def test_secrets_reference(self, config_home: Path, write_toml: WriteToml) -> None:
        conns = write_toml(
            "c.toml",
            '[a]\nuri = "postgresql://h/a"\nsecrets = "shared"\n'
            '[b]\nuri = "mongodb://h/b"\nsecrets = "shared"\n',
            0o644,
        )
        secrets = write_toml("s.toml", '[shared]\nusername = "u"\npassword = "p"\n')
        config = _read(store_path=conns, secrets_path=secrets)
        assert set(config) == {"a", "b"}
        assert config["a"]["password"] == config["b"]["password"] == "p"

    def test_readable_secrets_are_refused(
        self, config_home: Path, write_toml: WriteToml
    ) -> None:
        from metadata_crawler.connections.vault import SecretsError

        secrets = write_toml("s.toml", '[x]\npassword = "p"\n', 0o644)
        with pytest.raises(SecretsError, match="chmod 600"):
            _read(secrets_path=secrets)

    def test_encrypted_secrets_at_the_default_location(
        self,
        config_home: Path,
        write_toml: WriteToml,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Only ``secrets.toml.age`` exists; it must still be used."""
        pyrage = pytest.importorskip("pyrage")
        config_home.mkdir(parents=True)
        (config_home / "connections.toml").write_text(
            '[prod]\nuri = "postgresql://h/db"\n'
        )
        age = config_home / "secrets.toml.age"
        age.write_bytes(pyrage.passphrase.encrypt(b'[prod]\npassword = "pw"\n', "pp"))
        age.chmod(0o600)
        monkeypatch.setenv("MDC_SECRETS_PASSPHRASE", "pp")
        assert _read()["prod"]["password"] == "pw"


# ---------------------------------------------------------------------------
# read_configfiles and ConfigFiles
# ---------------------------------------------------------------------------


@pytest.fixture()
def config_files(config_home: Path, write_toml: WriteToml) -> Dict[str, Path]:
    conns = write_toml(
        "c.toml",
        """
[prod]
uri = "postgresql://db.example.org:6543/metadata"
db_schema = "mdc"

[mongo]
uri = "mongodb://mongo.example.org/metadata"
tls = true
secrets = "shared"

[waterpark]
uri = "s3://metadata/cat.yml"
endpoint_url = "https://s3.example.org"

[scratch]
uri = "cats/scratch.yml"

[explicit-backend]
uri = "s3://bucket/x.yml"
backend = "intake"
""",
        0o644,
    )
    secrets = write_toml(
        "s.toml",
        """
[prod]
user = "ab1234"
passwd = "pw"

[shared]
username = "m"
password = "mpw"

[waterpark]
key = "AK"
secret = "SK"
""",
    )
    return {"store_path": conns, "secrets_path": secrets}


class TestReadConfigfiles:
    """The context manager yields validated connections and closes them."""

    def test_backends_are_inferred(self, config_files: Dict[str, Path]) -> None:
        with read_configfiles(**config_files) as cfg:
            assert isinstance(cfg["prod"], PostgresConnection)
            assert isinstance(cfg["mongo"], MongoConnection)
            assert isinstance(cfg["waterpark"], IntakeConnection)
            assert isinstance(cfg["scratch"], IntakeConnection)
            assert isinstance(cfg["explicit-backend"], IntakeConnection)

    def test_values_arrive(self, config_files: Dict[str, Path]) -> None:
        with read_configfiles(**config_files) as cfg:
            prod = cfg.get("prod", PostgresConnection)
            assert (prod.host, prod.port, prod.database, prod.db_schema) == (
                "db.example.org",
                6543,
                "metadata",
                "mdc",
            )
            assert prod.username == "ab1234"
            assert prod.password is not None
            assert prod.password.get_secret_value() == "pw"
            mongo = cfg.get("mongo", MongoConnection)
            assert mongo.username == "m"
            assert mongo.uri_options() == {"tls": "true"}

    def test_closed_after_the_block(self, config_files: Dict[str, Path]) -> None:
        with read_configfiles(**config_files) as cfg:
            assert "prod" in cfg
        with pytest.raises(RuntimeError, match="closed"):
            cfg["prod"]
        assert list(cfg) == []

    def test_closed_after_an_exception(self, config_files: Dict[str, Path]) -> None:
        with pytest.raises(ZeroDivisionError):
            with read_configfiles(**config_files) as cfg:
                1 / 0
        with pytest.raises(RuntimeError):
            cfg["prod"]

    def test_invalid_entry_fails_on_enter(
        self, config_home: Path, write_toml: WriteToml
    ) -> None:
        conns = write_toml(
            "c.toml", '[x]\nuri = "postgresql://h/db"\ntypo = 1\n', 0o644
        )
        with pytest.raises(Exception, match="typo"):
            with read_configfiles(store_path=conns):
                pass

    def test_without_any_config(self, config_home: Path) -> None:
        with read_configfiles() as cfg:
            assert list(cfg) == []
            assert isinstance(cfg.resolve("mongodb://x/y"), MongoConnection)


class TestConfigFiles:
    """Validation and lookup of the merged entries."""

    def test_location_is_required(self) -> None:
        with pytest.raises(ValueError, match="'x'"):
            ConfigFiles({"x": {"description": "no location"}})

    @pytest.mark.parametrize("key", ["uri", "url", "path"])
    def test_all_location_keys(self, key: str) -> None:
        cfg = ConfigFiles({"x": {key: "/work/cat.yml"}})
        assert cfg["x"].url == "/work/cat.yml"
        cfg = ConfigFiles({"y": {key: "postgresql://h/db"}})
        assert isinstance(cfg["y"], PostgresConnection)
        assert cfg.get("y", PostgresConnection).host == "h"

    def test_unknown_backend(self) -> None:
        with pytest.raises(ValueError, match="unknown backend 'solr'"):
            ConfigFiles({"x": {"uri": "a", "backend": "solr"}})

    def test_unknown_name(self) -> None:
        cfg = ConfigFiles({"a": {"uri": "/a.yml"}, "b": {"uri": "/b.yml"}})
        with pytest.raises(KeyError, match="configured: a, b"):
            cfg["nope"]
        with pytest.raises(KeyError, match="configured: none"):
            ConfigFiles({})["nope"]

    def test_contains_and_iter(self) -> None:
        cfg = ConfigFiles({"a": {"uri": "/a.yml"}})
        assert "a" in cfg and "b" not in cfg
        assert list(cfg) == ["a"]

    def test_get_checks_the_type(self) -> None:
        cfg = ConfigFiles({"a": {"uri": "/a.yml"}})
        assert isinstance(cfg.get("a", IntakeConnection), IntakeConnection)
        with pytest.raises(TypeError, match="intake store"):
            cfg.get("a", PostgresConnection)

    def test_resolve(self) -> None:
        cfg = ConfigFiles({"prod": {"uri": "postgresql://h/db"}})
        assert cfg.resolve("prod") is cfg["prod"]
        overridden = cfg.resolve("prod", db_schema="other")
        assert isinstance(overridden, PostgresConnection)
        assert overridden.db_schema == "other"
        assert cfg.get("prod", PostgresConnection).db_schema == "metadata_crawler"
        assert isinstance(cfg.resolve("mongodb://x/y"), MongoConnection)
        adhoc = cfg.resolve(Path("/work/cat.yml"))
        assert isinstance(adhoc, IntakeConnection) and adhoc.url == "/work/cat.yml"

    def test_resolve_after_close(self) -> None:
        cfg = ConfigFiles({})
        cfg.close()
        with pytest.raises(RuntimeError):
            cfg.resolve("/a.yml")


# ---------------------------------------------------------------------------
# The shipped templates
# ---------------------------------------------------------------------------


def _template(name: str) -> str:
    return files("metadata_crawler").joinpath(f"connections/{name}.toml").read_text()


def _uncomment(text: str) -> str:
    """Turn the commented examples into active TOML."""
    return "\n".join(
        re.sub(r"^# (\[[^\]]+\]|[A-Za-z_][A-Za-z0-9_]* = .*)$", r"\1", line)
        for line in text.splitlines()
    )


class TestTemplates:
    """The templates must stay inert and match what the code accepts."""

    @pytest.mark.parametrize("name", ["connections", "secrets"])
    def test_inert_as_shipped(self, name: str) -> None:
        assert rtoml.loads(_template(name)) == {}

    def test_examples_validate(self, tmp_path: Path) -> None:
        conns = rtoml.loads(_uncomment(_template("connections")))
        secrets = rtoml.loads(_uncomment(_template("secrets")))
        assert conns, "the template should contain examples"
        resolve_relative_locations(conns, tmp_path)
        merge_secrets(conns, secrets)
        cfg = ConfigFiles(conns)
        assert set(cfg) == set(conns)

    @pytest.mark.parametrize("name", ["connections", "secrets"])
    def test_documented_environment_variables_exist(self, name: str) -> None:
        documented = set(re.findall(r"\bMDC_[A-Z_]+\b", _template(name)))
        known = {
            "MDC_CONFIG_PATH",
            "MDC_SECRETS_PATH",
            "MDC_SECRETS_PASSPHRASE",
            "MDC_AGE_IDENTITY",
        }
        assert documented <= known, f"unknown variables: {documented - known}"
