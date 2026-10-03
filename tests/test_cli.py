# tests/test_cli.py
from __future__ import annotations

import asyncio
import stat
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from typing import (
    Annotated,
    Any,
    AsyncIterator,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    cast,
)

import metadata_crawler.cli as mc_cli
import pytest
from metadata_crawler.api.stores.base import BaseConnection
from metadata_crawler.api.stores.jsonlines import IntakeConnection
from metadata_crawler.api.stores.mongodb import MongoConnection
from metadata_crawler.api.stores.postgresql import PostgresConnection
from metadata_crawler.connections import ConfigFiles
from pytest_mock import MockerFixture

# -----------------------------
# Helpers / fakes for patching
# -----------------------------


@dataclass
class Call:
    fn: str
    args: Tuple[Any, ...] = field(default_factory=tuple)
    kwargs: Dict[str, Any] = field(default_factory=dict)


class Recorder:
    """Collects calls for assertions."""

    def __init__(self) -> None:
        self.calls: List[Call] = []

    def record(self, fn: str) -> Callable[..., Any]:
        def _inner(*args: Any, **kwargs: Any) -> Any:
            self.calls.append(Call(fn=fn, args=args, kwargs=kwargs))
            return None

        return _inner


class FakeConfig:
    def __init__(self, doc: Dict[str, Any], perserve_comments: bool = False) -> None:
        self.merged_doc = doc

    def dumps(self) -> str:
        return "CONFIG_AS_TEXT"


class FakeIntakePath:
    def __init__(self, **storage_options: Any) -> None:
        self.storage_options = storage_options
        self.walk_called_with: Optional[str] = None

    async def walk(self, path: str) -> AsyncIterator[str]:
        # store the path so tests can assert
        self.walk_called_with = path
        for i in range(10):
            yield i


# Fake plugin class to exercise the dynamic subparsers
class DummyIngester:
    """dummy ingester"""

    # Annotated parameters -> the CLI builder reads "args" and other kwargs from the dict
    def index(
        self,
        path: Annotated[str, {"args": ["--path"], "help": "Path to data"}],
        limit: Annotated[int, {"args": ["--limit"], "help": "Limit"}] = 10,
    ) -> None:
        pass

    def delete(
        self,
        pattern: Annotated[str, {"args": ["--pattern"], "help": "Glob"}],
    ) -> None:
        pass


# Mark methods with _cli_help so your CLI includes them
DummyIngester.index._cli_help = "Index data"
DummyIngester.delete._cli_help = "Delete data"


# -----------------------------
# Tests
# -----------------------------


def _get_config(
    *_cfg: str,
    preserve_comments: bool = True,
    result: Optional[Dict[str, Any]] = None,
    **kwarg: Any,
) -> FakeConfig:
    return FakeConfig(result)


def test_crawl_parsing_and_dispatch(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    rec = Recorder()

    # Patch the functions used by the CLI
    monkeypatch.setattr(mc_cli, "add", rec.record("add"))
    monkeypatch.setattr(mc_cli, "get_config", partial(_get_config, result={"ok": True}))
    monkeypatch.setattr(
        mc_cli, "load_plugins", lambda ep: {}
    )  # no plugin subcommands in this test
    # Prevent real file logging side effects

    # Build and parse args for the "add" subcommand
    parser = mc_cli.ArgParse()
    args = parser.parse_args(
        [
            "add",
            "s3://bucket/catalog.yml",
            "--batch-size",
            "42",
            "--data-set",
            "cmip6",
            "--data-set",
            "icon",
            "--data-object",
            "foo.yaml",
            "--n-procs",
            "4",
            "-s",
            "anon",
            "true",
            "-s",
            "retries",
            "3",
            "-s",
            "timeout",
            "1.5",
            "-s",
            "endpoint_url",
            "http://minio:9000",
            "--shadow",
            "foo",
            "bar",
            "--shadow",
            "1",
            "2",
            "-v",
            "-v",
        ]
    )

    # Execute the apply function
    mc_cli._run(args, **parser.kwargs)

    # Assert that `add` was called once with parsed kwargs
    assert len(rec.calls) == 1 and rec.calls[0].fn == "add"
    kw = rec.calls[0].kwargs

    # Core arguments
    assert kw["store"] == "s3://bucket/catalog.yml"
    assert kw["batch_size"] == 42
    assert kw["n_procs"] == 4
    assert kw["data_set"] == ["cmip6", "icon"]
    assert kw["data_object"] == ["foo.yaml"]

    # Storage options should be type-coerced
    so = kw["storage_options"]
    assert so == {
        "anon": True,
        "retries": 3,
        "timeout": 1.5,
        "endpoint_url": "http://minio:9000",
    }
    # The CLI injects verbosity into kwargs
    assert kw["verbosity"] == 2


def test_glance_subcommand_prints_text(
    mocker: MockerFixture, capsys: pytest.CaptureFixture[str]
) -> None:
    # Patch config + logging
    import yaml

    mocker.patch(
        "metadata_crawler.cli.glance_metadata",
        return_value={"x": 1},
    )

    # Run full CLI entrypoint
    mc_cli.cli(["glance", "localhost", "-vvvvv"])

    out = yaml.safe_load(capsys.readouterr().out)
    assert out == {"x": 1}


def test_glance_subcommand_prints_json(
    mocker: MockerFixture, capsys: pytest.CaptureFixture[str]
) -> None:
    import json

    mocker.patch(
        "metadata_crawler.cli.glance_metadata",
        return_value={"x": 1},
    )
    mc_cli.cli(["glance", "localhost", "--json"])

    out = json.loads(capsys.readouterr().out)
    assert out == {"x": 1}


def test_config_subcommand_prints_text(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # Patch config + logging
    monkeypatch.setattr(mc_cli, "get_config", partial(_get_config, result={"x": 1}))
    monkeypatch.setattr(mc_cli, "load_plugins", lambda ep: {})

    # Run full CLI entrypoint
    mc_cli.cli(["config", "-c", "conf.toml"])

    out = capsys.readouterr().out
    assert "x" in out  # display_config prints cfg.dumps() by default


def test_config_subcommand_prints_json(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(mc_cli, "get_config", partial(_get_config, result={"x": 1}))
    monkeypatch.setattr(mc_cli, "load_plugins", lambda ep: {})

    mc_cli.cli(["config", "-c", "conf.toml", "--json"])

    out = capsys.readouterr().out
    assert '"x": 1' in out


def test_walk_intake_invokes_async_walk(monkeypatch: pytest.MonkeyPatch) -> None:
    # Patch IntakePath and asyncio.run to avoid running a real loop
    fake_ip = FakeIntakePath()

    def fake_ctor(**storage_options: Any) -> FakeIntakePath:
        # keep the one instance so we can assert inside the fake run
        nonlocal fake_ip
        return fake_ip

    def fake_run(coro: Any) -> int:
        # Execute the coroutine on a fresh loop to avoid interference
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(coro)
        finally:
            loop.close()
        return 1

    monkeypatch.setattr(mc_cli, "IntakePath", fake_ctor)  # class replacement
    monkeypatch.setattr(mc_cli.asyncio, "run", fake_run)
    monkeypatch.setattr(mc_cli, "load_plugins", lambda ep: {})

    mc_cli.cli(["walk-intake", "s3://bucket/catalog.yaml", "-s", "anon", "true"])

    # Ensure our async method got called with the path
    assert fake_ip.walk_called_with == "s3://bucket/catalog.yaml"
    assert fake_ip.storage_options == {}


def test_plugin_index_and_delete_wiring(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Validates that:
      - load_plugins() drives dynamic subparsers
      - Annotated[...] metadata is converted to argparse options
      - apply_func is bound to top-level index/delete with correct kwargs
    """
    rec = Recorder()

    # Patch top-level functions that CLI dispatches to
    monkeypatch.setattr(
        mc_cli, "index", lambda *a, **kw: rec.calls.append(Call("index", a, kw))
    )
    monkeypatch.setattr(
        mc_cli, "delete", lambda *a, **kw: rec.calls.append(Call("delete", a, kw))
    )
    monkeypatch.setattr(mc_cli, "load_plugins", lambda ep: {"dummy": DummyIngester})

    # Build CLI with plugin subcommands
    parser = mc_cli.ArgParse()

    # --- index path ---
    args = parser.parse_args(
        [
            "dummy",
            "index",
            "--path",
            "/data",
            "--limit",
            "5",
            "cat1.yml",
            "cat2.yml",
            "-s",
            "anon",
            "true",
        ]
    )
    mc_cli._run(args, **parser.kwargs)

    assert rec.calls and rec.calls[-1].fn == "index"
    kw = rec.calls[-1].kwargs
    # The CLI forwards parsed params using the parameter names (dest=param_name)
    assert kw["path"] == "/data"
    assert kw["limit"] == 5
    assert kw["metadata_stores"] == ["cat1.yml", "cat2.yml"]
    assert kw["index_system"] == "dummy"

    # --- delete path ---
    args = parser.parse_args(["dummy", "delete", "--pattern", "ua_*.nc"])
    mc_cli._run(args, **parser.kwargs)

    assert rec.calls and rec.calls[-1].fn == "delete"
    kw = rec.calls[-1].kwargs
    assert kw["pattern"] == "ua_*.nc"
    assert kw["index_system"] == "dummy"


def test_run_routes_exceptions_to_exception_handler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import metadata_crawler.utils as mc_utils

    class Boom(Exception):
        pass

    def raises(**_: Any) -> None:
        raise Boom("explode")

    ns = type("NS", (), {"apply_func": raises})  # argparse.Namespace-like

    # Fake logger to capture calls
    class FakeLogger:
        def __init__(self, level: int) -> None:
            self.level = level
            self.records: list[tuple[str, Any]] = []

        def error(self, msg: str, *args, exc_info: Any = None) -> None:
            if args:
                msg = msg % args
            self.records.append((msg, exc_info))

        def critical(self, msg: str, *args, exc_info: Any = None) -> None:
            if args:
                msg = msg % args
            self.records.append((msg, exc_info))

    # --- branch 1: level > 30 -> suffix added, exc_info=None ---
    high_logger = FakeLogger(level=50)
    monkeypatch.setattr(mc_utils, "logger", high_logger, raising=True)

    with pytest.raises(SystemExit) as ei1:
        mc_cli._run(cast(mc_cli.argparse.Namespace, ns), foo=1)

    assert ei1.value.code == 1
    assert len(high_logger.records) == 1
    msg1, exc1 = high_logger.records[0]
    assert "explode" in msg1
    assert exc1 is not None

    # --- branch 2: level <= 30 -> no suffix, exc_info is exception ---
    low_logger = FakeLogger(level=10)
    monkeypatch.setattr(mc_utils, "logger", low_logger, raising=True)

    with pytest.raises(SystemExit) as ei2:
        mc_cli._run(cast(mc_cli.argparse.Namespace, ns), foo=1)

    assert ei2.value.code == 1
    assert len(low_logger.records) == 1
    msg2, exc2 = low_logger.records[0]
    assert "explode" in msg1
    assert isinstance(exc2, Boom)


def test_process_storage_option_behavior() -> None:
    assert mc_cli._process_storage_option("true") is True
    assert mc_cli._process_storage_option("FALSE") is False
    assert mc_cli._process_storage_option("3") == 3
    assert mc_cli._process_storage_option("1.25") == 1.25
    assert mc_cli._process_storage_option("http://x") == "http://x"


# -----------------------------
# remove subcommand
# -----------------------------


@pytest.fixture()
def remove_recorder(monkeypatch: pytest.MonkeyPatch) -> Recorder:
    """Patch ``remove`` before the parser binds it as ``apply_func``."""
    rec = Recorder()
    monkeypatch.setattr(mc_cli, "remove", rec.record("remove"))
    monkeypatch.setattr(mc_cli, "load_plugins", lambda ep: {})
    return rec


def _run_cli(argv: List[str]) -> Dict[str, Any]:
    parser = mc_cli.ArgParse()
    args = parser.parse_args(argv)
    mc_cli._run(args, **parser.kwargs)
    return parser.kwargs


def test_remove_parsing_and_dispatch(remove_recorder: Recorder) -> None:
    _run_cli(
        [
            "remove",
            "s3://bucket/catalog.yml",
            "-f",
            "project",
            "CMIP6",
            "--facets",
            "file",
            "*_2016*",
            "-f",
            "project",
            "obs*",
            "-s",
            "anon",
            "true",
            "--storage-option",
            "timeout",
            "1.5",
            "--log-suffix",
            "rm",
            "-vv",
        ]
    )

    assert len(remove_recorder.calls) == 1
    call = remove_recorder.calls[0]
    assert call.fn == "remove"
    assert call.args == ()  # no config files are passed to remove
    kw = call.kwargs
    assert kw["store"] == "s3://bucket/catalog.yml"
    # Order and repeated keys are preserved; globs are passed through untouched.
    assert [tuple(f) for f in kw["facets"]] == [
        ("project", "CMIP6"),
        ("file", "*_2016*"),
        ("project", "obs*"),
    ]
    assert kw["dry_run"] is False
    assert kw["storage_options"] == {"anon": True, "timeout": 1.5}
    assert kw["log_suffix"] == "rm"
    assert kw["verbosity"] == 2


def test_remove_defaults(remove_recorder: Recorder) -> None:
    _run_cli(["remove", "cat.yml"])

    kw = remove_recorder.calls[0].kwargs
    assert kw["store"] == "cat.yml"
    assert not kw["facets"]
    assert kw["dry_run"] is False
    assert kw["storage_options"] == {}
    assert kw["verbosity"] == 0


@pytest.mark.parametrize("flag", ["--dry-run", "--dry_run"])
def test_remove_dry_run_flag(remove_recorder: Recorder, flag: str) -> None:
    _run_cli(["remove", "postgresql://localhost", "-f", "project", "x", flag])

    assert remove_recorder.calls[0].kwargs["dry_run"] is True


def test_remove_via_cli_entrypoint(remove_recorder: Recorder) -> None:
    mc_cli.cli(["remove", "mongodb://localhost", "-f", "variable", "pr"])

    assert len(remove_recorder.calls) == 1
    kw = remove_recorder.calls[0].kwargs
    assert kw["store"] == "mongodb://localhost"
    assert [tuple(f) for f in kw["facets"]] == [("variable", "pr")]


@pytest.mark.parametrize(
    "argv",
    [
        ["remove"],  # store is required
        ["remove", "cat.yml", "-f", "project"],  # a facet needs key and value
    ],
)
def test_remove_invalid_arguments(remove_recorder: Recorder, argv: List[str]) -> None:
    with pytest.raises(SystemExit) as exc:
        mc_cli.ArgParse().parse_args(argv)

    assert exc.value.code == 2
    assert not remove_recorder.calls


def test_remove_kwargs_match_the_api(remove_recorder: Recorder) -> None:
    """Everything the CLI forwards must be accepted by ``metadata_crawler.remove``."""
    import inspect

    import metadata_crawler

    _run_cli(["remove", "cat.yml", "-f", "project", "x", "--dry-run", "-s", "a", "1"])

    call = remove_recorder.calls[0]
    inspect.signature(metadata_crawler.remove).bind(*call.args, **call.kwargs)


def test_remove_errors_exit_non_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    def failing_remove(**_: Any) -> None:
        raise RuntimeError("Cannot remove entries from a store opened for writing.")

    monkeypatch.setattr(mc_cli, "remove", failing_remove)
    monkeypatch.setattr(mc_cli, "load_plugins", lambda ep: {})

    with pytest.raises(SystemExit) as exc:
        mc_cli.cli(["remove", "cat.yml", "-f", "project", "x"])

    assert exc.value.code == 1


def test_remove_is_listed_in_help(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit):
        mc_cli.ArgParse().parse_args(["--help"])

    assert "remove" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# Named connections: resolution of store arguments and init-config
# ---------------------------------------------------------------------------

WriteToml = Callable[..., Path]


class ApiRecorder:
    """Stand-in for an API function; records its keyword arguments."""

    def __init__(self) -> None:
        self.calls: List[Dict[str, Any]] = []

    def __call__(self, *args: Any, **kwargs: Any) -> None:
        self.calls.append({"args": args, **kwargs})

    @property
    def last(self) -> Dict[str, Any]:
        assert self.calls, "the API function was not called"
        return self.calls[-1]


@pytest.fixture()
def config(config_home: Path, write_toml: WriteToml) -> List[str]:
    """Connections and secrets files, returned as CLI options."""
    conns = write_toml(
        "c.toml",
        """
[prod]
uri = "postgresql://db.example.org/metadata"

[mongo]
uri = "mongodb://mongo.example.org/metadata"
secrets = "shared"

[cats]
uri = "s3://bucket/cat.yml"
endpoint_url = "https://s3.example.org"
""",
        0o644,
    )
    secrets = write_toml(
        "s.toml",
        """
[prod]
username = "ab1234"
password = "pw"

[shared]
username = "m"
password = "mpw"
""",
    )
    return ["--mdc-config", str(conns), "--mdc-secrets", str(secrets)]


@pytest.fixture()
def api(monkeypatch: pytest.MonkeyPatch) -> Dict[str, ApiRecorder]:
    """Replace the API functions the CLI dispatches to (before parsing)."""
    recorders = {name: ApiRecorder() for name in ("add", "remove", "index", "delete")}
    for name, recorder in recorders.items():
        monkeypatch.setattr(mc_cli, name, recorder)
    glance = ApiRecorder()

    def fake_glance(store: Any, backend: Any = None, **options: Any) -> Dict[str, Any]:
        glance(store=store, backend=backend, options=options)
        return {"ok": True}

    monkeypatch.setattr(mc_cli, "glance_metadata", fake_glance)
    recorders["glance"] = glance
    monkeypatch.setattr(mc_cli, "load_plugins", lambda ep: {"dummy": DummyIngester})
    return recorders


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class TestIsStoreName:
    @pytest.mark.parametrize("value", ["prod", "dev-mongo", "missing.yml"])
    def test_names(self, value: str) -> None:
        assert mc_cli._is_store_name(value)

    @pytest.mark.parametrize(
        "value",
        [None, "", 3, Path("prod"), "s3://b/c.yml", "postgresql://h/db"],
    )
    def test_not_names(self, value: Any) -> None:
        assert not mc_cli._is_store_name(value)

    def test_existing_paths(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        (tmp_path / "prod").mkdir()
        (tmp_path / "cat.yml").write_text("")
        assert not mc_cli._is_store_name("prod")
        assert not mc_cli._is_store_name("cat.yml")
        assert not mc_cli._is_store_name(str(tmp_path / "cat.yml"))

    def test_existing_home_path(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("HOME", str(tmp_path))
        (tmp_path / "cat.yml").write_text("")
        assert not mc_cli._is_store_name("~/cat.yml")


class TestNeedsConfig:
    @pytest.mark.parametrize(
        "kwargs, expected",
        [
            ({}, False),
            ({"store": None}, False),
            ({"store": "s3://b/c.yml"}, False),
            ({"store": "prod"}, True),
            ({"metadata_stores": []}, False),
            ({"metadata_stores": ["s3://b/c.yml", "prod"]}, True),
            ({"metadata_stores": ("prod",)}, True),
            ({"other": "prod"}, False),
        ],
    )
    def test_needs_config(self, kwargs: Dict[str, Any], expected: bool) -> None:
        assert mc_cli._needs_config(kwargs) is expected


class TestResolveStoreArgs:
    @pytest.fixture()
    def cfg(self) -> ConfigFiles:
        return ConfigFiles(
            {
                "prod": {"uri": "postgresql://h/db"},
                "cats": {"uri": "/work/cat.yml"},
            }
        )

    def test_single(self, cfg: ConfigFiles) -> None:
        kwargs: Dict[str, Any] = {"store": "prod"}
        mc_cli.resolve_store_args(kwargs, cfg)
        assert kwargs["store"] is cfg["prod"]

    @pytest.mark.parametrize("container", [list, tuple])
    def test_sequence(self, cfg: ConfigFiles, container: type) -> None:
        kwargs: Dict[str, Any] = {
            "metadata_stores": container(["prod", "s3://b/c.yml", "cats"])
        }
        mc_cli.resolve_store_args(kwargs, cfg)
        assert kwargs["metadata_stores"] == [cfg["prod"], "s3://b/c.yml", cfg["cats"]]

    def test_none_and_missing(self, cfg: ConfigFiles) -> None:
        kwargs: Dict[str, Any] = {"store": None, "other": "prod"}
        mc_cli.resolve_store_args(kwargs, cfg)
        assert kwargs == {"store": None, "other": "prod"}

    def test_existing_path_wins(
        self, cfg: ConfigFiles, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        (tmp_path / "prod").mkdir()
        kwargs: Dict[str, Any] = {"store": "prod"}
        mc_cli.resolve_store_args(kwargs, cfg)
        assert kwargs["store"] == "prod"

    @pytest.mark.parametrize("value", ["new-catalogue.yml", "localhost"])
    def test_unknown_names_are_passed_on(self, cfg: ConfigFiles, value: str) -> None:
        """A catalogue that doesn't exist yet, or a database host, stays as is."""
        kwargs: Dict[str, Any] = {"store": value, "metadata_stores": [value]}
        mc_cli.resolve_store_args(kwargs, cfg)
        assert kwargs == {"store": value, "metadata_stores": [value]}


# ---------------------------------------------------------------------------
# _run
# ---------------------------------------------------------------------------


class TestRunOpensConfig:
    @pytest.fixture()
    def opened(self, monkeypatch: pytest.MonkeyPatch) -> List[Dict[str, Any]]:
        calls: List[Dict[str, Any]] = []
        real = mc_cli.read_configfiles

        def spy(**kwargs: Any) -> Any:
            calls.append(kwargs)
            return real(**kwargs)

        monkeypatch.setattr(mc_cli, "read_configfiles", spy)
        return calls

    @pytest.mark.parametrize(
        "argv",
        [
            ["add", "s3://bucket/cat.yml"],
            ["dummy", "index", "s3://b/a.yml", "postgresql://h/db"],
            ["config"],
        ],
    )
    def test_not_for_urls(
        self,
        config_home: Path,
        api: Dict[str, ApiRecorder],
        opened: List[Dict[str, Any]],
        monkeypatch: pytest.MonkeyPatch,
        argv: List[str],
    ) -> None:
        monkeypatch.setattr(mc_cli, "display_config", lambda *a, **k: None)
        mc_cli.cli(argv)
        assert opened == []

    def test_not_for_existing_paths(
        self,
        config_home: Path,
        api: Dict[str, ApiRecorder],
        opened: List[Dict[str, Any]],
        tmp_path: Path,
    ) -> None:
        cat = tmp_path / "cat.yml"
        cat.write_text("")
        mc_cli.cli(["remove", str(cat), "-f", "project", "x"])
        assert opened == []
        assert api["remove"].last["store"] == str(cat)

    def test_paths_are_passed_on(
        self,
        config: List[str],
        api: Dict[str, ApiRecorder],
        opened: List[Dict[str, Any]],
    ) -> None:
        mc_cli.cli(["glance", "prod", *config])
        assert [Path(c["store_path"]).name for c in opened] == ["c.toml"]
        assert [Path(c["secrets_path"]).name for c in opened] == ["s.toml"]

    def test_environment_variables(
        self,
        config: List[str],
        api: Dict[str, ApiRecorder],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("MDC_CONFIG_PATH", config[1])
        monkeypatch.setenv("MDC_SECRETS_PATH", config[3])
        mc_cli.cli(["glance", "prod"])
        assert isinstance(api["glance"].last["store"], PostgresConnection)

    def test_without_config_option_keys(self, api: Dict[str, ApiRecorder]) -> None:
        """Namespaces without --mdc-config (older plugins, tests) still work."""
        namespace = type("NS", (), {"apply_func": api["add"]})
        mc_cli._run(namespace, store="s3://b/c.yml")  # type: ignore[arg-type]
        assert api["add"].last["store"] == "s3://b/c.yml"


# ---------------------------------------------------------------------------
# End to end through cli()
# ---------------------------------------------------------------------------


class TestCliResolution:
    def test_glance(self, config: List[str], api: Dict[str, ApiRecorder]) -> None:
        mc_cli.cli(["glance", "prod", *config])
        store = api["glance"].last["store"]
        assert isinstance(store, PostgresConnection)
        assert store.username == "ab1234"
        assert store.password is not None
        assert store.password.get_secret_value() == "pw"

    def test_glance_database_host(
        self, config: List[str], api: Dict[str, ApiRecorder]
    ) -> None:
        mc_cli.cli(["glance", "localhost", "--backend", "mongodb", *config])
        assert api["glance"].last["store"] == "localhost"
        assert api["glance"].last["backend"] == "mongodb"

    def test_remove(self, config: List[str], api: Dict[str, ApiRecorder]) -> None:
        mc_cli.cli(["remove", "mongo", "-f", "project", "x", *config])
        store = api["remove"].last["store"]
        assert isinstance(store, MongoConnection)
        assert store.username == "m"

    def test_add_named_store(
        self, config: List[str], api: Dict[str, ApiRecorder]
    ) -> None:
        mc_cli.cli(["add", "cats", *config])
        store = api["add"].last["store"]
        assert isinstance(store, IntakeConnection)
        assert store.s3.endpoint_url == "https://s3.example.org"

    def test_add_new_catalogue(
        self, config: List[str], api: Dict[str, ApiRecorder]
    ) -> None:
        """``mdc add new.yml`` creates a catalogue that doesn't exist yet."""
        mc_cli.cli(["add", "new.yml", *config])
        assert api["add"].last["store"] == "new.yml"

    def test_index_mixed_stores(
        self, config: List[str], api: Dict[str, ApiRecorder], tmp_path: Path
    ) -> None:
        cat = tmp_path / "cat.yml"
        cat.write_text("")
        mc_cli.cli(
            ["dummy", "index", "prod", str(cat), "s3://b/x.yml", "mongo", *config]
        )
        stores = api["index"].last["metadata_stores"]
        assert isinstance(stores[0], PostgresConnection)
        assert stores[1:3] == [str(cat), "s3://b/x.yml"]
        assert isinstance(stores[3], MongoConnection)
        assert api["index"].last["index_system"] == "dummy"

    def test_options_still_arrive(
        self, config: List[str], api: Dict[str, ApiRecorder]
    ) -> None:
        mc_cli.cli(["glance", "prod", "-s", "db_schema", "other", *config])
        assert api["glance"].last["options"] == {"db_schema": "other"}

    def test_config_option_keys_do_not_reach_the_api(
        self, config: List[str], api: Dict[str, ApiRecorder]
    ) -> None:
        mc_cli.cli(["remove", "prod", "-f", "project", "x", *config])
        assert "mdc_config" not in api["remove"].last
        assert "mdc_secrets" not in api["remove"].last


class TestCliErrors:
    def test_missing_config_file(
        self, config_home: Path, api: Dict[str, ApiRecorder], tmp_path: Path
    ) -> None:
        with pytest.raises(SystemExit) as exc:
            mc_cli.cli(["glance", "prod", "--mdc-config", str(tmp_path / "nope")])
        assert exc.value.code == 1
        assert api["glance"].calls == []

    def test_readable_secrets_file(
        self, config: List[str], api: Dict[str, ApiRecorder]
    ) -> None:
        Path(config[3]).chmod(0o644)
        with pytest.raises(SystemExit) as exc:
            mc_cli.cli(["glance", "prod", *config])
        assert exc.value.code == 1
        assert api["glance"].calls == []

    def test_broken_entry(
        self, config_home: Path, api: Dict[str, ApiRecorder], write_toml: WriteToml
    ) -> None:
        conns = write_toml("c.toml", '[prod]\nuri = "postgresql://h/db"\ntypo = 1\n')
        with pytest.raises(SystemExit) as exc:
            mc_cli.cli(["glance", "prod", "--mdc-config", str(conns)])
        assert exc.value.code == 1


# ---------------------------------------------------------------------------
# init-config
# ---------------------------------------------------------------------------


class TestInitConfigCommand:
    def test_creates_templates(
        self, config_home: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        mc_cli.cli(["init-config"])
        secrets = config_home / "secrets.toml"
        assert (config_home / "connections.toml").is_file()
        assert stat.S_IMODE(secrets.stat().st_mode) == 0o600
        assert capsys.readouterr().out.count("Created") == 2

    def test_keeps_existing_files(
        self, config_home: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        mc_cli.cli(["init-config"])
        (config_home / "secrets.toml").write_text("mine")
        mc_cli.cli(["init-config"])
        assert (config_home / "secrets.toml").read_text() == "mine"
        assert "Skipped" in capsys.readouterr().out

    def test_force(self, config_home: Path) -> None:
        mc_cli.cli(["init-config"])
        (config_home / "secrets.toml").write_text("mine")
        mc_cli.cli(["init-config", "--force"])
        assert (config_home / "secrets.toml").read_text() != "mine"

    def test_respects_environment_variables(
        self, config_home: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("MDC_SECRETS_PATH", str(tmp_path / "elsewhere.toml"))
        mc_cli.cli(["init-config"])
        assert (tmp_path / "elsewhere.toml").is_file()
        assert not (config_home / "secrets.toml").exists()

    @pytest.mark.parametrize("before", [True, False])
    def test_options_choose_the_targets(
        self, config_home: Path, tmp_path: Path, before: bool
    ) -> None:
        options = [
            "--mdc-config",
            str(tmp_path / "c.toml"),
            "--mdc-secrets",
            str(tmp_path / "s.toml"),
        ]
        argv = [*options, "init-config"] if before else ["init-config", *options]
        mc_cli.cli(argv)
        assert (tmp_path / "c.toml").is_file()
        assert stat.S_IMODE((tmp_path / "s.toml").stat().st_mode) == 0o600
        assert not config_home.exists() or not any(config_home.iterdir())


def test_connections_pass_through_unchanged(api: Dict[str, ApiRecorder]) -> None:
    """Callers of _run may already hand in resolved connections."""
    conn: BaseConnection = PostgresConnection.from_url("postgresql://h/db")
    namespace = type("NS", (), {"apply_func": api["remove"]})
    mc_cli._run(namespace, store=conn)  # type: ignore[arg-type]
    assert api["remove"].last["store"] is conn


def test_default_config_options(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without environment variables the options default to None."""
    monkeypatch.delenv("MDC_CONFIG_PATH", raising=False)
    monkeypatch.delenv("MDC_SECRETS_PATH", raising=False)
    monkeypatch.setattr(mc_cli, "load_plugins", lambda ep: {})
    args = mc_cli.ArgParse().parse_args(["glance", "prod"])
    assert args.mdc_config is None and args.mdc_secrets is None


@pytest.mark.parametrize(
    "argv",
    [
        ["--mdc-config", "/c.toml", "--mdc-secrets", "/s.toml", "-vv", "glance", "x"],
        ["glance", "x", "--mdc-config", "/c.toml", "--mdc-secrets", "/s.toml", "-vv"],
        [
            "--mdc-config",
            "/c.toml",
            "-v",
            "glance",
            "x",
            "--mdc-secrets",
            "/s.toml",
            "-v",
        ],
    ],
)
def test_general_options_before_and_after_the_sub_command(
    monkeypatch: pytest.MonkeyPatch, argv: List[str]
) -> None:
    """A sub command must not reset options given before it."""
    monkeypatch.setattr(mc_cli, "load_plugins", lambda ep: {})
    args = mc_cli.ArgParse().parse_args(argv)
    assert args.mdc_config == Path("/c.toml")
    assert args.mdc_secrets == Path("/s.toml")
    assert args.verbose in (1, 2)
