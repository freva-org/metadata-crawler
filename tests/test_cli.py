# tests/test_cli.py
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from functools import partial
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

import pytest
from pytest_mock import MockerFixture

import metadata_crawler.cli as mc_cli

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
