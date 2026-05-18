"""Command line interface for the data crawler."""

from __future__ import annotations

import argparse
import asyncio
import inspect
import os
import sys
from functools import partial
from json import dumps
from pathlib import Path
from typing import (
    Annotated,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
    get_args,
    get_origin,
    get_type_hints,
)

import yaml
from rich_argparse import ArgumentDefaultsRichHelpFormatter

from metadata_crawler import add, delete, get_config, glance_metadata, index

from ._version import __version__
from .api.metadata_stores import (
    CatalogueBackends,
    CatalogueBackendType,
)
from .api.stores import IndexName
from .backends.intake import IntakePath
from .logger import (
    THIS_NAME,
    apply_verbosity,
    logger,
)
from .utils import exception_handler, load_plugins

StorageScalar = Union[str, int, float, bool]
StorageOptions = Dict[str, StorageScalar]
KwargValue = Union[str, int, float, Path, StorageOptions, List[str], List[int], None]


def walk_catalogue(
    path: str,
    storage_options: Optional[Dict[str, Any]] = None,
    **kwargs: Any,
) -> int:
    """Recursively traverse an intake catalogue.

    Parameters
    ^^^^^^^^^^

    path:
        The path to the intake catalogue
    storage_options:
        Optional configuration passed to open catalogues residing on non posix
        storage backends, such as S3/MinIO
    """

    async def _walk(path: str, **storage_options: Any) -> int:
        num_items = 0
        ip = IntakePath(**storage_options)
        async for md in ip.walk(path):
            print(md)
            num_items += 1
        return num_items

    storage_options = storage_options or {}
    return asyncio.run(_walk(path, **storage_options))


def _flatten(inp: Union[List[str], List[List[str]]]) -> List[str]:

    out = []
    for item in inp:
        out += item if isinstance(item, list) else [item]
    return out


def _get_storage_option_from_env() -> List[Tuple[str, str]]:
    """Construct storage options from environment vars."""
    options: List[Tuple[str, str]] = []
    for env, _, var in [
        s.strip().partition(":")
        for s in os.getenv("MDC_STORAGE_OPTIONS", "").split(",")
        if s.strip()
    ]:
        options.append((env, var))
    return options


def _process_storage_option(option: str) -> Union[str, bool, int, float]:

    if option.lower() in ("false", "true", "yes", "y"):
        return option.lower() in ["true", "yes", "y"]
    try:
        return int(option)
    except ValueError:
        pass
    try:
        return float(option)
    except ValueError:
        pass
    return option


def display_config(
    *config: Union[Path, str],
    json: bool = False,
    no_comments: bool = False,
    **kwargs: Any,
) -> None:
    """Display the config file."""
    cfg = get_config(*config, preserve_comments=no_comments is False)
    if json is False:
        print(cfg.dumps())
    else:
        print(dumps(cfg.merged_doc, indent=3))


def _glance_metadata(
    store: Union[str, Path],
    json: bool = False,
    backend: Optional[CatalogueBackendType] = None,
    verbose: int = 0,
    **kwargs: Any,
) -> None:
    """Display the metadata content of a catalogue/database."""
    meta = dumps(
        glance_metadata(store, backend=backend, **kwargs.get("storage_options", {}))
    )
    if json:
        print(meta)
    else:
        print(yaml.dump(yaml.safe_load(meta), default_flow_style=False))


class ArgParse:
    """Command line interface definition.

    Properties
    ----------
    kwargs: dict[str, Union[str, float, Path]]
            property holding all parsed keyword arguments.
    """

    kwargs: Optional[Dict[str, KwargValue]] = None
    verbose: int = 0
    epilog: str = (
        "See also https://metadata-crawler.readthedocs.io for a detailed documentation."
    )

    def __init__(self) -> None:
        """Instantiate the CLI class."""
        self.verbose: int = 0
        self.parser = argparse.ArgumentParser(
            prog=THIS_NAME,
            description="Add/Remove metadata to/from a metadata index.",
            formatter_class=ArgumentDefaultsRichHelpFormatter,
            epilog=self.epilog,
        )
        self.parser.add_argument(
            "-V",
            "--version",
            action="version",
            version=f"%(prog)s {__version__}",
            help="Print the version end exit",
        )
        self._add_general_config_to_parser(self.parser)
        self.subparsers = self.parser.add_subparsers(
            description="Collect or ingest metadata",
            required=True,
        )
        self._add_config_parser()
        self._add_walk_catalogue()
        self._add_crawler_subcommand()
        self._add_inspect()
        self._index_submcommands()

    def _add_config_parser(self) -> None:
        parser = self.subparsers.add_parser(
            "config",
            description="Display config",
            help="Display config",
            formatter_class=ArgumentDefaultsRichHelpFormatter,
            epilog=self.epilog,
        )
        parser.add_argument(
            "-c",
            "--config",
            help="Path(s) to the config_file(s)",
            type=Path,
            action="append",
            default=None,
        )
        parser.add_argument("--json", help="Print in json format.", action="store_true")
        parser.add_argument(
            "--no-comments",
            "--drop-comments",
            help="Do not include comments in the config file.",
            action="store_true",
            default=False,
        )
        parser.set_defaults(apply_func=display_config)
        parser.add_argument(
            "-v",
            "--verbose",
            action="count",
            default=self.verbose,
            help="Increase the verbosity level.",
        )

    def _add_crawler_subcommand(self) -> None:
        """Add sub command for crawling metadata."""
        eval_config = os.getenv("EVALUATION_SYSTEM_CONFIG_DIR")
        parser = self.subparsers.add_parser(
            "add",
            description="Harvest (add) metadata",
            help="Harvest (crawl) metadata",
            formatter_class=ArgumentDefaultsRichHelpFormatter,
            epilog=self.epilog,
        )
        parser.add_argument(
            "store",
            type=str,
            help="Url or path to the metadata store",
        )
        parser.add_argument(
            "--backend",
            "--catalogue-backend",
            "-cb",
            type=str,
            help="Source type of the storage backend.",
            choices=CatalogueBackends.__members__.keys(),
            default=os.getenv("MDC_BACKEND"),
        )
        parser.add_argument(
            "--data-store-prefix",
            "--prefix",
            "--collection",
            "--table",
            type=str,
            help=(
                "Name or location of the metadata store. "
                "For jsonlines: a path prefix for the .json.gz files "
                "(relative to the catalogue YAML or absolute). "
                "For mongodb: the collection name. "
                "For sql: the table name."
            ),
            default="metadata",
        )
        parser.add_argument(
            "-c",
            "--config-file",
            "--config-dir",
            type=Path,
            help="Path(s) to the metadata config file(s)",
            action="append",
            default=[eval_config] if eval_config else None,
        )
        parser.add_argument(
            "-b",
            "--batch-size",
            type=int,
            default=25_000,
            help="Set the batch size for ingestion.",
        )
        parser.add_argument(
            "--scan-concurrency",
            "--concurrency",
            type=int,
            default=1024,
            help="Level of aync concurrency for data discovery.",
        )
        parser.add_argument(
            "-d",
            "--data-object",
            "--data-obj",
            type=str,
            help="Objects (directories or catalogue files) that are processed.",
            default=None,
            action="append",
        )
        parser.add_argument(
            "-ds",
            "--data-set",
            type=str,
            help=(
                "The name of the dataset(s) that are processed. "
                "names can contain wildcards such as ``xces-*``."
            ),
            default=None,
            action="append",
        )
        parser.add_argument(
            "--no-sweep",
            action="store_true",
            default=False,
            help=(
                "Skip removal of stale records after crawling. "
                "By default, database backends (MongoDB, PostgreSQL) "
                "remove entries older than the grace period "
                "(set via --sweep-grace-period). "
                "Use this flag for partial or incremental crawls "
                "where not all data sources are being re-discovered."
            ),
        )
        parser.add_argument(
            "--sweep-grace-period",
            type=int,
            default=os.getenv("MDC_GRACE_DAYS", "5"),
            help=(
                "Number of days to keep records before they become eligible "
                "for sweeping. Records older than this grace period are "
                "removed after a crawl. Overrides the MDC_GRACE_DAYS "
                "environment variable. Defaults to 5 days."
            ),
        )
        parser.add_argument(
            "-p",
            "--password",
            help=(
                "Ask for a password and set it to the DRS_STORAGE_PASSWD env variable."
            ),
            action="store_true",
        )
        parser.add_argument(
            "--n-procs",
            "--procs",
            help="Set the number of parallel processes for collecting.",
            type=int,
            default=None,
        )
        parser.add_argument(
            "--latest-version",
            type=str,
            default=IndexName().latest,
            help="Name of the core holding 'latest' metadata.",
        )
        parser.add_argument(
            "--all-versions",
            type=str,
            default=IndexName().all,
            help="Name of the core holding 'all' metadata versions.",
        )
        parser.add_argument(
            "--comp-level",
            "-z",
            help="Set the compression level for compressing files.",
            default=4,
            type=int,
        )
        parser.add_argument(
            "--storage-option",
            "--storage_option",
            "-s",
            help=(
                "Set additional storage options for adding metadata to the"
                "metadata store"
            ),
            action="append",
            nargs=2,
        )
        parser.add_argument(
            "--fail-under",
            help=" Fail if less than X of the discovered files could be indexed.",
            type=int,
            default=-1,
        )
        parser.add_argument(
            "--shadow",
            help=(
                "'Shadow' these storage options. This is useful to hide secrets "
                "in public data catalogues."
            ),
            action="append",
            default=None,
            nargs="+",
        )
        self._add_general_config_to_parser(parser)
        parser.set_defaults(apply_func=add)

    def _add_general_config_to_parser(self, parser: argparse.ArgumentParser) -> None:
        """Add the most common arguments to a given parser."""
        parser.add_argument(
            "-v",
            "--verbose",
            action="count",
            default=self.verbose,
            help="Increase the verbosity level.",
        )
        parser.add_argument(
            "--log-suffix",
            type=str,
            help="Add a suffix to the log file output.",
            default=None,
        )

    def _add_inspect(self) -> None:
        """Add an inspect subcommand for table metadata inspection."""
        parser = self.subparsers.add_parser(
            "glance",
            description="Inspect/glance the table metadata.",
            help="Inspect/glance the table metadata",
            formatter_class=ArgumentDefaultsRichHelpFormatter,
            epilog=self.epilog,
        )
        parser.add_argument(
            "store",
            type=str,
            help="Path/Url to the intake catalogue",
        )
        parser.add_argument(
            "--backend",
            "--catalogue-backend",
            "-cb",
            type=str,
            help="Source type of the storage backend.",
            choices=CatalogueBackends.__members__.keys(),
            default=os.getenv("MDC_BACKEND"),
        )
        parser.add_argument(
            "--storage-option",
            "--storage_option",
            "-s",
            help=(
                "Set additional storage options for adding metadata to the"
                "metadata store"
            ),
            action="append",
            nargs=2,
        )
        parser.add_argument(
            "--json",
            help="Parse output as JSON string.",
            action="store_true",
        )
        self._add_general_config_to_parser(parser)
        parser.set_defaults(apply_func=_glance_metadata)

    def _add_walk_catalogue(self) -> None:
        """Add a subcommand for walking an intake catalogue."""
        parser = self.subparsers.add_parser(
            "walk-intake",
            description="Walk an intake catalogue",
            help="Walk an intake catalogue",
            formatter_class=ArgumentDefaultsRichHelpFormatter,
            epilog=self.epilog,
        )

        parser.add_argument(
            "path",
            type=str,
            help="Path/Url to the intake catalogue",
        )
        parser.add_argument(
            "--storage-option",
            "--storage_option",
            "-s",
            help=(
                "Set additional storage options for adding metadata to the"
                "metadata store"
            ),
            action="append",
            nargs=2,
        )
        parser.add_argument(
            "-v",
            "--verbose",
            action="count",
            default=self.verbose,
            help="Increase the verbosity level.",
        )
        parser.set_defaults(apply_func=walk_catalogue)

    def _index_submcommands(self) -> None:
        """Add sub command for adding metadata to the solr server."""
        entry_point = "metadata_crawler.ingester"
        for plugin, cls in load_plugins(entry_point).items():
            cli_methods: Dict[str, Callable[..., Any]] = {}
            for name in ("index", "delete"):
                method = getattr(cls, name, None)
                if hasattr(method, "_cli_help"):
                    cli_methods[name] = cast(Callable[..., Any], method)
            if cli_methods:
                subparser = self.subparsers.add_parser(
                    plugin,
                    help=cls.__doc__,
                    description=cls.__doc__,
                    formatter_class=ArgumentDefaultsRichHelpFormatter,
                    epilog=self.epilog,
                )
                cmd_parser = subparser.add_subparsers(required=True)
            for name, method in cli_methods.items():
                parser = cmd_parser.add_parser(
                    name,
                    help=getattr(method, "_cli_help", ""),
                    description=getattr(method, "_cli_help", ""),
                    formatter_class=ArgumentDefaultsRichHelpFormatter,
                    epilog=self.epilog,
                )
                parser.add_argument(
                    "-b",
                    "--batch-size",
                    type=int,
                    default=5_000,
                    help="Set the batch size for indexing.",
                )
                parser.add_argument(
                    "--storage-option",
                    "--storage_option",
                    "-s",
                    help=(
                        "Set additional storage options for adding metadata to "
                        " the metadata store"
                    ),
                    action="append",
                    nargs=2,
                )
                parser.add_argument(
                    "--backend",
                    "--catalogue-backend",
                    "-cb",
                    type=str,
                    help="Source type of the storage backend.",
                    choices=CatalogueBackends.__members__.keys(),
                    default=os.getenv("MDC_BACKEND"),
                )
                params = inspect.signature(method).parameters
                annotations = get_type_hints(method, include_extras=True)
                for param_name, param in params.items():
                    if param_name == "self":
                        continue
                    cli_meta = None
                    base_type = None
                    ann = annotations.get(param_name, None)

                    # 1) Annotated[...] style
                    if get_origin(ann) is Annotated:
                        base_type, *extras = get_args(ann)
                        # find the dict emitted by cli_parameter()
                        cli_meta = next(
                            (e for e in extras if isinstance(e, dict) and "args" in e),
                            None,
                        )

                    # 2) default-as-parameter style
                    if (
                        cli_meta is None
                        and isinstance(param.default, dict)
                        and "args" in param.default
                    ):
                        cli_meta = param.default
                        # annotation is the base type
                        base_type = ann if ann is not inspect._empty else None

                    # if we found a cli_meta, wire it up
                    if cli_meta:
                        arg_names = cli_meta["args"]
                        add_kwargs = {k: v for k, v in cli_meta.items() if k != "args"}

                        # preserve any explicit default
                        if (
                            param.default is not inspect._empty
                            and cli_meta is not param.default
                        ):
                            add_kwargs["default"] = param.default

                        # enforce the base type if supplied
                        if base_type and "type" not in add_kwargs:
                            add_kwargs["type"] = base_type
                        parser.add_argument(*arg_names, dest=param_name, **add_kwargs)
                if name == "index":
                    parser.add_argument(
                        "metadata_stores",
                        help="File path to the metadata store.",
                        type=str,
                        nargs="*",
                    )

                    parser.set_defaults(apply_func=partial(index, index_system=plugin))
                else:
                    parser.set_defaults(apply_func=partial(delete, index_system=plugin))
                self._add_general_config_to_parser(parser)

    def parse_args(self, argv: list[str]) -> argparse.Namespace:
        """Parse the arguments for the command line interface.

        Parameters
        ----------
        argv: list[str]
            List of command line arguments that is parsed.

        Returns
        -------
        argparse.Namespace
        """
        args = self.parser.parse_args(argv)
        self.kwargs = {
            k: v
            for (k, v) in args._get_kwargs()
            if k
            not in (
                "apply_func",
                "verbose",
                "version",
                "storage-option",
                "storage_option",
                "shadow",
            )
        }
        storage_option_pairs: List[Tuple[str, str]] = _get_storage_option_from_env() + (
            getattr(args, "storage_option", None) or []
        )
        so: StorageOptions = {}
        for option, value in storage_option_pairs:
            so[option] = _process_storage_option(value)
        if getattr(args, "shadow", None) or []:
            self.kwargs["shadow"] = _flatten(args.shadow)
        self.kwargs["storage_options"] = so
        self.verbose = args.verbose
        self.kwargs["verbosity"] = self.verbose
        return args


def _run(
    parser: argparse.Namespace,
    **kwargs: KwargValue,
) -> None:
    """Apply the parsed method."""
    old_level = apply_verbosity(
        getattr(parser, "verbose", 0), suffix=getattr(parser, "log_suffix", None)
    )
    cfg_files = (
        cast(
            Optional[Sequence[Path]],
            kwargs.pop("config_file", kwargs.pop("config", [])),
        )
        or []
    )
    try:
        parser.apply_func(*cfg_files, **kwargs)
    except Exception as error:
        exception_handler(error)
    finally:
        logger.set_level(old_level)


def cli(sys_args: list[str] | None = None) -> None:
    """Methods that creates the command line argument parser."""
    try:
        parser = ArgParse()
        args = parser.parse_args(sys_args or sys.argv[1:])
        kwargs = parser.kwargs or {}
        _run(args, **kwargs)
    except KeyboardInterrupt:
        raise SystemExit("Exiting program")
