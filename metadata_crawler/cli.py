"""Command line interface for the data crawler."""

from __future__ import annotations

import argparse
import asyncio
import inspect
import logging
import os
import sys
import time
from datetime import datetime
from functools import partial
from pathlib import Path
from typing import Annotated, Union, cast, get_args, get_origin, get_type_hints

import uvloop

from ._version import __version__
from .api.config import ConfigMerger
from .logger import THIS_NAME, add_file_handle, logger, set_log_level
from .run import async_add, call
from .utils import exception_handler, load_plugins

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


async def display_config(config: Path | None) -> None:
    """Display the config file."""

    default_config = Path(__file__).parent / "drs_config.toml"
    cfg = ConfigMerger(config or default_config)
    print(cfg.dumps())


class ArgParse:
    """Command line interface definition.

    Properties
    ----------
    kwargs: dict[str, Union[str, float, Path]]
            property holding all parsed keyword arguments.
    """

    kwargs: dict[str, Union[str, float, Path, int]] = {}
    verbose: int = 0
    epilog: str = (
        "See also "
        "https://freva.gitlab-pages.dkrz.de/metadata-crawler-source/docs/"
        " for a detailed documentation."
    )

    def __init__(self) -> None:
        """Instantiate the CLI class."""
        self.verbose: int = 0
        self.parser = argparse.ArgumentParser(
            prog=THIS_NAME,
            description="Add/Remove metadata to/from a metadata index.",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
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
        self.parser.add_argument(
            "--cron",
            help=(
                "Run the crawler on a regular basis in cron mode, the "
                "argument should follow a crontab like entry. The system"
                " will then run the command according to the crontab like "
                "entry, for example: */10 9-17 1 * *"
                "if None is given (default) then cron mode won't be used."
            ),
            default=None,
            type=str,
        )
        self.subparsers = self.parser.add_subparsers(
            description="Collect or ingest metadata",
            required=True,
        )
        self._add_config_parser()
        self._add_crawler_subcommand()
        self._index_submcommands()

    def _add_config_parser(self) -> None:
        parser = self.subparsers.add_parser(
            "config",
            description="Display config",
            help="Display config",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
            epilog=self.epilog,
        )
        parser.add_argument(
            "-c",
            "--config",
            help="Path to the config_file",
            type=Path,
        )
        parser.set_defaults(apply_func=display_config)

    def _add_crawler_subcommand(self) -> None:
        """Add sub command for crawling metadata."""
        parser = self.subparsers.add_parser(
            "crawl",
            description="Collect (crawl) metadata",
            help="Collect (crawl) metadata",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
            epilog=self.epilog,
        )
        parser.add_argument(
            "store",
            type=str,
            help="The metadata store, can be path on file or a mongodb url",
        )
        parser.add_argument(
            "-c",
            "--config-file",
            "--config-dir",
            type=Path,
            help="Directory holding the metadata and server settings.",
            default=os.environ.get("EVALUATION_SYSTEM_CONFIG_DIR"),
        )
        parser.add_argument(
            "-b",
            "--batch-size",
            type=int,
            default=2500,
            help="Set the batch size for ingestion.",
        )
        parser.add_argument(
            "-d",
            "--data-object",
            "--data-obj",
            type=str,
            help="Objects (directories or catalogue files) that are processed.",
            default=None,
            action="append",
        ),
        parser.add_argument(
            "-ds",
            "--data-set",
            type=str,
            help="The name of the dataset(s) that are processed.",
            default=None,
            action="append",
        )
        parser.add_argument(
            "-p",
            "--password",
            help=(
                "Ask for a password and set it to the DRS_STORAGE_PASSWD "
                "env variable."
            ),
            action="store_true",
        )
        parser.add_argument(
            "--comp-level",
            "-z",
            help="Set the compression level for compressing files.",
            default=4,
            type=int,
        )
        self._add_general_config_to_parser(parser)
        parser.set_defaults(apply_func=async_add)

    def _add_general_config_to_parser(
        self, parser: argparse.ArgumentParser
    ) -> None:
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

    def _index_submcommands(self) -> None:
        """Add sub command for adding metadata to the solr server."""
        entry_point = "metadata_crawler.ingester"
        for plugin, cls in load_plugins(entry_point).items():
            cli_methods = {}
            for name in ("index", "delete"):
                method = getattr(cls, name, None)
                if hasattr(method, "_cli_help"):
                    cli_methods[name] = method
            if cli_methods:
                subparser = self.subparsers.add_parser(
                    plugin,
                    help=cls.__doc__,
                    description=cls.__doc__,
                    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                    epilog=self.epilog,
                )
                cmd_parser = subparser.add_subparsers(required=True)
            for name, method in cli_methods.items():
                parser = cmd_parser.add_parser(
                    name,
                    help=method._cli_help,
                    description=method._cli_help,
                    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                    epilog=self.epilog,
                )
                parser.add_argument(
                    "-b",
                    "--batch-size",
                    type=int,
                    default=2500,
                    help="Set the batch size for ingestion.",
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
                            (
                                e
                                for e in extras
                                if isinstance(e, dict) and "args" in e
                            ),
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
                        add_kwargs = {
                            k: v for k, v in cli_meta.items() if k != "args"
                        }

                        # preserve any explicit default
                        if (
                            param.default is not inspect._empty
                            and cli_meta is not param.default
                        ):
                            add_kwargs["default"] = param.default

                        # enforce the base type if supplied
                        if base_type and "type" not in add_kwargs:
                            add_kwargs["type"] = base_type
                        parser.add_argument(
                            *arg_names, dest=param_name, **add_kwargs
                        )
                if name == "index":
                    parser.add_argument(
                        "catalogue_file",
                        help="File path to the metadata store.",
                        type=Path,
                    )

                self._add_general_config_to_parser(parser)
                parser.set_defaults(
                    apply_func=partial(call, index_system=plugin, method=name)
                )

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
            if k not in ("apply_func", "verbose", "version", "cron", "log_suffix")
        }
        self.verbose = args.verbose
        add_file_handle(args.log_suffix)
        set_log_level(self.log_level)
        return args

    @property
    def log_level(self) -> int:
        """Get the log level."""
        return max(logging.ERROR - 10 * self.verbose, logging.DEBUG)


def _run(
    parser: argparse.Namespace,
    **kwargs: Union[str, int, float, bool, Path, None],
) -> None:
    """Apply the parsed method."""
    try:
        uvloop.run(parser.apply_func(**kwargs))
    except Exception as error:
        exception_handler(error)


def cron_mode(
    cron: str,
    parser: argparse.Namespace,
    **kwargs: Union[str, int, float, bool, Path, None],
) -> None:
    """Run the crawler in cron mode."""
    from cron_converter import Cron

    cron_instance = Cron(cron)
    reference = datetime.now()
    schedule = cron_instance.schedule(reference)
    set_log_level(logging.INFO)
    while True:
        exec_time = schedule.next()
        logger.info("Next execution: %s", exec_time.isoformat())
        while exec_time > datetime.now():
            time.sleep(5)
        try:
            _run(parser, **kwargs)
        # This exception doesn't happen, due to the _run method catching it
        except SystemExit:  # pragma: no cover
            pass


def cli(sys_args: list[str] | None = None) -> None:
    """Methods that creates the command line argument parser."""

    try:
        parser = ArgParse()
        args = parser.parse_args(sys_args or sys.argv[1:])
        if args.cron:
            cron_mode(args.cron, args, **parser.kwargs)
        else:
            _run(args, **parser.kwargs)
    except KeyboardInterrupt:
        raise SystemExit("Exiting program")
