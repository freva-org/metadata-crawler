"""Collection of aync data ingest classes."""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Optional

import aiohttp
from motor.motor_asyncio import AsyncIOMotorClient

from ..api.cli import Annotated, cli_function, cli_parameter
from ..logger import logger
from .base import BaseIngest


class MongoIngest(BaseIngest):
    """Ingest metadata into a mongoDB server."""

    name = "mongo"

    _CliConfig = {
        "add": {
            "help": "Add metadata to the mongoDB metadata server.",
            "args": {},
        },
        "del": {
            "help": "Remove metadata from the mongoDB server.",
            "args": {
                "facets": {
                    "name": ("-f", "--facets"),
                    "type": str,
                    "nargs": 2,
                    "action": "append",
                    "help": "Search facets matching the delete query.",
                }
            },
        },
    }

    def __init__(
        self,
        uri: str = "",
        batch_size: int = 2500,
        **kwargs: str,
    ) -> None:
        super().__init__(batch_size)
        uri = (uri or "mgongo://localhost:27017").rstrip("/")
        if "timeout=" not in uri:
            uri += "/?timeoutMS=500000"
        self.client = AsyncIOMotorClient(uri)
        self.db = self.client.get_default_database()

    @cli_function(
        help="Add metadata to the mongoDB metadata server.",
    )
    async def index(
        self,
        *,
        server: str = cli_parameter(
            "-s",
            "--server",
            help="The <host>:<port> to the mngoDB server",
            type=str,
        ),
        core: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> None:
        submit: dict[str, list[dict[str, str | list[str] | list[float]]]] = (
            defaultdict(list)
        )
        pass

    @cli_function(
        help="Remove metadata from the mongoDB metadata server.",
    )
    async def delete(
        self,
        *,
        server: Annotated[
            str,
            cli_parameter(
                "-s",
                "--server",
                help="The <host>:<port> to the mongoDB server",
                type=str,
            ),
        ] = None,
        facets: Annotated[
            list[tuple[str, str]],
            cli_parameter(
                "-f",
                "--facets",
                type=str,
                nargs=2,
                action="append",
                help="Search facets matching the delete query.",
            ),
        ] = None,
    ) -> None:
        query = []
