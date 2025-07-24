"""Interact with the INTAKE metadata catalogues."""

from __future__ import annotations

import pathlib
from typing import (
    Any,
    AsyncIterator,
    Dict,
    Literal,
    TypedDict,
)

from anyio import Path

import intake

from .base import BasePath, Metadata

DatasetKwargs = TypedDict(
    "DatasetKwargs",
    {
        "engine": str,
        "chunks": Dict[str, Any],
        "decode_times": bool,
        "combine": Literal["by_coords", "nested"],
    },
    total=False,
)


class IntakePath(BasePath):
    """Class to interact with the Intake metadata catalogues."""

    def __init__(self, **storage_options: Any) -> None:
        super().__init__(**storage_options)

    async def is_file(self, path: str | Path | pathlib.Path) -> bool:
        """Check if a given path is a file."""
        return True

    async def is_dir(self, path: str | Path | pathlib.Path) -> bool:
        """Check if a given path is a directory."""
        return False

    async def _walk_catalogue(
        self,
        cat: intake.catalog.Catalogue,
    ) -> AsyncIterator[Metadata]:

        for name in cat:
            entry = getattr(cat, name, None)
            if isinstance(entry, intake.catalog.Catalog):
                async for md in self._walk_catalogue(entry):
                    yield md
            elif isinstance(entry, intake.source.base.DataSource):
                for path in (
                    entry.urlpath
                    if isinstance(entry.urlpath, list)
                    else [entry.urlpath]
                ):
                    yield Metadata(path=path, metadata=entry.metadata)

    async def rglob(
        self, path: str | Path | pathlib.Path, glob_pattern: str = "*"
    ) -> AsyncIterator[Metadata]:
        """Go through catalogue path."""

        async for md in self._walk_catalogue(intake.open_catalog(str(path))):
            if "." + md.path.rpartition(".")[-1] in self.suffixes:
                yield md
