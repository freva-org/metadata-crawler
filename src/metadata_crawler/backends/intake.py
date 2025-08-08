"""Interact with the INTAKE metadata catalogues."""

from __future__ import annotations

import pathlib
from fnmatch import fnmatch
from typing import (
    Any,
    AsyncIterator,
    List,
    Optional,
    Union,
)

import fsspec
import intake
from anyio import Path

from .base import BasePath, Metadata


class IntakePath(BasePath):
    """Class to interact with the Intake metadata catalogues."""

    _fs_type: None

    def __init__(
        self, suffixes: Optional[List[str]] = None, **storage_options: Any
    ) -> None:
        super().__init__(suffixes=suffixes, **storage_options)

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

    async def iterdir(
        self,
        path: Union[str, Path, pathlib.Path],
    ) -> AsyncIterator[str]:
        """Get all sub directories from a given path.

        Parameter
        ---------
        path : str, asyncio.Path, pathlib.Path
            Path of the object store

        Yields
        ------
        str:
            1st level sub directory
        """
        yield str(path)

    async def rglob(
        self, path: str | Path | pathlib.Path, glob_pattern: str = "*"
    ) -> AsyncIterator[Metadata]:
        """Go through catalogue path."""

        async for md in self._walk_catalogue(intake.open_catalog(str(path))):
            if "." + md.path.rpartition(".")[-1] in self.suffixes and fnmatch(
                md.path, glob_pattern
            ):
                yield md

    def path(self, path: Union[str, Path, pathlib.Path]) -> str:
        """Get the full path (including any schemas/netlocs).

        Parameters
        ----------
        path: str, asyncio.Path, pathlib.Path
            Path of the object store

        Returns
        -------
        str:
            URI of the object store

        """
        return path

    def uri(self, path: Union[str, Path, pathlib.Path]) -> str:
        """Get the uri of the object store.

        Parameters
        ----------
        path: str, asyncio.Path, pathlib.Path
            Path of the object store

        Returns
        -------
        str:
            URI of the object store
        """

        fs_type, path = fsspec.core.split_protocol(str(path))
        fs_type = fs_type or "file"
        return "{fs_type}://{path}"

    def fs_type(self, path: Union[str, Path, pathlib.Path]) -> str:
        """Define the file system type."""
        fs_type, _ = fsspec.core.split_protocol(str(path))
        return fs_type or "posix"
