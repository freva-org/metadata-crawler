"""Interact with the a posix file system."""

from __future__ import annotations

import glob
import pathlib
from typing import Any, AsyncIterator, Dict

from anyio import Path

from .base import BasePath, Metadata


class PosixPath(BasePath):
    """Class to interact with a Posix file system."""

    def __init__(self, **storage_options: Any) -> None:
        super().__init__(**storage_options)

    async def is_dir(self, path: str | Path | pathlib.Path) -> bool:
        """Check if a given path is a directory object on the storage system.

        Parameter
        ---------
        path : str, asyncio.Path, pathlib.Path
            Path of the object store

        Returns
        -------
        bool: True if path is dir object, False if otherwise or doesn't exist
        """
        return await Path(path).is_dir()

    async def is_file(self, path: str | Path | pathlib.Path) -> bool:
        """Check if a given path is a file object on the storage system.

        Parameter
        ---------
        path : str, asyncio.Path, pathlib.Path
            Path of the object store


        Returns
        -------
        bool: True if path is file object, False if otherwise or doesn't exist

        """
        return await Path(path).is_file()

    async def iterdir(
        self, path: str | Path | pathlib.Path
    ) -> AsyncIterator[str]:
        """Get all sub directories from a given path.

        Parameter
        ---------
        path : str, asyncio.Path, pathlib.Path
            Path of the object store

        Yields
        ------
        str: 1st level sub directory
        """
        try:
            async for out_d in Path(path).iterdir():
                yield str(out_d)
        except NotADirectoryError:
            yield str(path)
        except FileNotFoundError:
            pass

    async def rglob(
        self, path: str | Path | pathlib.Path, glob_pattern: str = "*"
    ) -> AsyncIterator[str]:
        """Search recursively for paths matching a given glob pattern.

        Parameter
        ---------
        path : str, asyncio.Path, pathlib.Path
            Path of the object store
        glob_pattern: str
            Pattern that the target files must match

        Yields
        ------
        str: Path of the object store that matches the glob pattern.
        """
        async for out_f in Path(path).rglob(glob_pattern):
            if out_f.suffix in self.suffixes:
                yield Metadata(path=str(out_f))
