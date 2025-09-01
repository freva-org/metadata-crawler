"""Interact with the a posix file system."""

from __future__ import annotations

import pathlib
from typing import AsyncIterator, Union

from anyio import Path

from ..api.storage_backend import MetadataType, PathTemplate


class PosixPath(PathTemplate):
    """Class to interact with a Posix file system."""

    _fs_type = "posix"

    async def is_dir(self, path: Union[str, Path, pathlib.Path]) -> bool:
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

    async def is_file(self, path: Union[str, Path, pathlib.Path]) -> bool:
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
        self, path: Union[str, Path, pathlib.Path]
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
        self, path: Union[str, Path, pathlib.Path], glob_pattern: str = "*"
    ) -> AsyncIterator[MetadataType]:
        """Search recursively for paths matching a given glob pattern.

        Parameter
        ---------
        path : str, asyncio.Path, pathlib.Path
            Path of the object store
        glob_pattern: str
            Pattern that the target files must match

        Yields
        ------
        MetadataType: Path of the object store that matches the glob pattern.
        """
        p = Path(path)
        if await self.is_file(p) or p.suffix == ".zarr":
            yield MetadataType(path=str(p), metadata={})
        else:
            async for out_f in p.rglob(glob_pattern):
                if out_f.suffix in self.suffixes:
                    yield MetadataType(path=str(out_f), metadata={})

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
        return str(pathlib.Path(path).absolute())

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
        return f"file://{pathlib.Path(path).absolute()}"
