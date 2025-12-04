"""Interact with the a posix file system."""

from __future__ import annotations

import pathlib
from typing import AsyncIterator, Sequence, Union

from metadata_crawler._helper import posix as _posix_rs

from ..api.storage_backend import MetadataType, PathTemplate


class PosixPath(PathTemplate):
    """Class to interact with a Posix file system."""

    _fs_type = "posix"

    async def is_dir(self, path: Union[str, pathlib.Path]) -> bool:
        """Check if a given path is a directory object on the storage system.

        Parameter
        ---------
        path : str, pathlib.Path
            Path of the object store

        Returns
        -------
        bool: True if path is dir object, False if otherwise or doesn't exist
        """
        return await _posix_rs.is_dir(str(path))

    async def is_file(self, path: Union[str, pathlib.Path]) -> bool:
        """Check if a given path is a file object on the storage system.

        Parameter
        ---------
        path : str, pathlib.Path
            Path of the object store


        Returns
        -------
        bool: True if path is file object, False if otherwise or doesn't exist
        """
        return await _posix_rs.is_file(str(path))

    async def iterdir(self, path: Union[str, pathlib.Path]) -> AsyncIterator[str]:
        """Get all sub directories from a given path.

        Parameter
        ---------
        path : str, pathlib.Path
            Path of the object store

        Yields
        ------
        str: 1st level sub directory
        """
        entries: Sequence[str] = await _posix_rs.iterdir(str(path))
        for entry in entries:
            yield entry

    async def rglob(
        self, path: Union[str, pathlib.Path], glob_pattern: str = "*"
    ) -> AsyncIterator[MetadataType]:
        """Search recursively for paths matching a given glob pattern.

        Parameter
        ---------
        path : str, pathlib.Path
            Path of the object store
        glob_pattern: str
            Pattern that the target files must match

        Yields
        ------
        MetadataType: Path of the object store that matches the glob pattern.
        """
        for p in await _posix_rs.rglob(
            str(path),
            glob_pattern,
            list(self.suffixes) or None,
        ):
            yield MetadataType(path=p, metadata={})

    def path(self, path: Union[str, pathlib.Path]) -> str:
        """Get the full path (including any schemas/netlocs).

        Parameters
        ----------
        path: str, pathlib.Path
            Path of the object store

        Returns
        -------
        str:
            URI of the object store
        """
        return str(pathlib.Path(path).absolute())

    def uri(self, path: Union[str, pathlib.Path]) -> str:
        """Get the uri of the object store.

        Parameters
        ----------
        path: str, pathlib.Path
            Path of the object store

        Returns
        -------
        str:
            URI of the object store
        """
        return f"file://{pathlib.Path(path).absolute()}"
