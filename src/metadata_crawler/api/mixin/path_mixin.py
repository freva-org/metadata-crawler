"""Definitions for path manipulatins."""

from pathlib import Path
from typing import Tuple, Union
from urllib.parse import urlsplit

import fsspec
from anyio import Path as aPath


class PathMixin:
    """Class that defines typical Path operations."""

    async def suffix(self, path: Union[str, Path, aPath]) -> str:
        """Get the suffix of a given input path.

        Parameters
        ^^^^^^^^^^
        path: str, asyncio.Path, pathlib.Path
            Path of the object store

        Returns
        ^^^^^^-
        str: The file type extension of the path.
        """
        return Path(path).suffix

    def get_fs_and_path(self, uri: str) -> Tuple[fsspec.AbstractFileSystem, str]:
        """Return (fs, path) suitable for xarray.

        Parameters
        ^^^^^^^^^^
        uri:
            Path to the object store / file name


        Returns
        ^^^^^^-
        fsspec.AbstractFileSystem, str:
            The AbstractFileSystem class and the corresponding path to the
            data store.
        """
        protocol, path = fsspec.core.split_protocol(uri)
        protocol = protocol or "file"
        path = urlsplit(uri.removeprefix(f"{protocol}://")).path
        return fsspec.filesystem(protocol), path
