"""Interact with an S3 Object Store."""

import asyncio
import pathlib
from typing import AsyncIterator, Optional, Tuple, Union, cast

import fsspec
from anyio import Path
from s3fs import S3FileSystem

from ..api.storage_backend import MetadataType, PathTemplate
from ..logger import logger


class S3Path(PathTemplate):
    """Class to interact with an S3 object store."""

    _fs_type = "s3"

    def __post_init__(self) -> None:
        self._client: Optional[S3FileSystem] = None
        self.storage_options = self.storage_options or {"anon": True}

    async def close(self) -> None:
        """Close the connection."""
        client = await self._get_client()
        await client.s3.close()

    def get_fs_and_path(self, path: str) -> Tuple[fsspec.AbstractFileSystem, str]:
        """S3 implementation for returning (fs, path) suitable for xarray.

        Parameters
        ^^^^^^^^^^
        path:
            Path to the object store / file name

        Returns
        ^^^^^^^
        fsspec.AbstractFileSystem, str:
            The AbstractFileSystem class and the corresponding path to the
            data store.
        """
        return fsspec.filesystem("s3", **self.storage_options), path

    async def _get_client(self) -> S3FileSystem:
        if self._client is None:
            logger.debug(
                "Creating S3 Filesystem with storage_options: %s",
                self.storage_options,
            )
            loop = asyncio.get_running_loop()
            self._client = S3FileSystem(
                asynchronous=True, loop=loop, **self.storage_options
            )
            self._client._loop = loop

            await self._client.set_session()
        return self._client

    async def is_file(self, path: Union[str, Path, pathlib.Path]) -> bool:
        """Check if a given path is a file object on the storage system."""
        client = await self._get_client()
        return cast(bool, await client._isfile(str(path)))

    async def is_dir(self, path: str | Path | pathlib.Path) -> bool:
        """Check if a given path is a directory object on the storage system."""
        client = await self._get_client()
        return cast(bool, await client._isdir(str(path)))

    async def iterdir(
        self, path: Union[str, Path, pathlib.Path]
    ) -> AsyncIterator[str]:
        """Retrieve sub directories of directory."""
        client = await self._get_client()
        path = str(path)
        if await self.is_file(path):
            yield path
        else:
            for _content in await client._lsdir(path):
                size: int = _content.get("size") or 0
                if _content.get("type", "") == "directory" or size > 0:
                    yield _content.get("name", "")

    async def rglob(
        self, path: str | Path | pathlib.Path, glob_pattern: str = "*"
    ) -> AsyncIterator[MetadataType]:
        """Search recursively for files matching a ``glob_pattern``.

        Parameters
        ^^^^^^^^^^
        path: str
            A resource composed by:
                - bucket, 'bucketname'
                - prefix, 'prefix/to/a/path'
            E.g.: '/bucketname/prefix/to/objects'
            Will be translated into a request to
            `self.url`+`/bucketname?prefix="prefix/to/objects`
        glob_pattern: str
            A string reprenseting several glob patterns, separated by '|'
            E.g.: '*.zarr|*.nc|*.hdf5'
        """
        client = await self._get_client()
        if await self.is_file(path):
            yield MetadataType(path=str(path), metadata={})
        else:
            for suffix in self.suffixes:
                for content in await client._glob(f"{path}/**/*{suffix}"):
                    yield MetadataType(path=f"/{content}", metadata={})

    def path(self, path: Union[str, Path, pathlib.Path]) -> str:
        """Get the full path (including any schemas/netlocs).

        Parameters
        ^^^^^^^^^^
        path: str, asyncio.Path, pathlib.Path
            Path of the object store

        Returns
        ^^^^^^^
        str:
            URI of the object store
        """
        return cast(
            str, fsspec.filesystem("s3", **self.storage_options).url(str(path))
        )

    def uri(self, path: Union[str, Path, pathlib.Path]) -> str:
        """Get the uri of the object store.

        Parameters
        ^^^^^^^^^^
        path: str, asyncio.Path, pathlib.Path
            Path of the object store

        Returns
        ^^^^^^^
        str:
            URI of the object store
        """
        return self.path(path)
