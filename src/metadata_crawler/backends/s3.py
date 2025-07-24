"""Interact with an S3 Object Store."""

from __future__ import annotations

import asyncio
import pathlib
from typing import Any, AsyncIterator, Optional, Tuple

import fsspec
from anyio import Path
from s3fs import S3FileSystem

from ..logger import logger
from ..utils import PrintLock  # noqa
from .base import BasePath, Metadata


class S3Path(BasePath):
    """Class to interact with an S3 object store.

    Parameters
    ----------
    username: str, default: None
        Key id that is used to access the resource/bucket.
        This is only needed if the bucket is not public
    password: str, default: None
        Respective secret, only needed if resource is private
    url: str, default: https://s3.eu-dkrz-1.dkrz.cloud
        Url of the S3 endpoint
    account: str, default: None
        The AWS profile to use for fetching these objects.
    """

    fs_type = "s3"

    def __init__(self, **storage_options: Any):
        super().__init__(**storage_options)
        self._client: Optional[S3FileSystem] = None

    async def close(self) -> None:
        client = await self.get_client()
        await client.s3.close()

    def get_fs_and_path(self, path: str) -> Tuple[fsspec.AbstractFileSystem, str]:

        return fsspec.filesystem("s3", **self.storage_options), path

    async def get_client(self) -> S3FileSystem:
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

    async def is_file(self, path: str | Path | pathlib.Path) -> bool:
        """Check if a given path is a file object on the storage system."""
        client = await self.get_client()
        return await client._isfile(str(path))

    async def is_dir(self, path: str | Path | pathlib.Path) -> bool:
        """Check if a given path is a directory object on the storage system."""
        client = await self.get_client()
        return await client._isdir(str(path))

    async def iterdir(
        self, path: str | Path | pathlib.Path
    ) -> AsyncIterator[str]:
        client = await self.get_client()
        for _content in await client._lsdir(str(path)):
            if _content.get("type", "") == "directory":
                yield f'{_content.get("name", "")}'

    async def rglob(
        self, path: str | Path | pathlib.Path, glob_pattern: str = "*"
    ) -> AsyncIterator[str]:
        """Search recursively for files matching the extensions given by 'glob_pattern'.

        Parameters
        ----------
        path: str
            A resource composed by:
                - bucket, 'bucketname'
                - prefix, 'prefix/to/a/path'
            E.g.: '/bucketname/prefix/to/objects'
            Will be translated into a request to `self.url`+`/bucketname?prefix="prefix/to/objects`
        glob_pattern: str
            A string reprenseting several glob patterns, separated by '|'
            E.g.: '*.zarr|*.nc|*.hdf5'
        """
        client = await self.get_client()
        for content in await client._glob(f"{path}/**/{glob_pattern}"):
            yield Metadata(path=f"/{content}")
