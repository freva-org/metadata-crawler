"""Interact with an S3 Object Store."""

import asyncio
import pathlib
from fnmatch import fnmatch
from typing import AsyncIterator, Dict, Optional, Tuple, Union, cast
from urllib.parse import quote, unquote

import fsspec
from s3fs import S3FileSystem

from ..api.storage_backend import MetadataType, PathTemplate
from ..logger import logger


class S3Path(PathTemplate):
    """Class to interact with an S3 object store."""

    _fs_type = "s3"

    def __post_init__(self) -> None:
        self._client: Optional[S3FileSystem] = None
        self.storage_options = self.storage_options or {"anon": True}
        client_kwargs: Dict[str, str] = self.storage_options.get("client_kwargs", {})
        endpoint = (
            client_kwargs.get("endpoint_url", self.storage_options.get("endpoint_url"))
            or ""
        )
        host = endpoint.split("://", 1)[-1]
        self._endpoint: str = "" if "amazonaws" in host else endpoint.rstrip("/")
        self._netloc = self._endpoint.rpartition("://")[-1]

    @staticmethod
    def _norm(path: Union[str, pathlib.Path]) -> str:
        """Normalise a path before handing it to a private s3fs method.

        ``S3FileSystem.split_path`` deliberately *restores* a trailing slash
        onto the key (``key += trail``), so ``_lsdir("bucket/pre/")`` lists
        with the prefix ``pre//`` and returns nothing.  The public
        ``ls``/``find`` wrappers call ``_strip_protocol`` first, which is why
        they behave.  Every call into a ``_``-method has to do the same.
        """
        return cast(str, S3FileSystem._strip_protocol(str(path))).rstrip("/")

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
                asynchronous=True,
                loop=loop,
                skip_instance_cache=True,
                use_listings_cache=False,
                **self.storage_options,
            )
            self._client._loop = loop

            await self._client.set_session()
        return self._client

    async def _access_uri(self, path: str) -> str:
        if not self._endpoint:
            return self.uri(path)
        stripped = self.path(path).lstrip("/")
        bucket, _, obj = stripped.partition("/")
        key = quote(obj, safe="/")
        return f"{self._endpoint}/{bucket}/{key}"

    async def is_file(self, path: Union[str, pathlib.Path]) -> bool:
        """Check if a given path is a file object on the storage system."""
        client = await self._get_client()
        return cast(bool, await client._isfile(self._norm(path)))

    async def is_dir(self, path: Union[str, pathlib.Path]) -> bool:
        """Check if a given path is a directory object on the storage system."""
        client = await self._get_client()
        return cast(bool, await client._isdir(self._norm(path)))

    async def iterdir(self, path: Union[str, pathlib.Path]) -> AsyncIterator[str]:
        """Retrieve sub directories of directory."""
        client = await self._get_client()
        path = self._norm(path)
        if await self.is_file(path):
            yield self.uri(path)
        else:
            for _content in await client._lsdir(path):
                size: int = _content.get("size") or 0
                if _content.get("type", "") == "directory" or size > 0:
                    yield (_content.get("name") or "").rstrip("/")

    async def rglob(
        self, path: Union[str, pathlib.Path], glob_pattern: str = "*"
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
        root = self._norm(path)
        suffixes = tuple(s.lower() for s in self.suffixes)
        patterns = [p.strip() for p in glob_pattern.split("|") if p.strip()] or ["*"]

        def _matches(name: str) -> bool:
            rel = name[len(root) :].lstrip("/")
            base = rel.rpartition("/")[-1]
            return any(fnmatch(rel, p) or fnmatch(base, p) for p in patterns)

        # A directory-like store (``*.zarr``) *is* the dataset - never descend
        # into it.  This mirrors the posix/rust backend, which short-circuits
        # on ``is_file || is_zarr`` before starting to walk.
        if root.lower().endswith(suffixes) or await self.is_file(root):
            yield MetadataType(path=await self._access_uri(f"/{root}"), metadata={})
            return

        stack = [root]
        while stack:
            current = stack.pop()
            for entry in await client._lsdir(current):
                name = (entry.get("name") or "").rstrip("/")
                if not name or name == current:
                    continue
                if name.lower().endswith(suffixes):
                    if _matches(name):
                        yield MetadataType(
                            path=await self._access_uri(f"/{name}"), metadata={}
                        )
                    continue  # prune: never walk inside a data store
                if entry.get("type", "") == "directory":
                    stack.append(name)

    def path(self, path: Union[str, pathlib.Path]) -> str:
        """Get the full path (without any schemas/netlocs).

        Parameters
        ^^^^^^^^^^
        path: str, pathlib.Path
            Path of the object store

        Returns
        ^^^^^^^
        str:
            Path of the object store
        """
        scheme, p = fsspec.core.split_protocol(str(path))
        path = p.lstrip("/")
        if scheme and scheme.startswith("http"):
            p = unquote(p).removeprefix(self._netloc)
        return f"/{p.lstrip('/').rstrip('/')}"

    def uri(self, path: Union[str, pathlib.Path]) -> str:
        """Get the uri of the object store.

        Parameters
        ^^^^^^^^^^
        path: str, pathlib.Path
            Path of the object store

        Returns
        ^^^^^^^
        str:
            URI of the object store
        """
        path = self.path(path).lstrip("/").rstrip("/")
        return f"s3://{path}"
