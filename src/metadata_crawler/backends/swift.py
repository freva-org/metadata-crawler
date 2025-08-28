"""Interact with the OpenStack swift cloud."""

from __future__ import annotations

import asyncio
import pathlib
from fnmatch import fnmatch
from typing import AsyncIterator, Dict, List, Optional, Tuple, Union, cast
from urllib.parse import SplitResult, urljoin, urlsplit, urlunparse

import aiohttp
import fsspec
from anyio import Path

from ..api.storage_backend import MetadataType, PathTemplate


def _basename(key: str) -> str:
    return pathlib.PosixPath(key[:-1] if key.endswith("/") else key).name


class SwiftPath(PathTemplate):
    """Class to interact with the OpenStack swift cloud storage system."""

    _fs_type = "swift"

    def __post_init__(self) -> None:
        self.storage_options = self.storage_options or {}
        self.os_password = self.storage_options.get("os_password", self._pw)
        self.os_user_id = self.storage_options.get("os_user_id", self._user)
        self.os_project_id = self.storage_options.get("os_project_id")
        self.os_auth_token = self.storage_options.get("os_auth_token") or None
        self._os_storage_url = self.storage_options.get(
            "os_storage_url", ""
        ).rstrip("/")
        self.os_auth_url = self.storage_options.get(
            "os_auth_url", self._guess_tempauth_url(self._os_storage_url)
        )
        self._container = self.storage_options.get(
            "container", self._os_storage_url.split("/")[-1]
        ).rstrip("/")
        self._os_storage_url = self._os_storage_url.removesuffix(self._container)
        self._url_split: Optional[SplitResult] = None

    @staticmethod
    def _guess_tempauth_url(storage_url: str) -> str:
        """Construct the swift url.

        Heuristic: For TempAuth, switch '/v1/...' to '/auth/v1.0' on same host:port.
        Returns None if storage_url doesn't look like a Swift v1 endpoint.
        """
        p = urlsplit(storage_url)
        # Typical Swift proxy paths: '/v1/...' or '/swift/v1/...'
        if not (p.path.startswith("/v1/") or p.path.startswith("/swift/v1/")):
            return ""
        # Use same scheme+netloc, set path to /auth/v1.0
        return urlunparse((p.scheme, p.netloc, "/auth/v1.0", "", "", ""))

    @property
    def storage_path(self) -> str:
        """Path part of the storage url."""
        split = self.url_split
        return "/" + split.path.lstrip("/").rstrip("/")

    @property
    def url_split(self) -> SplitResult:
        """Retrieve the split parts of the storage url."""
        if self._url_split is not None:
            return self._url_split
        if not self._os_storage_url:
            raise RuntimeError("os_storage_url must be set")
        storage_url = self._os_storage_url.removesuffix(self._container)
        self._url_split = urlsplit(urljoin(storage_url, self._container))
        return self._url_split

    @property
    def _anon(self) -> bool:
        """Decide if we can logon at all."""
        return False if self.os_password or self.headers else True

    async def logon(self) -> None:
        """Logon to the swfit system if necessary."""
        headers = {
            "X-Auth-User": f"{self.os_project_id}:{self.os_user_id}",
            "X-Auth-Key": self.os_password,
        }
        async with aiohttp.ClientSession() as session:
            async with session.get(self.os_auth_url, headers=headers) as res:
                if res.status != 200:
                    raise ValueError(f"Logon to {self.os_auth_url} failed")
                self.os_auth_token = res.headers["X-Auth-Token"]

    def _is_zarr_like_match(self, key: str, glob_pattern: str) -> bool:
        key_l = key.lower()
        base = _basename(key)
        if key_l.endswith(".zarr") or key_l.endswith(".zarr/"):
            if ".zarr" in self.suffixes and fnmatch(base, glob_pattern):
                return True
        return False

    async def _url_fragments(self, url: str) -> Tuple[str, str]:
        url_split = urlsplit(url)
        url_path = (
            ("/" + url_split.path.lstrip("/"))
            .removeprefix(self.storage_path)
            .rstrip("/")
            .lstrip("/")
        )

        parsed_url = SplitResult(
            url_split.scheme or self.url_split.scheme,
            url_split.netloc or self.url_split.netloc,
            f"{self.storage_path}/{url_path}",
            url_split.query,
            url_split.fragment,
        )
        _path = pathlib.PosixPath(parsed_url.path).parts[1:]
        url_prefix = "/".join(_path[:3])
        prefix = "/".join(_path[3:])
        if prefix:
            prefix += "/"
        url_head = f"{parsed_url.scheme}://{parsed_url.netloc}/{url_prefix}"
        return url_head, prefix

    async def _read_json(
        self, path: str, delimiter: Optional[str] = "/"
    ) -> List[Dict[str, str]]:
        url, prefix = await self._url_fragments(path)
        suffix = f"?format=json&prefix={prefix}"
        if delimiter:
            suffix += f"&delimiter={delimiter}"
        else:
            suffix = suffix.rstrip("/")
        url = f"{url}{suffix}"
        errors = {
            403: PermissionError(f"Permission denied for {path}"),
            404: FileNotFoundError(f"No such file or directory {path}"),
        }
        async with aiohttp.ClientSession() as session:
            for _ in range(2):
                async with session.get(url, headers=self.headers) as res:
                    if res.status < 300:
                        return cast(list[dict[str, str]], await res.json())
                    if res.status == 401:
                        await self.logon()
                        continue
        raise errors.get(res.status, RuntimeError(f"Failed to query {path}"))

    def _get_dir_from_path(self, data: dict[str, str]) -> str | None:
        if (
            data.get("subdir")
            or data.get("content_type", "") == "application/directory"
        ):
            return data.get("subdir") or data.get("name")
        return None

    @property
    def headers(self) -> dict[str, str]:
        """Define the headers used to interact with swift."""
        if self.os_auth_token is None:
            return {}
        return {"X-Auth-Token": self.os_auth_token}

    async def is_file(self, path: str | Path | pathlib.Path) -> bool:
        """Check if a given path is a file object on the storage system."""
        try:
            data = (await self._read_json(str(path)))[0]
        except (FileNotFoundError, IndexError):
            return False
        return self._get_dir_from_path(data) is None

    async def is_dir(self, path: str | Path | pathlib.Path) -> bool:
        """Check if a given path is a directory object on the storage system."""
        try:
            data = (await self._read_json(str(path)))[0]
        except (FileNotFoundError, IndexError):
            return False
        return self._get_dir_from_path(data) is not None

    async def iterdir(
        self, path: Union[str, Path, pathlib.Path]
    ) -> AsyncIterator[str]:
        """Get all sub directories of a directory."""
        try:
            for data in await self._read_json(str(path)):
                new_path = self._get_dir_from_path(data)
                if new_path:
                    out = (
                        str(path).lstrip("/")
                        + "/"
                        + pathlib.PosixPath(new_path).name
                    )
                    yield out
        except (FileNotFoundError, PermissionError):
            pass

    async def rglob(
        self,
        path: Union[str, Path, pathlib.Path],
        glob_pattern: str = "*",
    ) -> AsyncIterator[MetadataType]:
        """Search recursively for files matching a glob_pattern."""
        delimiter: Optional[str] = None
        if await self.is_dir(path):
            delimiter = "/"
        for data in await self._read_json(str(path), delimiter=delimiter):
            # swift doesn't natively support pagination, so we need to do it
            # ourselves.
            name = data.get("name")
            dir_name = self._get_dir_from_path(data)
            if dir_name:
                # if it's an actual object named foo.zarr, treat as zarr store
                if self._is_zarr_like_match(dir_name, glob_pattern):
                    yield MetadataType(path=dir_name.rstrip("/"), metadata={})
                else:
                    async for md in self.rglob(dir_name, glob_pattern):
                        yield md
            elif name:
                if pathlib.PosixPath(name).suffix in self.suffixes and fnmatch(
                    name, glob_pattern
                ):
                    yield MetadataType(path=name, metadata={})

    def get_fs_and_path(self, uri: str) -> Tuple[fsspec.AbstractFileSystem, str]:
        """Return (fs, path) suitable for xarray.

        Parameters
        ----------
        uri:
            Path to the object store / file name


        Returns
        -------
        fsspec.AbstractFileSystem, str:
            The AbstractFileSystem class and the corresponding path to the
            data store.
        """
        url_split = urlsplit(uri)
        url_path = (
            ("/" + url_split.path.lstrip("/"))
            .removeprefix(self.storage_path)
            .rstrip("/")
            .lstrip("/")
        )
        url = SplitResult(
            url_split.scheme or self.url_split.scheme,
            url_split.netloc or self.url_split.netloc,
            f"{self.storage_path}/{url_path}",
            url_split.query,
            url_split.fragment,
        ).geturl()
        if not self._anon:
            asyncio.run(self.logon())
        return (
            fsspec.filesystem(
                "http",
                headers=self.headers,
                block_size=2**20,
            ),
            url,
        )

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
        url_split = urlsplit(str(path))
        if not url_split.netloc:
            path = f"{self.url_split.path}/{url_split.path}"
        else:
            path = url_split.path

        res = SplitResult(
            url_split.scheme or self.url_split.scheme,
            url_split.netloc or self.url_split.netloc,
            path,
            url_split.query,
            url_split.fragment,
        ).geturl()
        return res

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
        return self.path(path)
