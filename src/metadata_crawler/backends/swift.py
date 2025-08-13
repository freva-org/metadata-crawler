"""Interact with the OpenStack swift cloud."""

from __future__ import annotations

import pathlib
from fnmatch import fnmatch
from getpass import getuser
from typing import Any, AsyncIterator, Tuple, Union, cast
from urllib.parse import SplitResult, urljoin, urlsplit

import aiohttp
from anyio import Path
from rich.prompt import Prompt

from ..utils import PrintLock
from .base import BasePath, Metadata


class SwiftPath(BasePath):
    """Class to interact with the OpenStack swift cloud storage system."""

    _fs_type = "swift"

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)
        self.os_password = self.storage_options.get("os_password") or self._pw
        self.os_user_id = (
            self.storage_options.get("os_user_id") or self._user or getuser()
        )
        self.os_auth_token = self.storage_options.get("os_auth_token")
        self.os_auth_url = self.storage_options.get(
            "os_auth_url", "https://swift.dkrz.de/auth/v1"
        )
        self.os_project_id = self.storage_options.get("os_project_id", "")
        self.os_storage_url = self.storage_options.get(
            "os_storage_url", ""
        ).rstrip("/")
        self.container = self.storage_options.get(
            "container", self.os_storage_url.split("/")[-1]
        ).rstrip("/")
        self.os_storage_url = self.os_storage_url.removesuffix(self.container)
        self.url_split = urlsplit(urljoin(self.os_storage_url, self.container))

    async def logon(self) -> None:
        """Logon to the swfit system if necessary."""
        with PrintLock:
            self.os_password = self.os_password or Prompt.ask(
                "Password for swift storage.", password=True
            )
        headers = {
            "X-Auth-User": f"{self.os_project_id}:{self.os_user_id}",
            "X-Auth-Key": self.os_password,
        }
        async with aiohttp.ClientSession() as session:
            async with session.get(self.os_auth_url, headers=headers) as res:
                if res.status != 200:
                    raise ValueError(f"Logon to {self.os_auth_url} failed")
                self.os_auth_token = res.headers["X-Auth-Token"]

    async def _url_fragments(self, url: str) -> Tuple[str, str]:
        url_split = urlsplit(url)
        parsed_url = SplitResult(
            url_split.scheme or self.url_split.scheme,
            url_split.netloc or self.url_split.netloc,
            f"{self.url_split.path}/{url_split.path.rstrip('/').lstrip('/')}",
            url_split.query,
            url_split.fragment,
        )
        url_path = pathlib.PosixPath(parsed_url.path).parts[1:]
        url_prefix = "/".join(url_path[:3])
        prefix = "/".join(url_path[3:])
        if prefix:
            prefix += "/"
        url_head = f"{parsed_url.scheme}://{parsed_url.netloc}/{url_prefix}"
        return url_head, prefix

    async def _read_json(
        self, path: str, delimiter: str | None = "/"
    ) -> list[dict[str, str]]:
        url, prefix = await self._url_fragments(path)
        # prefix = url.removeprefix(self.os_storage_url)
        suffix = f"?format=json&prefix={prefix}"
        if delimiter:
            suffix += f"&delimiter={delimiter}"
        url = f"{url}{suffix}"
        async with aiohttp.ClientSession() as session:
            for _ in range(2):
                async with session.get(url, headers=self.headers) as res:
                    if res.status == 401:
                        await self.logon()
                        continue
                    if res.status == 403:
                        raise PermissionError(f"Permission denied for {path}")
                    if res.status == 404:
                        raise FileNotFoundError(
                            f"No such file or directory {path}"
                        )
                    return cast(list[dict[str, str]], await res.json())
        # this never happens, bencause of the raise error in logon
        raise ValueError(
            f"Login to {self.os_storage_url} failed"
        )  # pragma: no cover

    async def _get_dir_from_path(self, data: dict[str, str]) -> str | None:
        if "subdir" in data:
            return data["subdir"]
        if data.get("content_type", "") == "application/directory":
            return data.get("name")
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
        return await self._get_dir_from_path(data) is None

    async def is_dir(self, path: str | Path | pathlib.Path) -> bool:
        """Check if a given path is a directory object on the storage system."""
        try:
            data = (await self._read_json(str(path)))[0]
        except (FileNotFoundError, IndexError):
            return False
        return await self._get_dir_from_path(data) is not None

    async def iterdir(
        self, path: Union[str, Path, pathlib.Path]
    ) -> AsyncIterator[str]:
        try:
            for data in await self._read_json(str(path)):
                new_path = await self._get_dir_from_path(data)
                if new_path:
                    yield "/" + str(path).lstrip("/") + "/" + pathlib.PosixPath(
                        new_path
                    ).name
        except (FileNotFoundError, PermissionError):
            pass

    async def rglob(
        self, path: str | Path | pathlib.Path, glob_pattern: str = "*"
    ) -> AsyncIterator[Metadata]:
        """Search recursively for files matching a glo_pattern."""
        for data in await self._read_json(str(path), delimiter=None):
            name = data.get("name")
            if name:
                new_path = pathlib.PosixPath(name)
                if (
                    fnmatch(new_path.name, glob_pattern)
                    and new_path.suffix in self.suffixes
                ):
                    url, _ = await self._url_fragments(str(path))
                    result = urljoin(url, name).removeprefix(self.os_storage_url)
                    yield Metadata(path="/" + result.lstrip("/"))

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
        url = urlsplit(urljoin(self.os_storage_url, f"{self.container}/{path}"))
        return SplitResult(
            "swift", url.netloc, url.path, url.query, url.fragment
        ).geturl()

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
