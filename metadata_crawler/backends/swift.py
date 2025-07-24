"""Interact with the OpenStack swift cloud."""

from __future__ import annotations

import asyncio
import pathlib
from datetime import datetime
from fnmatch import fnmatch
from typing import AsyncIterator, cast
from urllib.parse import urljoin, urlparse

import aiohttp
from anyio import Path
from rich.prompt import Prompt

from ..utils import PrintLock
from .base import BasePath


class SwiftPath(BasePath):
    """Class to interact with the OpenStack swift cloud storage system.

    Parameters
    ----------
    username: str, default: None
        User name that is used to logon to the system.
        This is only needed if no valid token was given.
    password: str, default: None
        Password that is used to logon and generate an access token.
        This is only needed if no valid token was given.
    url: str, default: https://archive.dkrz.de/api/v2
        Url of the StrongLink rest API service
    container: str, default: None
        The swift storage container name that is used to store the data.
    """

    password_prompt: str = "[b]Password to logon to the swift system[/b]"
    # TODO: Make the url scalable ...
    _url: str = "https://swift.dkrz.de"
    fs_type = "swift"

    def __init__(
        self,
        username: str | None = None,
        password: str | None = None,
        url: str | None = None,
        account: str | None = None,
    ):
        super().__init__(
            username=username, password=password, account=account, url=url
        )

    async def logon(self) -> None:
        """Logon to the swfit system if necessary."""
        with PrintLock:
            self.password = self.password or Prompt.ask(
                self.password_prompt, password=True
            )
        headers = {
            "X-Auth-User": f"{self.account}:{self.username}",
            "X-Auth-Key": self.password,
        }
        async with aiohttp.ClientSession() as session:
            async with session.get(
                urljoin(self._url, "auth/v1"), headers=headers
            ) as res:
                if res.status != 200:
                    raise ValueError(f"Logon to {self._url} failed")
                self.token = res.headers["X-Auth-Token"]

    @staticmethod
    async def _url_fragments(url: str) -> tuple[str, str]:
        parsed_url = urlparse(url)
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
        raise ValueError(f"Login to {self._url} failed")  # pragma: no cover

    async def _get_dir_from_path(self, data: dict[str, str]) -> str | None:
        if "subdir" in data:
            return data["subdir"]
        if data.get("content_type", "") == "application/directory":
            return data.get("name")
        return None

    @property
    def headers(self) -> dict[str, str]:
        """Define the headers used to interact with swift."""
        if self.token is None:
            return {}
        return {"X-Auth-Token": self.token}

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
        self, path: str | Path | pathlib.Path
    ) -> AsyncIterator[str]:
        try:
            for data in await self._read_json(str(path)):
                new_path = await self._get_dir_from_path(data)
                if new_path:
                    yield str(path) + "/" + pathlib.PosixPath(new_path).name
        except (FileNotFoundError, PermissionError):
            pass

    async def mtime(self, path: str | Path | pathlib.Path) -> float:
        """Get the modification time of a path."""
        try:
            data = (await self._read_json(str(path)))[0]
            timestamp = datetime.fromisoformat(data["last_modified"])
        except (KeyError, FileNotFoundError, IndexError) as error:
            raise FileNotFoundError(
                f"No such file or directory {path}"
            ) from error
        return timestamp.timestamp()

    async def rglob(
        self, path: str | Path | pathlib.Path, glob_pattern: str = "*"
    ) -> AsyncIterator[str]:
        """Search recursively for files matching a glo_pattern."""
        for data in await self._read_json(str(path), delimiter=None):
            name = data.get("name")
            if name:
                new_path = pathlib.PosixPath(name)
                if new_path.suffix == ".zarr":
                    if fnmatch(new_path.name, glob_pattern):
                        url, _ = await self._url_fragments(str(path))
                        yield url + "/" + name

    async def read_metadata_from_path(
        self, attr: str, path: str | Path | pathlib.Path
    ) -> str:
        """Access the meta data by key-value pair

        Parameters
        ----------
        attr: str
            The name of the meta data attribute that should be reade.
        path: str
            Path of the object store

        Returns
        -------
        str: value of the meta data for the key

        Raises
        ------
        KeyError: if attribute is not present in metadata.
        """
        return await asyncio.to_thread(self._read_attr, attr, str(path))
