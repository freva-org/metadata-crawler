"""Interact with the StrongLink hsm system."""

from __future__ import annotations

import os
import pathlib
from fnmatch import fnmatch
from typing import Any, AsyncIterator, Literal, NamedTuple, cast

import aiohttp
from anyio import Path
from rich.prompt import Prompt

from ..utils import Console, PrintLock
from .base import BasePath

SchemaType = NamedTuple(
    "SchemaType", [("id", int), ("sub_id", int), ("type", int)]
)


class HsmPath(BasePath):
    """Class to interact with the HSM StrongLink Tape archive system.

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
    """

    # TODO: Make the url scalable ...
    password_prompt: str = "[b]Password to logon to the hsm system[/b]"
    _url: str = "https://archive.dkrz.de/api/v2"
    fs_type = "hsm"
    _password: list[str] = []

    def __init__(
        self,
        username: str | None = None,
        password: str | None = None,
        url: str | None = None,
        account: str | None = None,
    ) -> None:
        super().__init__(
            username=username, password=password, account=account, url=url
        )
        self.url = self.url or self._url
        if self.password:
            self._password.append(self.password)
        self._udm_schema: dict[str, dict[str, SchemaType]] | None = None

    async def _get_id_from_path(
        self,
        root_path: str | Path | pathlib.Path,
        source_type: Literal["namespaces", "resources"] = "namespaces",
    ) -> int:
        """Get the id of a path in the hsm system."""
        data = await self._read_json(f"/{source_type}/exists?path={root_path}")
        return cast(int, data["id"])

    async def _get_udm_schemas(self) -> None:
        await self.logon()
        self._udm_schema = {}
        schema_data = cast(
            list[dict[str, Any]], await self._read_json("/udm_schemas")
        )
        for schema in schema_data:
            name = schema["attributes"]["name"]
            sch_id = schema["id"]
            self._udm_schema[name] = {}
            for sub_id, sub_sch in schema["attributes"]["schema"].items():
                self._udm_schema[name][sub_sch["name"]] = SchemaType(
                    sch_id,
                    int(sub_id),
                    sub_sch["type"],
                )

    async def _read_namespace_data(
        self, root_path: str | Path | pathlib.Path
    ) -> dict[str, list[dict[str, str]]]:
        search_id = await self._get_id_from_path(root_path)
        data = await self._read_json(f"/namespaces/{search_id}/children")
        attrs = cast(dict[str, list[dict[str, str]]], data["attributes"])
        return {
            "namespaces": attrs.get("namespaces", []),
            "resources": attrs.get("resources", []),
        }

    async def _read_json(self, path: str) -> dict[str, Any]:
        """Open a connection to the HSM RestAPI and read the json content."""
        async with aiohttp.ClientSession() as session:
            for _ in range(2):
                async with session.get(
                    self.url + path,
                    headers=await self._session_header,
                    ssl=False,
                ) as res:
                    if res.status == 403:
                        await self.logon()
                        continue
                    if res.status == 404:
                        raise FileNotFoundError(
                            f"No such file or directory {path}"
                        )
                    out = await res.json()
                    return cast(dict[str, Any], out["data"])
        # TOGET: This raise doesn't happen, due to logon retry
        raise ValueError(f"Login to {self.url} failed")  # pragma: no cover

    async def _is_name_space(
        self,
        path: str | Path | pathlib.Path,
        source_type: Literal["resources", "namespaces"],
    ) -> bool:
        search_id = await self._get_id_from_path(path, source_type)
        data = await self._read_json(f"/{source_type}/{search_id}")
        try:
            return cast(dict[str, bool], data["attributes"])["is_ns"]
        # TOGET: it seems we don't get this exception in HSM object at all due to the last raises, but we mock it in the tests just in case
        except KeyError as error:
            raise FileNotFoundError(f"{source_type} does not exist") from error

    async def logon(self) -> None:
        """Logon to the hsm system if necessary."""
        if not self._password:
            with PrintLock:
                Console.print("", end="")
                self._password.append(
                    Prompt.ask(self.password_prompt, password=True)
                )
        data = {
            "data": {
                "attributes": {
                    "domain": "ldap",
                    "name": self.username,
                    "password": self._password[0],
                },
                "type": "authentication",
            }
        }
        headers = {"Content-type": "application/json"}
        url = self.url + "/authentication"
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url,
                json=data,
                headers=headers,
                ssl=False,
            ) as res:
                if res.status != 200:
                    raise ValueError(f"Logon to {self.url} failed")
                self.token = cast(
                    str,
                    (await res.json())
                    .get("data", {})
                    .get("attributes", {})
                    .get("session_key", ""),
                )

    @property
    async def _session_header(self) -> dict[str, str]:
        if self.token is None:
            await self.logon()
        return {
            "X-SDS-SessionKey": cast(str, self.token),
            "Content-type": "application/json",
        }

    async def is_dir(self, path: str | Path | pathlib.Path) -> bool:
        """Check if a given namespace is a directory."""
        try:
            return await self._is_name_space(path, "namespaces")
        except FileNotFoundError:
            return False

    async def is_file(self, path: str | Path | pathlib.Path) -> bool:
        """Check if a given path is a file object on the storage system."""
        try:
            return await self._is_name_space(path, "resources") is False
        except FileNotFoundError:
            return False

    async def iterdir(
        self, path: str | Path | pathlib.Path
    ) -> AsyncIterator[str]:
        try:
            data = await self._read_namespace_data(path)
            for namespace in data.get("namespaces", []):
                yield os.path.join(path, namespace["name"])
        except Exception:
            pass

    async def mtime(self, path: str | Path | pathlib.Path) -> float:
        """Get the modification time of a path."""
        search_id = await self._get_id_from_path(path, "resources")
        data = (await self._read_json(f"/resources/{search_id}"))["attributes"]
        try:
            return cast(float, data["modified"]) / 1000.0
        except KeyError as error:
            raise FileNotFoundError(
                f"No such file or directory {path}"
            ) from error

    async def rglob(
        self, path: str | Path | pathlib.Path, glob_pattern: str = "*"
    ) -> AsyncIterator[str]:
        """Search recursively for files matching a glo_pattern."""
        data = await self._read_namespace_data(path)
        for resource in data.get("resources", []):
            if fnmatch(resource["name"], glob_pattern):
                yield os.path.join(str(path), resource["name"])
        for _dir in data.get("namespaces", []):
            async for search_path in self.rglob(
                os.path.join(str(path), _dir["name"]), glob_pattern
            ):
                yield search_path

    async def _get_udm_schema_key(self, attr: str) -> tuple[str, str]:
        for key, values in (await self.udm_schemas).items():
            for sub_key in values.keys():
                if attr.lower() == sub_key.lower():
                    return key, sub_key
        raise KeyError(f"{attr} not in meta data schema")

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
        search_id = await self._get_id_from_path(path, "resources")
        key, sub_key = await self._get_udm_schema_key(attr)
        schema_attr = (await self.udm_schemas)[key][sub_key]
        data = await self._read_json(f"/resources/{search_id}/udms")
        for schema in data["relationships"]:
            if cast(int, schema["udm_schema_id"]) == schema_attr.id:
                for udm in schema["udms"]:
                    if cast(int, udm["field_id"]) == schema_attr.sub_id:
                        return cast(str, udm["value"])
        # TOGET: This raise doesn't happen, due to self._get_udm_schema_key already raising an error
        raise KeyError(f"Attribute not found: {attr}")  # pragma: no cover

    @property
    async def udm_schemas(self) -> dict[str, dict[str, SchemaType]]:
        """Get all metadata schema definitions."""
        if self._udm_schema is None:
            await self._get_udm_schemas()
        return cast(dict[str, dict[str, SchemaType]], self._udm_schema)

    async def uri(self, path: str | Path | pathlib.Path) -> str:
        return f"slk://{path}"
