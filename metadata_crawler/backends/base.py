"""API for adding new storage backends via :py:class:`BasePath`
==============================================================
"""

from __future__ import annotations

import abc
import os
import pathlib
import threading
from getpass import getuser
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple

import fsspec
import xarray as xr
from anyio import Path
from pydantic import BaseModel, Field

from .lookup_tables import cmor_lookup


def fs_and_path(
    url: str, **storage_options
) -> Tuple[fsspec.AbstractFileSystem, str]:
    """
    Return (fs, path) for any URL that fsspec understands (file, s3, swift, http, ...).
    """
    protocol, path = fsspec.core.split_protocol(url)
    protocol = protocol or "file"
    fs = fsspec.filesystem(protocol, **storage_options)
    return fs, path


class Metadata(BaseModel):
    """Meta data that is attached to each discovered path."""

    path: str
    metadata: Dict[str, Any] = Field(default_factory=dict)


class BasePath(metaclass=abc.ABCMeta):
    """Base class for interacting with different storage systems.

    This class defines fundamental methods that should be implemented
    to retrieve information across different storage systems.

    Parameters
    ----------
    url: str, default: None
        The url of the storage system. If None given (default) no url is needed.
    username: str, default: None
        The user name used to interact with the storage system. Only relevant
        if a user name is needed.
    account: str, default: None
        The account name used to interact with the storage system. Only
        relevant if an account name is needed.
    password: str, default: None
        The password used to sign in to the storage system. Only
        relevant if a sign in is supported by the storage system.

    Attributes
    ----------
    """

    _lock = threading.RLock()

    def __init__(
        self, suffixes: Optional[List[str]] = None, **storage_options: Any
    ) -> None:
        self._user: str = os.environ.get("DRS_STORAGE_USER") or getuser()
        self._pw: str = os.environ.get("DRS_STORAGE_PASSWD") or ""
        self.suffixes = suffixes or [".nc", ".girb", ".zarr", ".tar", ".hdf5"]
        self.storage_options = storage_options

    async def __aenter__(self) -> "BasePath":
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    async def close(self) -> None:
        """Close any open sessions."""

    def get_fs_and_path(self, path: str) -> Tuple[fsspec.AbstractFileSystem, str]:
        """Return (fs, path) suitable for xarray."""
        return fs_and_path(path)

    def open_dataset(self, file_name: str, **read_kws: Any) -> xr.Dataset:
        """Open a dataset."""
        fs, path = self.get_fs_and_path(file_name)

        def _get_engine(file_name: str) -> str:
            engines = {
                "cfggrib": (".grb", ".grib", "gb"),
                "h5netcdf": (".nc", ".nc4", ".netcdf", ".cdf", ".hdf5", ".h5"),
                "zarr": (".zarr", ".zar"),
            }
            for eng, suffixes in engines.items():
                for suffix in suffixes:
                    if file_name.endswith(suffix):
                        return eng
            return ""

        engine = read_kws.pop("engine", _get_engine(file_name)) or None

        if engine == "zarr":
            mapper = fs.get_mapper(path)
            return xr.open_zarr(mapper)
        with fs.open(path, "rb") as stream:
            return xr.open_dataset(stream, engine=engine)

    def lookup(self, file_name: str, *attrs: str, **read_kws: Any) -> Any:
        """Get metdata from a lookup table."""
        keys = tuple(attrs)
        d = cmor_lookup
        with self._lock:
            for a in keys[:-1]:
                d = d.setdefault(a, {})
            if keys[-1] in d:
                return d[attrs[-1]]
            d[keys[-1]] = self.read_attr(keys[-1], file_name, **read_kws)
        return d[keys[-1]]

    def read_attr(
        self, attr: str, file_name: str | pathlib.Path, **read_kws: Any
    ) -> str:
        with self.open_dataset(file_name, **read_kws) as dset:
            attrs = dset.attrs
            for var in dset.variables:
                for name, value in dset[var].attrs.items():
                    attrs[f"{var}.{name}"] = value
            return attrs[attr]

    @abc.abstractmethod
    async def is_dir(self, path: str | Path | pathlib.Path) -> bool:
        """Check if a given path is a directory object on the storage system.

        Parameter
        ---------
        path : str, asyncio.Path, pathlib.Path
            Path of the object store

        Returns
        -------
        bool: True if path is dir object, False if otherwise or doesn't exist
        """

    @abc.abstractmethod
    async def is_file(self, path: str | Path | pathlib.Path) -> bool:
        """Check if a given path is a file object on the storage system.

        Parameter
        ---------
        path : str, asyncio.Path, pathlib.Path
            Path of the object store


        Returns
        -------
        bool: True if path is file object, False if otherwise or doesn't exist

        """

    async def iterdir(
        self, path: str | Path | pathlib.Path
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
        yield str(path)  # pragma: no cover

    @abc.abstractmethod
    async def rglob(
        self, path: str | Path | pathlib.Path, glob_pattern: str = "*"
    ) -> AsyncIterator[Metadata]:
        """Search recursively for paths matching a given glob pattern.

        Parameters
        ----------
        path : str, asyncio.Path, pathlib.Path
            Path of the object store
        glob_pattern: str
            Pattern that the target files must match

        Yields
        ------
        str: Path of the object store that matches the glob pattern.
        """
        ...

    @staticmethod
    async def suffix(path: str | Path | pathlib.Path) -> str:
        """Get the suffix of a given input path.

        Parameters
        ----------
        path: str, asyncio.Path, pathlib.Path
            Path of the object store

        Returns
        -------
        str: The file type extension of the path.
        """
        return Path(path).suffix

    @staticmethod
    async def name(path: str | Path | pathlib.Path) -> str:
        """Get the name of the object store.

        Parameters
        ----------
        path: str, asyncio.Path, pathlib.Path
            Path of the object store

        Returns
        -------
        str: The 'file name' of the path.
        """
        return Path(path).name

    async def uri(self, path: str | Path | pathlib.Path) -> str:
        """Get the uri of the object store.

        Parameters
        ----------
        path: str, asyncio.Path, pathlib.Path
            Path of the object store

        Returns
        -------
        str: URI of the object store
        """
        return str(path)
