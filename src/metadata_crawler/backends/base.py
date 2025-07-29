"""API for adding new storage backends via :py:class:`BasePath`
==============================================================
"""

from __future__ import annotations

import abc
import os
import pathlib
import threading
from getpass import getuser
from typing import (
    Any,
    AsyncIterator,
    ClassVar,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
)

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

    _fs_type: ClassVar[Optional[str]]
    """Defination of the file system time for each implementation."""

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

    async def __aexit__(self, *args: Any, **kwargs: Any) -> None:
        await self.close()

    async def close(self) -> None:
        """Close any open sessions."""

    def get_fs_and_path(self, path: str) -> Tuple[fsspec.AbstractFileSystem, str]:
        """Return (fs, path) suitable for xarray.

        Parameters
        ----------
        path:
            Path to the object store / file name


        Returns
        -------
        fsspec.AbstractFileSystem, str:
            The AbstractFileSystem class and the corresponding path to the
            data store.

        """
        return fs_and_path(path)

    def open_dataset(self, path: str, **read_kws: Any) -> xr.Dataset:
        """Open a dataset with xarray.

        Parameters
        ----------
        path:
            Path to the object store / file name
        **read_kws:
            Keyword arguments passed to open the datasets.

        Returns
        -------
        xarray.Dataset:
            The xarray dataset.
        """
        fs, path = self.get_fs_and_path(path)

        def _get_engine(file_name: str) -> str:
            engines = {
                "cfgrib": (".grb", ".grib", "gb"),
                "h5netcdf": (".nc", ".nc4", ".netcdf", ".cdf", ".hdf5", ".h5"),
                "zarr": (".zarr", ".zar"),
            }
            for eng, suffixes in engines.items():
                for suffix in suffixes:
                    if file_name.endswith(suffix):
                        return eng
            return ""

        engine = read_kws.pop("engine", _get_engine(path)) or None

        if engine == "zarr":
            mapper = fs.get_mapper(path)
            return xr.open_zarr(mapper)
        with fs.open(path, "rb") as stream:
            return xr.open_dataset(stream, engine=engine)

    def lookup(self, path: str, *attrs: str, **read_kws: Any) -> Any:
        """Get metdata from a lookup table.

        This function will read metadata from a pre-defined cache table and if
        the metadata is not present in the cache table it'll read the
        the object store and add the metdata to the cache table.

        Parameters
        ----------

        path:
            Path to the object store / file name
        *attrs:
            A tuple represnting nested attributes. Attributes are nested for
            more efficent lookup. ('atmos', '1hr', 'tas') will translate into
            a tree of ['atmos']['1hr']['tas']
        **read_kws:
            Keyword arguments passed to open the datasets.

        """
        keys = tuple(attrs)
        d = cmor_lookup
        with self._lock:
            for a in keys[:-1]:
                d = d.setdefault(a, {})
            if keys[-1] in d:
                return d[attrs[-1]]
            d[keys[-1]] = self.read_attr(keys[-1], path, **read_kws)
        return d[keys[-1]]

    def read_attr(
        self, attribute: str, path: Union[str, pathlib.Path], **read_kws: Any
    ) -> Any:
        """Get a metadata attribute from a datastore object.

        Parameters
        ----------
        attr: The attribute that is queired can be of the form of
              <attribute>, <variable>.<attribute>, <attribute>,
              <variable>.<attribute>
        path: Path to the object strore / file path
        read_kws: Keyword arguments for opening the datasets.

        Retruns
        -------
        str: Metadata from the data.
        """

        with self.open_dataset(path, **read_kws) as dset:
            attrs = dset.attrs
            for var in dset.variables:
                for name, value in dset[var].attrs.items():
                    attrs[f"{var}.{name}"] = value
            return attrs[attribute]

    @abc.abstractmethod
    async def is_dir(self, path: Union[str, Path, pathlib.Path]) -> bool:
        """Check if a given path is a directory object on the storage system.

        Parameters
        ----------
        path : str, asyncio.Path, pathlib.Path
            Path of the object store

        Returns
        -------
        bool: True if path is dir object, False if otherwise or doesn't exist
        """

    @abc.abstractmethod
    async def is_file(self, path: Union[str, Path, pathlib.Path]) -> bool:
        """Check if a given path is a file object on the storage system.

        Parameter
        ---------
        path : str, asyncio.Path, pathlib.Path
            Path of the object store

        Returns
        -------
        bool:
            True if path is file object, False if otherwise or doesn't exist

        """
        ...

    @abc.abstractmethod
    async def iterdir(
        self,
        path: Union[str, Path, pathlib.Path],
    ) -> AsyncIterator[str]:
        """Get all sub directories from a given path.

        Parameter
        ---------
        path : str, asyncio.Path, pathlib.Path
            Path of the object store

        Yields
        ------
        str:
            1st level sub directory
        """
        ...

    @abc.abstractmethod
    async def rglob(
        self, path: Union[str, Path, pathlib.Path], glob_pattern: str = "*"
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
    async def suffix(path: Union[str, Path, pathlib.Path]) -> str:
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
    async def name(path: Union[str, Path, pathlib.Path]) -> str:
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

    def fs_type(self, path: Union[str, Path, pathlib.Path]) -> str:
        """Define the file system type."""
        return self._fs_type or ""

    @abc.abstractmethod
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
        ...

    @abc.abstractmethod
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
        ...
