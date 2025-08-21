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
from urllib.parse import urlsplit

import fsspec
import xarray as xr
from anyio import Path
from jinja2 import Template
from pydantic import BaseModel, Field

from ..backends.lookup_tables import cmor_lookup


class Metadata(BaseModel):
    """Meta data that is attached to each discovered path."""

    path: str
    metadata: Dict[str, Any] = Field(default_factory=dict)


class TemplateMixin:
    """Apply templating egine jinja2."""

    def storage_template(self, key: str, default: Optional[str] = None) -> str:
        """Template a jinja2 string."""
        value = self.storage_options.get(key) or ""
        default = default or ""
        return Template(value).render(env=os.environ) or default


class PathMixin:
    """Class that defines typical Path operations."""

    async def suffix(self, path: Union[str, Path, pathlib.Path]) -> str:
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

    async def name(self, path: Union[str, Path, pathlib.Path]) -> str:
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
        protocol, path = fsspec.core.split_protocol(uri)
        protocol = protocol or "file"
        path = urlsplit(uri.removeprefix(f"{protocol}://")).path
        return fsspec.filesystem(protocol), path


class BasePath(abc.ABCMeta):
    """Every storage backend class should be of this type."""


class PathTemplate(abc.ABC, PathMixin, TemplateMixin, metaclass=BasePath):
    """Base class for interacting with different storage systems.

    This class defines fundamental methods that should be implemented
    to retrieve information across different storage systems.

    Parameters
    ----------
    suffixes: List[str], default:  [".nc", ".girb", ".zarr", ".tar", ".hdf5"]
        A list of available file suffixes.

    **storage_options: Any
        Information needed to interact with the storage system.

    Attributes
    ----------
    _user : str
        Value of the ``DRS_STORAGE_USER`` env variable (defaults to current user)
    _pw : str
        a password passed by the ``DRS_STORAGE_PASSWD`` env variable
    suffixes: List[str]
        A list of available file suffixes.
    storage_options: Dist[str, Any]
        A dict with information needed to interact with the storage system.
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
        self.__post_init__()

    def __post_init__(self) -> None:
        """This method is called after the __init__ method. If you need to
        assign any attributes redifine this method in your class.
        """

    async def __aenter__(self) -> "PathTemplate":
        return self

    async def __aexit__(self, *args: Any, **kwargs: Any) -> None:
        await self.close()

    async def close(self) -> None:
        """Close any open sessions."""

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
                "cfgrib": (".grb", ".grib", ".gb"),
                "h5netcdf": (".nc", ".nc4", ".netcdf", ".cdf", ".hdf5", ".h5"),
                "zarr": (".zarr", ".zar"),
            }
            for eng, suffixes in engines.items():
                for suffix in suffixes:
                    if file_name.endswith(suffix):
                        return eng
            return ""

        kwargs = read_kws.copy()
        engine = kwargs.setdefault("engine", _get_engine(path) or None)

        if engine == "zarr":
            dset: xr.Dataset = xr.open_zarr(fs.get_mapper(path))
            return dset
        if fs.protocol[0] == "file":
            return xr.open_mfdataset(path, **kwargs)
        with fs.open(path, "rb") as stream:
            return xr.open_dataset(stream, **kwargs)

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
        d: Dict[str, Any] = cmor_lookup
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

        with self.open_dataset(str(path), **read_kws) as dset:
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
        ...  # pragma: no cover

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
        yield ""  # pragma: no cover

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
        yield Metadata(path="")  # pragma: no cover

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
        ...  # pragma: no cover

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
        ...  # pragma: no cover
