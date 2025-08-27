"""API for adding new storage backends via :py:class:`BasePath`."""

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
    TypedDict,
    Union,
    cast,
)

import h5netcdf
import xarray as xr
from anyio import Path
from pydantic import BaseModel, Field

from .mixin import LookupMixin, PathMixin, TemplateMixin


class MetadataType(TypedDict):
    """A dict representation of the metadata."""

    path: str
    metadata: Dict[str, Any]


class Metadata(BaseModel):
    """Meta data that is attached to each discovered path."""

    path: str
    metadata: Dict[str, Any] = Field(default_factory=dict)


class BasePath(abc.ABCMeta):
    """Every storage backend class should be of this type."""


class PathTemplate(
    abc.ABC, PathMixin, TemplateMixin, LookupMixin, metaclass=BasePath
):
    """Base class for interacting with different storage systems.

    This class defines fundamental methods that should be implemented
    to retrieve information across different storage systems.

    Parameters
    ^^^^^^^^^^
    suffixes: List[str], default:  [".nc", ".girb", ".zarr", ".tar", ".hdf5"]
        A list of available file suffixes.

    Other Parameters
    ^^^^^^^^^^^^^^^^
    storage_options: Any
        Information needed to interact with the storage system.

    Attributes
    ^^^^^^^^^^
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
    """Definition of the file system time for each implementation."""

    _lock = threading.RLock()

    def __init__(
        self, suffixes: Optional[List[str]] = None, **storage_options: Any
    ) -> None:

        self._user: str = os.environ.get("DRS_STORAGE_USER") or getuser()
        self._pw: str = os.environ.get("DRS_STORAGE_PASSWD") or ""
        self.suffixes = suffixes or [".nc", ".girb", ".zarr", ".tar", ".hdf5"]
        self.storage_options = cast(
            Dict[str, Any], self.render_templates(storage_options or {}, {})
        )
        self.set_static_from_nested()
        self.__post_init__()

    def __post_init__(self) -> None:
        """Call this method after the __init__ get called.

        If you need to assign any attributes redefine this method in your class.
        """

    async def close(self) -> None:
        """Close any open sessions."""

    def open_dataset(
        self, path: str, **read_kws: Any
    ) -> Union[xr.Dataset, h5netcdf.core.File]:
        """Open a dataset with xarray.

        Parameters
        ^^^^^^^^^^
        path:
            Path to the object store / file name
        **read_kws:
            Keyword arguments passed to open the datasets.

        Returns
        ^^^^^^-
        xarray.Dataset:
            The xarray dataset.
        """
        fs, path = self.get_fs_and_path(path)

        def _get_engine(file_name: str) -> str:
            engines = {
                "grb": "cfgrib",
                "grib": "cfgrib",
                "gb": "gb",
                "nc": "h5netcdf",
                "nc4": "h5netcdf",
                "netcdf": "h5netcdf",
                "cdf": "h5netcdf",
                "hdf5": "h5netcdf",
                "h5": "h5netcdf",
                "zarr": "zarr",
                "zar": "zarr",
            }
            suffix = file_name.rpartition(".")[-1]
            return engines.get(suffix, "")

        kwargs = read_kws.copy()
        engine = kwargs.setdefault("engine", _get_engine(path) or None)

        if engine == "zarr":
            dset: xr.Dataset = xr.open_zarr(fs.get_mapper(path))
            return dset
        if fs.protocol[0] == "file" and engine == "h5netcdf":
            return h5netcdf.File(path)
        if fs.protocol[0] == "file":
            return xr.open_mfdataset(path, **kwargs)
        with fs.open(path, "rb") as stream:
            return xr.open_dataset(stream, **kwargs)

    def read_attr(
        self, attribute: str, path: Union[str, pathlib.Path], **read_kws: Any
    ) -> Any:
        """Get a metadata attribute from a datastore object.

        Parameters
        ^^^^^^^^^^
        attr: The attribute that is queried can be of the form of
              <attribute>, <variable>.<attribute>, <attribute>,
              <variable>.<attribute>
        path: Path to the object store / file path
        read_kws: Keyword arguments for opening the datasets.

        Returns
        ^^^^^^^
        str: Metadata from the data.
        """
        with self.open_dataset(str(path), **read_kws) as dset:
            if "." not in attribute:
                return dset.attrs[attribute]
            var, _, attr = attribute.partition(".")
            return dset[var].attrs[attr]

    @abc.abstractmethod
    async def is_dir(self, path: Union[str, Path, pathlib.Path]) -> bool:
        """Check if a given path is a directory object on the storage system.

        Parameters
        ^^^^^^^^^^
        path : str, asyncio.Path, pathlib.Path
            Path of the object store

        Returns
        ^^^^^^-
        bool: True if path is dir object, False if otherwise or doesn't exist
        """

    @abc.abstractmethod
    async def is_file(self, path: Union[str, Path, pathlib.Path]) -> bool:
        """Check if a given path is a file object on the storage system.

        Parameters
        ^^^^^^^^^^
        path:
            Path of the object store

        Returns
        ^^^^^^^
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

        Parameters
        ^^^^^^^^^^
        path:
            Path of the object store

        Yields
        ^^^^^^
        str:
            1st level sub directory
        """
        yield ""  # pragma: no cover

    @abc.abstractmethod
    async def rglob(
        self, path: Union[str, Path, pathlib.Path], glob_pattern: str = "*"
    ) -> AsyncIterator[MetadataType]:
        """Search recursively for paths matching a given glob pattern.

        Parameters
        ^^^^^^^^^^
        path:
            Path of the object store
        glob_pattern: str
            Pattern that the target files must match

        Yields
        ^^^^^^
        MetadataType: Path of the object store that matches the glob pattern.
        """
        yield MetadataType(path="", metadata={})  # pragma: no cover

    def fs_type(self, path: Union[str, Path, pathlib.Path]) -> str:
        """Define the file system type."""
        return self._fs_type or ""

    @abc.abstractmethod
    def path(self, path: Union[str, Path, pathlib.Path]) -> str:
        """Get the full path (including any schemas/netlocs).

        Parameters
        ^^^^^^^^^^
        path:
            Path of the object store

        Returns
        ^^^^^^^
        str:
            URI of the object store
        """
        ...  # pragma: no cover

    @abc.abstractmethod
    def uri(self, path: Union[str, Path, pathlib.Path]) -> str:
        """Get the uri of the object store.

        Parameters
        ^^^^^^^^^^
        path:
            Path of the object store

        Returns
        ^^^^^^^
        str:
            URI of the object store
        """
        ...  # pragma: no cover
