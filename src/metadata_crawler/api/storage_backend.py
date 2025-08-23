"""API for adding new storage backends via :py:class:`BasePath`
==============================================================
"""

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
    Mapping,
    Optional,
    Tuple,
    Union,
    cast,
)
from urllib.parse import urlsplit

import fsspec
import xarray as xr
from anyio import Path
from jinja2 import Environment, Undefined
from pydantic import BaseModel, Field

from ..backends.lookup_tables import cmor_lookup


class Metadata(BaseModel):
    """Meta data that is attached to each discovered path."""

    path: str
    metadata: Dict[str, Any] = Field(default_factory=dict)


class TemplateMixin:
    """Apply templating egine jinja2."""

    storage_options: Optional[Dict[str, Any]] = None

    @staticmethod
    def render_templates(
        data: Any,
        context: Mapping[str, Any],
        *,
        max_passes: int = 2,
    ) -> Any:
        """
        Recursively render Jinja2 templates found in strings within arbitrary data.

        This function traverses common container types (``dict``, ``list``,
        ``tuple``, ``set``), dataclasses, namedtuples, and ``pathlib.Path`` objects.
        Every string encountered is treated as a Jinja2 template and rendered with
        the provided ``context``. Rendering can be repeated up to ``max_passes``
        times to resolve templates that produce further templates on the first pass.

        Parameters
        ^^^^^^^^^^
        data:
            Arbitrary Python data structure. Supported containers are ``dict``
            (keys and values), ``list``, ``tuple`` (including namedtuples),
            ``set``, dataclasses (fields), and ``pathlib.Path``.
            Scalars (e.g., ``int``, ``float``, ``bool``, ``None``) are returned
            unchanged. Strings are rendered as Jinja2 templates.
        context:
            Mapping of template variables available to Jinja2 during rendering.
        max_passes:
            Maximum number of rendering passes to perform on each string,
            by default ``2``. Increase this if templates generate further
            templates that need resolution.

        Returns
        ^^^^^^^
        Any:
            A structure of the same shape with all strings rendered. Container and
            object types are preserved where feasible (e.g., ``tuple`` stays a
            ``tuple``, namedtuple stays a namedtuple, dataclass remains the
            same dataclass type).

        Raises
        ^^^^^^^
        jinja2.TemplateError
            For other Jinja2 template errors encountered during rendering.

        Notes
        ^^^^^^
        * Dictionary keys are also rendered if they are strings (or nested
          containers with strings). If rendering causes key collisions, the
          **last** rendered key wins.
        * For dataclasses, all fields are rendered and a new instance is returned using
          ``dataclasses.replace``. Frozen dataclasses are supported.
        * Namedtuples are detected via the ``_fields`` attribute and
          reconstructed with the same type.

        Examples
        ^^^^^^^^^

        .. code-block::python

            data = {
                "greeting": "Hello, {{ name }}!",
                "items": ["{{ count }} item(s)", 42],
                "path": {"root": "/home/{{ user }}", "cfg": "{{ root }}/cfg"},
            }
            ctx = {"name": "Ada", "count": 3, "user": "ada", "root": "/opt/app"}
            render_templates(data, ctx)
        {'greeting': 'Hello, Ada!',
         'items': ['3 item(s)', 42],
         'path': {'root': '/home/ada', 'cfg': '/opt/app/cfg'}}
        """
        env = Environment(undefined=Undefined, autoescape=False)

        env_map = dict(os.environ)

        def _env_get(name: str, default: Any = None) -> Any:
            return env_map.get(name, default)

        def _getenv_filter(varname: str, default: Any = None) -> Any:
            return env_map.get(varname, default)

        env.globals.setdefault("env", _env_get)
        env.globals.setdefault("ENV", env_map)
        env.filters.setdefault("getenv", _getenv_filter)

        def _render_str(s: str) -> str:
            out = s
            for _ in range(max_passes):
                new = env.from_string(out).render(context)
                if new == out:
                    break
                out = new
            return out

        def _walk(obj: Any) -> Any:
            if isinstance(obj, str):
                return _render_str(obj)

            if isinstance(obj, dict):
                rendered: dict[Any, Any] = {}
                for k, v in obj.items():
                    rk = _render_str(k) if isinstance(k, str) else k
                    rendered[rk] = _walk(v)
                return rendered

            if isinstance(obj, list):
                return [_walk(x) for x in obj]

            if isinstance(obj, tuple):
                return tuple(_walk(x) for x in obj)

            if isinstance(obj, set):
                return {_walk(x) for x in obj}

            return obj

        return _walk(data)


class PathMixin:
    """Class that defines typical Path operations."""

    async def suffix(self, path: Union[str, Path, pathlib.Path]) -> str:
        """Get the suffix of a given input path.

        Parameters
        ^^^^^^^^^^
        path: str, asyncio.Path, pathlib.Path
            Path of the object store

        Returns
        ^^^^^^-
        str: The file type extension of the path.
        """
        return Path(path).suffix

    def get_fs_and_path(self, uri: str) -> Tuple[fsspec.AbstractFileSystem, str]:
        """Return (fs, path) suitable for xarray.

        Parameters
        ^^^^^^^^^^
        uri:
            Path to the object store / file name


        Returns
        ^^^^^^-
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
    ^^^^^^^^^^
    suffixes: List[str], default:  [".nc", ".girb", ".zarr", ".tar", ".hdf5"]
        A list of available file suffixes.

    **storage_options: Any
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
    """Defination of the file system time for each implementation."""

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
        self.__post_init__()

    def __post_init__(self) -> None:
        """This method is called after the __init__ method. If you need to
        assign any attributes redifine this method in your class.
        """

    async def close(self) -> None:
        """Close any open sessions."""

    def open_dataset(self, path: str, **read_kws: Any) -> xr.Dataset:
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

    def lookup(
        self, path: str, attribute: str, *tree: str, **read_kws: Any
    ) -> Any:
        """Get metdata from a lookup table.

        This function will read metadata from a pre-defined cache table and if
        the metadata is not present in the cache table it'll read the
        the object store and add the metdata to the cache table.

        Parameters
        ^^^^^^^^^^

        path:
            Path to the object store / file name
        attribute:
            The attribute that is retrieved from the data.
            variable attributes can be definde by a ``.``.
            For example: ``tas.long_name`` would get attribute ``long_name``
            from variable ``tas``.
        *tree:
            A tuple representing nested attributes. Attributes are nested for
            more efficent lookup. ('atmos', '1hr', 'tas') will translate into
            a tree of ['atmos']['1hr']['tas']
        **read_kws:
            Keyword arguments passed to open the datasets.

        """
        keys = tuple(tree) + (attribute,)
        d: Dict[str, Any] = cmor_lookup
        with self._lock:
            for a in keys[:-1]:
                d = d.setdefault(a, {})
            if keys[-1] in d:
                return d[attribute]
            d[keys[-1]] = self.read_attr(attribute, path, **read_kws)
        return d[keys[-1]]

    def read_attr(
        self, attribute: str, path: Union[str, pathlib.Path], **read_kws: Any
    ) -> Any:
        """Get a metadata attribute from a datastore object.

        Parameters
        ^^^^^^^^^^
        attr: The attribute that is queried can be of the form of
              <attribute>, <variable>.<attribute>, <attribute>,
              <variable>.<attribute>
        path: Path to the object strore / file path
        read_kws: Keyword arguments for opening the datasets.

        Retruns
        ^^^^^^^
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
    ) -> AsyncIterator[Metadata]:
        """Search recursively for paths matching a given glob pattern.

        Parameters
        ^^^^^^^^^^
        path:
            Path of the object store
        glob_pattern: str
            Pattern that the target files must match

        Yields
        ^^^^^^
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
