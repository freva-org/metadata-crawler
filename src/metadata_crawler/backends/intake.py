"""Interact with the INTAKE metadata catalogues."""

from __future__ import annotations

import pathlib
from fnmatch import fnmatch
from types import NoneType
from typing import (
    Any,
    AsyncIterator,
    Callable,
    Dict,
    Union,
)
from urllib.parse import unquote, urlparse

import fsspec
import intake
import pandas as pd
from anyio import Path

from ..api.storage_backend import Metadata, MetadataType, PathTemplate
from ..logger import logger


class IntakePath(PathTemplate):
    """Class to interact with the Intake metadata catalogues."""

    _fs_type = None

    async def is_file(self, path: str | Path | pathlib.Path) -> bool:
        """Check if a given path is a file."""
        return True

    async def is_dir(self, path: str | Path | pathlib.Path) -> bool:
        """Check if a given path is a directory."""
        return False

    @staticmethod
    def _normalize_path(path: str) -> str:
        """Turn file:// URLs into OS paths; leave others as-is."""
        if isinstance(path, str) and path.startswith("file://"):
            return unquote(urlparse(path).path)
        return path

    async def _walk_yaml_catalogue(
        self,
        cat: intake.catalog.Catalog,
    ) -> AsyncIterator[MetadataType]:

        for name in cat:
            entry = cat[name]
            container = getattr(entry, "container", None)

            if container == "catalog":
                async for md in self._walk_yaml_catalogue(entry()):
                    yield md
                continue

            src = entry()
            meta = getattr(src, "_entry", src).describe() or {}
            args = meta.get("args", {})
            urlpath = (
                args.get("urlpath")
                or args.get("path")
                or args.get("url")
                or meta.get("uri")
                or meta.get("file")
                or args.get("urlpaths")
            ) or []
            for raw_path in urlpath if isinstance(urlpath, list) else [urlpath]:
                path = self._normalize_path(raw_path)
                logger.debug("Found file %s", path)
                yield MetadataType(
                    path=path,
                    metadata=getattr(src, "metadata", meta.get("metadata", {})),
                )

    @staticmethod
    def _to_py(value: Any) -> Any:
        if isinstance(value, (float, int, bool, str, NoneType)):
            return value
        try:
            if hasattr(value, "tolist"):
                return value.tolist()
            if pd.isna(value):
                return None
        except Exception:
            pass
        return value

    async def _walk_esm_catalogue(
        self,
        cat: intake.catalog.Catalog,
    ) -> AsyncIterator[MetadataType]:
        df: pd.DataFrame = getattr(cat, "df", pd.DataFrame())
        cols = list(df.columns)
        for row in df.itertuples(index=False, name=None):
            meta: Dict[str, Any] = {k: self._to_py(v) for k, v in zip(cols, row)}
            urlpath = (
                meta.get("urlpath")
                or meta.get("path")
                or meta.get("url")
                or meta.get("uri")
                or meta.get("file")
                or meta.get("urlpaths")
            ) or []
            for raw_path in urlpath if isinstance(urlpath, list) else [urlpath]:
                path = self._normalize_path(raw_path)
                logger.debug("Found file %s", path)
                yield MetadataType(path=path, metadata=meta)

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
        yield str(path)

    def _is_esm_catalogue(self, path: str) -> bool:
        if not self._normalize_path(path).endswith(".json"):
            return False
        esmcat = False
        fs = fsspec.get_filesystem_class(
            fsspec.core.split_protocol(path)[0] or "file"
        )(**self.storage_options)
        with fs.open(path, mode="rb", **self.storage_options) as stream:
            num = 0
            for line in stream:
                if "esmcat" in line.decode("utf-8"):
                    esmcat = True
                    break
                if num > 19:
                    break
                num += 1
        return esmcat

    async def rglob(
        self, path: str | Path | pathlib.Path, glob_pattern: str = "*"
    ) -> AsyncIterator[MetadataType]:
        """Go through catalogue path."""
        path = str(path)
        if self._is_esm_catalogue(path):
            cat: intake.catalog.Catalog = intake.open_esm_datastore(
                path, **self.storage_options
            )
            func: Callable[[str], AsyncIterator[MetadataType]] = (
                self._walk_esm_catalogue
            )
        else:
            cat = intake.open_catalog(path, **self.storage_options)
            func = self._walk_yaml_catalogue

        async for md in func(cat):
            if "." + md["path"].rpartition(".")[-1] in self.suffixes and fnmatch(
                md["path"], glob_pattern
            ):
                yield md

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
        return str(path)

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
        fs_type, path = fsspec.core.split_protocol(str(path))
        fs_type = fs_type or "file"
        return f"{fs_type}://{path}"

    def fs_type(self, path: Union[str, Path, pathlib.Path]) -> str:
        """Define the file system type."""
        fs_type, _ = fsspec.core.split_protocol(str(path))
        return fs_type or "posix"

    async def walk(self, path: str) -> AsyncIterator[Metadata]:
        """Walk a catalogue."""
        async for md in self.rglob(path):
            yield Metadata(path=md["path"], metadata=md["metadata"])
