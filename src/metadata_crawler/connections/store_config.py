"""Named connections to catalogue stores."""

from __future__ import annotations

from pathlib import Path
from typing import (
    Any,
    Dict,
    Iterator,
    Mapping,
    Type,
    Union,
)

from ..api.stores.base import BaseConnection, ConnT

StorageOptions = Dict[str, Any]


class ConfigFiles:
    """Merged and validated connections, read once."""

    def __init__(self, raw: Mapping[str, Mapping[str, Any]]) -> None:
        self._raw = {name: dict(entry) for name, entry in raw.items()}
        # Validate everything up front, so broken entries fail at the ``with``.
        self._conns: Dict[str, BaseConnection] = {
            name: self._validate(name, entry) for name, entry in self._raw.items()
        }
        self._closed = False

    @staticmethod
    def _validate(name: str, entry: Mapping[str, Any]) -> BaseConnection:
        url = entry.get("url") or entry.get("path") or entry.get("uri")
        if not url:
            raise ValueError(f"store {name!r} has no 'url/path/uri'")
        model = (
            BaseConnection.for_backend(str(entry["backend"]))
            if entry.get("backend")
            else BaseConnection.for_url(url=url)
        )
        data = {k: v for k, v in entry.items() if k != "backend"}
        return model.model_validate({"name": name, **data})

    def _check_open(self) -> None:
        if self._closed:
            raise RuntimeError("the configuration was already closed")

    def __contains__(self, name: object) -> bool:
        return name in self._conns

    def __iter__(self) -> Iterator[str]:
        return iter(self._conns)

    def __getitem__(self, name: str) -> BaseConnection:
        self._check_open()
        try:
            return self._conns[name]
        except KeyError:
            known = ", ".join(sorted(self._conns)) or "none"
            raise KeyError(f"unknown store {name!r} (configured: {known})") from None

    def get(self, name: str, kind: Type[ConnT]) -> ConnT:
        """Get a configured connection, checked to be of the given type."""
        conn = self[name]
        if not isinstance(conn, kind):
            raise TypeError(f"store {name!r} is a {conn.backend} store")
        return conn

    def resolve(self, ref: Union[str, Path], **overrides: Any) -> BaseConnection:
        """Get a configured store by name, or an ad-hoc one for a URL or path.

        *overrides* (``-s`` on the command line) win over the configuration.
        """
        self._check_open()
        ref = str(ref)
        if "://" not in ref and ref in self._conns:
            if not overrides:
                return self._conns[ref]
            return self._validate(ref, {**self._raw[ref], **overrides})
        return self._validate(ref, {"url": ref, **overrides})

    def close(self) -> None:
        """Drop all connections; secrets taken out of the block stay alive."""
        self._conns.clear()
        self._raw.clear()
        self._closed = True
