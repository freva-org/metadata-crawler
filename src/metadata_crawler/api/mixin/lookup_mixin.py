"""Definitions for lookup table mixins."""

import abc
from collections import OrderedDict
from threading import Event, Lock
from types import MappingProxyType
from typing import (
    Any,
    ClassVar,
    Dict,
    Iterator,
    Mapping,
    MutableMapping,
    Tuple,
)

from .lookup_tables import cmor_lookup as _CMOR_NESTED

Key = Tuple[str, ...]


def _flatten_static(
    prefix: Tuple[str, ...], node: Mapping[str, Any], out: Dict[Key, Any]
) -> None:
    """Flatten nested CMOR-like dict.

      cmip6 -> CF3hr -> tas -> {'realm': 'atmos', 'time-frequency': '3hrPt', ...}
    into keys ('cmip6','CF3hr','tas','realm') -> 'atmos'.
    """
    for k, v in node.items():
        if isinstance(v, Mapping):
            if v and not all(isinstance(x, Mapping) for x in v.values()):
                for leaf_k, leaf_v in v.items():
                    out[prefix + (k, leaf_k)] = leaf_v
            else:
                _flatten_static(prefix + (k,), v, out)
        else:
            out[prefix + (k,)] = v


class _LRU(MutableMapping[Key, Any]):
    """Tiny LRU (thread-unsafe; guarded by mixin lock)."""

    def __init__(self, maxsize: int = 100_000) -> None:
        self.maxsize = maxsize
        self._od: "OrderedDict[Key, Any]" = OrderedDict()

    def __getitem__(self, k: Key) -> Any:
        v = self._od.pop(k)
        self._od[k] = v
        return v

    def get(self, k: Key, default: Any = None) -> Any:
        if k in self._od:
            v = self._od.pop(k)
            self._od[k] = v
            return v
        return default

    def __setitem__(self, k: Key, v: Any) -> None:
        if k in self._od:
            self._od.pop(k)
        self._od[k] = v
        if len(self._od) > self.maxsize:
            self._od.popitem(last=False)

    def __delitem__(self, k: Key) -> None:
        del self._od[k]

    def __iter__(self) -> Iterator[Any]:
        return iter(self._od)

    def __len__(self) -> int:
        return len(self._od)

    def clear(self) -> None:
        self._od.clear()


class LookupMixin:
    """Provide a Mixing with a thread safe lookup().

    The mixin does:
      - process-wide static table (CMOR) via CMOR_STATIC
      - per-instance dynamic cache (LRU) for file-derived attrs
      - in-flight de-duplication for concurrent misses

    Subclass must implement:
      def read_attr(self, attribute: str, path: str, **read_kws: Any) -> Any
    """

    # ---- class-level knobs (override in subclass if desired) ----
    LOOKUP_CACHE_MAXSIZE: ClassVar[int] = 100_000
    LOOKUP_INCLUDE_PATH_IN_KEY: ClassVar[bool] = False

    # Read-only static CMOR table (flattened). Set this once from your cmor_lookup.
    CMOR_STATIC: Mapping[Key, Any] = MappingProxyType({})

    def set_static_from_nested(self) -> None:
        """Flatting the cmor lookup table."""
        if not self.CMOR_STATIC:
            flat: Dict[Key, Any] = {}
            _flatten_static((), _CMOR_NESTED, flat)
            self.CMOR_STATIC = MappingProxyType(flat)
        self._ensure_lookup_state()

    # ---- lazy state so we don't need __init__ ----
    def _ensure_lookup_state(self) -> None:
        if not hasattr(self, "_lookup__ready"):
            object.__setattr__(self, "_lookup__lk", Lock())
            object.__setattr__(
                self, "_lookup__cache", _LRU(self.LOOKUP_CACHE_MAXSIZE)
            )
            object.__setattr__(self, "_lookup__inflight", {})
            object.__setattr__(
                self,
                "_lookup__include_path",
                bool(self.LOOKUP_INCLUDE_PATH_IN_KEY),
            )
            object.__setattr__(self, "_lookup__ready", True)

    # ---- key strategy (override if you need something else) ----
    def _static_key(self, attribute: str, tree: Tuple[str, ...]) -> Key:
        return (*tree, attribute)

    def _dynamic_key(
        self, path: str, attribute: str, tree: Tuple[str, ...]
    ) -> Key:
        include_path: bool = getattr(self, "_lookup__include_path")
        return (path, *tree, attribute) if include_path else (*tree, attribute)

    @abc.abstractmethod
    def read_attr(self, attribute: str, path: str, **read_kws: Any) -> Any:
        """Get a metadata attribute from a datastore object."""
        ...  # pragma: no cover

    def lookup(
        self, path: str, attribute: str, *tree: str, **read_kws: Any
    ) -> Any:
        """Get metadata from a lookup table.

        This function will read metadata from a pre-defined cache table and if
        the metadata is not present in the cache table it'll read the
        the object store and add the metadata to the cache table.

        Parameters
        ^^^^^^^^^^

        path:
            Path to the object store / file name
        attribute:
            The attribute that is retrieved from the data.
            variable attributes can be defined by a ``.``.
            For example: ``tas.long_name`` would get attribute ``long_name``
            from variable ``tas``.
        *tree:
            A tuple representing nested attributes. Attributes are nested for
            more efficient lookup. ('atmos', '1hr', 'tas') will translate into
            a tree of ['atmos']['1hr']['tas']

        Other Parameters
        ^^^^^^^^^^^^^^^^
        **read_kws:
            Keyword arguments passed to open the datasets.

        """
        self._ensure_lookup_state()
        tree_t = tuple(tree)

        # 1) static CMOR (no lock)
        skey = self._static_key(attribute, tree_t)
        try:
            return self.CMOR_STATIC[skey]
        except KeyError:
            pass

        # 2) dynamic cache (first check without lock)
        dkey = self._dynamic_key(path, attribute, tree_t)
        cache: _LRU = getattr(self, "_lookup__cache")
        val = cache.get(dkey, None)
        if val is not None:
            return val

        lk: Lock = getattr(self, "_lookup__lk")
        inflight: Dict[Key, Event] = getattr(self, "_lookup__inflight")

        # 3) create/find in-flight guard under lock (double-checked)
        with lk:
            if dkey in cache:
                return cache[dkey]
            ev = inflight.get(dkey)
            if ev is None:
                ev = Event()
                inflight[dkey] = ev
                owner = True
            else:
                owner = False

        if owner:
            try:
                val = self.read_attr(attribute, path, **read_kws)
            finally:
                with lk:
                    if val is not None:
                        cache[dkey] = val
                    ev.set()
                    inflight.pop(dkey, None)
            return val
        else:
            ev.wait()
            with lk:
                return cache.get(dkey, None)

    def clear_dynamic_cache(self) -> None:
        """Clear the cache if needed."""
        self._ensure_lookup_state()
        getattr(self, "_lookup__cache").clear()
        getattr(self, "_lookup__inflight").clear()
