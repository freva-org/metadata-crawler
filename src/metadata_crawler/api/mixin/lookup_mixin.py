"""Definitions for lookup table mixins."""

import atexit
import os
from types import MappingProxyType
from typing import Any, Dict, Mapping, Tuple

from appdirs import user_cache_dir
from diskcache import Cache

from .lookup_tables import cmor_lookup as _NESTED

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


_flat: Dict[Key, Any] = {}

_dir = os.getenv("MDC_LOOKUP_CACHE_DIR") or os.path.join(
    user_cache_dir("metadata-crawler", "freva"), "lookup"
)
os.makedirs(_dir, exist_ok=True)
_DC = Cache(
    _dir, size_limit=2 * 1024**3, eviction="least-recently-used", cull_limit=10
)
atexit.register(_DC.close)


class LookupMixin:
    """Provide a Mixing with a process safe lookup().

    The mixin does:
      - process-wide static table (CMOR) via CMOR_STATIC
      - per-instance disk cache for file-derived attrs
      - in-flight de-duplication for concurrent misses

    Subclass must implement:
      def read_attr(self, attribute: str, path: str, **read_kws: Any) -> Any
    """

    CMOR_STATIC: Mapping[Key, Any] = {}

    def set_static_from_nested(self) -> None:
        """Flatting the cmor lookup table."""
        if not self.CMOR_STATIC:
            _flatten_static((), _NESTED, _flat)
            self.CMOR_STATIC = MappingProxyType(_flat)

    def read_attr(self, attribute: str, path: str, **read_kws: Any) -> Any:
        """Get a metadata attribute from a datastore object."""
        raise NotImplementedError  # pragma: no cover

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
        # 1) static fast-path
        try:
            return self.CMOR_STATIC[tree]
        except KeyError:
            pass
        # 2) process-safe disk cache (key includes path)
        val = _DC.get(tree)
        if val is None:
            val = self.read_attr(attribute, path, **read_kws)
            if not _DC.add(tree, val):
                val = _DC.get(tree)
        return val
