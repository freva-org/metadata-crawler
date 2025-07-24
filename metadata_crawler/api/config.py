from __future__ import annotations

import asyncio
import inspect
import os
import textwrap
from copy import deepcopy
from enum import StrEnum
from pathlib import Path
from typing import Any, Callable, Dict, List, Literal, Optional
from urllib.parse import urlsplit

import numpy as np
import tomli
import tomlkit
import xarray
from pydantic import BaseModel, ConfigDict, Field, ValidationError, root_validator
from tomlkit.container import OutOfOrderTableProxy, Table

from ..backends.base import Metadata
from ..utils import load_plugins


def is_async_callable(fn: Callable[..., Any]) -> bool:
    """
    Return True if `fn` is an async function (i.e. defined with `async def`).
    """
    return asyncio.iscoroutinefunction(fn) or inspect.iscoroutinefunction(fn)


class MetadataSource(StrEnum):
    """Representation of how the metadata should be retrieved."""

    storage = "storage"  # via intake/fdb5/etc.
    path = "path"  # parse via specs_dir/specs_file
    data = "data"  # read attributes from the file itself


class VarAttrRule(BaseModel):
    var: str  # "__variable__" or literal; supports "{variable}" format
    attr: str  # attribute name on DataArray.attrs
    default: Any = None


class StatRule(BaseModel):
    stat: Literal["min", "max", "minmax", "range", "bbox"]
    var: Optional[str] = None  # for numeric stats on a single var
    vars: Optional[List[str]] = None  # for bbox: [lat, lon]
    coord: Optional[str] = None  # for time range, etc.
    lat: Optional[str] = None  # convenience keys for bbox
    lon: Optional[str] = None
    default: Any = None


class ConfigMerger:
    """
    Loads a system and user TOML, merges user → system under 'drs_settings',
    preserves comments/formatting, and lets you inspect or write the result.
    """

    def __init__(self, user_path: Path | str):
        # parse both documents
        system_path = Path(__file__).parent / "drs_config.toml"
        self._system_doc = tomlkit.parse(system_path.read_text(encoding="utf-8"))
        self._user_doc = tomlkit.parse(
            Path(user_path).read_text(encoding="utf-8")
        )
        self._merge_tables(self._system_doc, self._user_doc)

    def _merge_tables(self, base: Table, override: Table) -> None:

        for key, value in override.items():
            if key not in base:
                base[key] = value
                continue
            if isinstance(value, (Table, OutOfOrderTableProxy)):
                self._merge_tables(base[key], value)
            else:
                base[key] = value

    @property
    def merged_doc(self) -> tomlkit.TOMLDocument:
        """Return the merged TOMLDocument."""
        return self._system_doc

    def dumps(self) -> str:
        """Return the merged document as a TOML string."""
        return tomlkit.dumps(self.merged_doc)


def strip_protocol(inp: str | Path) -> Path:
    """Extract the path from a given input file system."""
    abs_path = Path(urlsplit(str(inp)).path).expanduser()
    return Path(abs_path)


class CrawlerSettings(BaseModel):
    """Define the user input for a data crawler session."""

    name: str
    search_path: str


class PathSpecs(BaseModel):
    """Implementation of the Directory reference syntax."""

    dir_parts: List[str] = Field(default_factory=list)
    file_parts: List[str] = Field(default_factory=list)
    file_sep: str = "_"

    def get_metadata_from_path(self, rel_path: Path) -> Dict[str, Any]:
        """Read path encoded metadata from path specs."""
        dir_parts = rel_path.parent.parts
        file_parts = rel_path.name.split(self.file_sep)
        if len(dir_parts) == len(self.dir_parts):
            data: Dict[str, Any] = dict(zip(self.dir_parts, dir_parts))
        else:
            raise ValueError(
                (
                    f"Number of dir parts for {rel_path.parent} do not match "
                    f"in {rel_path} ({len(self.dir_parts)} {len(dir_parts)})"
                )
            )
        if len(file_parts) == len(self.file_parts):
            _parts = dict(zip(self.file_parts, file_parts))
        elif (
            len(file_parts) == len(self.file_parts) - 1 and "fx" in rel_path.name
        ):
            _parts = dict(zip(self.file_parts[:-1], file_parts))
        else:
            raise ValueError(
                (
                    f"Number of file parts for {rel_path.name} do not match "
                    f"in {rel_path} ({len(self.file_parts)} "
                    f"{len(file_parts)})"
                )
            )
        _parts.setdefault("time", "fx")
        data.update({k: v for (k, v) in _parts.items() if k not in data})
        data.pop("_", None)
        return data


class DataSpecs(BaseModel):
    globals: Dict[str, str] = Field(default_factory=dict)
    var_attrs: Dict[str, VarAttrRule] = Field(default_factory=dict)
    stats: Dict[str, StatRule] = Field(default_factory=dict)
    read_kws: Dict[str, Any] = Field(default_factory=dict)

    @staticmethod
    def _resolve_placeholder(value: str, data: Dict[str, Any]) -> Any:
        if value.startswith("__coord:"):
            value.split(":", 1)[1]
        # allow "{variable}" style
        for k, v in data.items():
            if k in value:
                return v
        return value

    def _set_global_attributes(
        self, dset: "xarray.Dataset", out: Dict[str, Any]
    ) -> None:

        for facet, attr in self.globals.items():
            if attr == "__variable__":
                out[facet] = list(map(str, dset.data_vars))
            else:
                out[facet] = dset.attrs.get(attr)

    def _set_variable_attributes(
        self, dset: "xarray.Dataset", out: Dict[str, Any]
    ) -> None:

        def get_val(rule: VarAttrRule, vname: str):
            default = rule.default.replace("__name__", vname) or vname
            if vname in dset:
                return dset[vname].attrs.get(rule.attr, default)
            return default

        for facet, rule in self.var_attrs.items():
            resolved: str | List[str] = self._resolve_placeholder(rule.var, out)
            var_list = [resolved] if isinstance(resolved, str) else list(resolved)
            vals = [get_val(rule, v) for v in var_list]
            if len(vals) == 1:
                out[facet] = vals[0]
            else:
                out[facet] = vals

    def _apply_stats_rules(
        self, dset: "xarray.Dataset", out: Dict[str, Any]
    ) -> None:

        for facet, rule in self.stats.items():
            match rule.stat:
                case "bbox":
                    lat = rule.lat or (rule.vars[0] if rule.vars else None)
                    lon = rule.lon or (
                        rule.vars[1] if rule.vars and len(rule.vars) > 1 else None
                    )
                    if lat in dset and lon in dset:
                        latv = dset[lat].values
                        lonv = dset[lon].values
                        out[facet] = [
                            float(lonv.min()),
                            float(lonv.max()),
                            float(latv.min()),
                            float(latv.max()),
                        ]
                    else:
                        out[facet] = rule.default

                case "range":
                    coord = (
                        self._resolve_placeholder(rule.coord, out, dset)
                        if rule.coord
                        else None
                    )
                    if coord and coord in dset.coords:
                        arr = dset.coords[coord].values
                        out[facet] = [str(arr.min()), str(arr.max())]
                    else:
                        out[facet] = rule.default

                case "min" | "max" | "minmax":
                    var_name = (
                        self._resolve_placeholder(rule.var, out, dset)
                        if rule.var
                        else None
                    )
                    if var_name and var_name in dset:
                        arr = dset[var_name].values
                        if rule.stat == "min":
                            out[facet] = float(np.nanmin(arr))
                        elif rule.stat == "max":
                            out[facet] = float(np.nanmax(arr))
                        else:
                            out[facet] = [
                                float(np.nanmin(arr)),
                                float(np.nanmax(arr)),
                            ]
                    else:
                        out[facet] = rule.default

    def extract_from_data(self, dset: xarray.Dataset) -> Dict[str, Any]:
        """Extract metadata from the data."""
        data: Dict[str, Any] = {}
        self._set_global_attributes(dset, data)
        self._set_variable_attributes(dset, data)
        self._apply_stats_rules(dset, data)
        return data


class Datasets(BaseModel):
    """Definition of datasets that should be crawled."""

    __pydantic_extra__: Dict[str, str] = Field(init=False)
    model_config = ConfigDict(extra="allow")
    root_path: str | Path
    drs_format: str = "cmip5"
    fs_type: str = "posix"
    username: Optional[str] = None
    api_key: Optional[str] = None
    defaults: Dict[str, Any] = Field(default_factory=dict)
    storage_options: Dict[str, Any] = Field(default_factory=dict)

    @root_validator(pre=True)
    def validate_storage_options(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        # ensure only allowed sources are present
        storage_options = values.get("storage_options", {})
        for option in storage_options:
            value = storage_options[option]
            if isinstance(value, str) and value.lower().startswith("__env:"):
                key, _, default = value.split(":", 1)[1].partition(",")
                storage_options[option] = os.getenv(key.upper(), default)

        values["storage_options"] = storage_options
        return values

    def model_post_init(self, __context: Any = None) -> None:
        storage_plugins = load_plugins("metadata_crawler.storage")
        try:
            self.backend = storage_plugins[self.fs_type](**self.storage_options)
        except KeyError:
            raise NotImplementedError(
                f"Backend not available. `{self.fs_type}` extension missing?"
            ) from None


class SpecialRule(BaseModel):
    type: Literal["conditional", "lookup", "function"]
    condition: Optional[str] = None
    true: Optional[Any] = None
    false: Optional[Any] = None
    call: Optional[str] = None
    method: Optional[str] = None
    args: List[str] = Field(default_factory=dict)
    items: List[str] = Field(default_factory=dict)


class Dialect(BaseModel):
    facets: Dict[str, str | list[str]] = Field(default_factory=dict)
    defaults: Dict[str, Any] = Field(default_factory=dict)
    path_specs: PathSpecs = Field(default_factory=PathSpecs)
    data_specs: DataSpecs = Field(default_factory=DataSpecs)
    special: Dict[str, SpecialRule] = Field(default_factory=dict)
    domains: Dict[str, List[float]] = Field(default_factory=dict)
    sources: List[MetadataSource] = Field(
        default_factory=lambda: [
            MetadataSource.path,
        ],
        description="Priority list of where to retrieve metadata",
    )
    inherits_from: Optional[str] = None

    @root_validator(pre=True)
    def validate_sources(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        # ensure only allowed sources are present
        srcs = values.get("sources", [])
        for s in srcs:
            if s not in MetadataSource:
                raise ValueError(f"Invalid metadata source: {s}")
        return values


class DRSConfig(BaseModel):
    datasets: Dict[str, Datasets]
    facets: Dict[str, str]
    suffixes: List[str] = Field(default_factory=list)
    storage_options: Dict[str, Any] = Field(default_factory=dict)
    defaults: Dict[str, Any] = Field(default_factory=dict)
    special: Dict[str, SpecialRule] = Field(default_factory=dict)
    dialect: Dict[str, Dialect]

    def model_post_init(self, __context: Any = None) -> None:
        self._defaults = {}
        self.suffixes = self.suffixes or [
            ".zarr",
            ".zar",
            ".nc4",
            ".nc",
            ".tar",
            ".hdf5",
            ".h5",
        ]
        for key, dset in self.datasets.items():
            self.dialect.setdefault(key, self.dialect[dset.drs_format])
            dset.backend.suffixes = self.suffixes
            for key, option in self.storage_options.items():
                dset.backend.storage_options.setdefault(key, option)
        for key, dset in self.datasets.items():
            self._defaults.setdefault(key, {})
            for k, _def in self.defaults:
                self._defaults[key].setdefault(k, _def)
            for k, _def in self.dialect[dset.drs_format].defaults.items():
                self._defaults[key].setdefault(k, _def)
            for k, _def in (dset.defaults or {}).items():
                self._defaults[key].setdefault(k, _def)

    @root_validator(pre=True)
    def _resolve_inheritance(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """
        After loading raw TOML into dicts, but before model instantiation,
        merge any dialects that declare `inherits_from`.
        """

        def _deep_merge(a: Dict[str, Any], b: Dict[str, Any]):
            for k, v in b.items():
                if k in a and isinstance(a[k], dict) and isinstance(v, dict):
                    if not v:
                        a[k] = {}
                    else:
                        _deep_merge(a[k], v)
                else:
                    a[k] = v

        raw = values.get("dialect", {})
        merged = deepcopy(raw)
        for name, cfg in raw.items():
            parent = cfg.get("inherits_from")
            if parent:
                if parent not in merged:
                    raise ValueError(
                        f"Dialect '{name}' inherits from unknown " f"'{parent}'"
                    )
                # take parent base, then overlay this dialect
                base = deepcopy(merged[parent])  # shallow copy of parent raw dict
                # remove inherits_from to avoid cycles
                child = deepcopy(cfg)
                child.pop("inherits_from", None)
                # deep-merge child into base
                _deep_merge(base, child)
                base["inherits_from"] = parent
                merged[name] = base

        values["dialect"] = merged
        return values

    @root_validator(pre=True)
    def _ensure_dialects(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """Ensure every dialect is a Dialect model."""
        raw = values.get("dialect", {})
        values["dialect"] = {k: v for k, v in raw.items()}
        return values

    def _apply_special_rules(self, standard: str, inp: Metadata) -> None:
        call = {
            "self": self,
            "data": inp.metadata,
            "standard": standard,
            "__file_name__": inp.path,
            "storage_backend": self.datasets[standard].backend,
        }
        specials = {
            f: s
            for f, s in self.special.items()
            if f not in self.dialect[standard].special
        }
        for facet, rule in {**self.dialect[standard].special, **specials}.items():
            result: Any = None
            match rule.type:
                case "conditional":
                    cond = eval(rule.condition, {}, call)
                    result = rule.true if cond else rule.false
                case "lookup":
                    args = [eval(arg, {}, call) for arg in rule.items]
                    return
                    result = self.datasets[standard].backend.lookup(
                        inp.path,
                        standard,
                        *args,
                        **self.dialect[standard].data_specs.read_kws,
                    )
                case "function":
                    call_str = textwrap.dedent(rule.method or rule.call).strip()
                    func = eval(call_str, {}, call)
                    args = [eval(arg, {}, call) for arg in rule.args]
                    if is_async_callable(func):
                        result = asyncio.run(func(*args))
                    else:
                        result = func(*args)
            if result:
                inp.metadata[facet] = result

    def _metadata_from_path(self, path: str, standard: str) -> Dict[str, Any]:
        """Extract the metadata from the path."""
        drs_type = self.datasets[standard].drs_format
        rel_path = (
            strip_protocol(path)
            .with_suffix("")
            .relative_to(self.datasets[standard].root_path)
        )
        return self.dialect[drs_type].path_specs.get_metadata_from_path(rel_path)

    @classmethod
    def load(cls, config_path: Path | str) -> DRSConfig:
        cfg = tomli.loads(ConfigMerger(config_path).dumps())
        settings = cfg.pop("drs_settings")
        try:
            return cls(datasets=cfg, **settings)
        except ValidationError as e:
            msgs = []
            for err in e.errors():
                loc = ".".join(str(x) for x in err["loc"])
                msgs.append(f"{loc}: {err['msg']}")
            raise ValueError(
                "DRSConfig validation failed:\n" + "\n".join(msgs)
            ) from None

    def max_directory_tree_level(
        self, search_dir: str | Path, drs_type: str
    ) -> int:
        """Get the maximum level for descending into directories.

        When searching for files in a directory we can only traverse
        the directory search tree until the version level is reached.
        This level is set as a hard threshold. If the drs type has no version
        we can indeed go all the way down to the file level.
        """
        search_dir = strip_protocol(search_dir)
        root_path = strip_protocol(self.datasets[drs_type].root_path)
        version = self.facets.get("version", "version")
        try:
            version_idx = self.dialect[drs_type].path_specs.dir_parts.index(
                version
            )
        except ValueError:
            # No version given
            version_idx = len(self.dialect[drs_type].path_specs.dir_parts)
        if root_path == search_dir:
            current_pos = 0
        else:
            current_pos = len(search_dir.relative_to(root_path).parts)
        return version_idx - current_pos

    def is_complete(self, data: Dict[str, Any], standard: str) -> bool:
        """Check if all metadata that can be collected was collected."""
        if not data:
            return False
        complete = True
        preset = {**self._defaults[standard], **self.dialect[standard].special}
        facets = (k for k, v in self.facets.items() if not v.startswith("__"))
        for facet in self.dialect[standard].facets or facets:
            if facet not in data and facet not in preset:
                complete = False
        return complete

    def _read_metadata(self, standard: str, inp: Metadata) -> Dict[str, Any]:
        """Get the metadata from a store."""
        drs_type = self.datasets[standard].drs_format
        for source in self.dialect[drs_type].sources:
            if self.is_complete(inp.metadata, standard) is True:
                break
            match source:
                case MetadataSource.path:
                    inp.metadata.update(
                        self._metadata_from_path(inp.path, standard)
                    )
                case MetadataSource.data:
                    with self.datasets[standard].backend.open_dataset(
                        inp.path, **self.dialect[standard].data_specs.read_kws
                    ) as ds:
                        inp.metadata.update(
                            self.dialect[standard].data_specs.extract_from_data(
                                ds
                            )
                        )
        for key, default in self._defaults[standard].items():
            inp.metadata.setdefault(key, default)
        self._apply_special_rules(standard, inp)
        return inp.metadata

    def read_metadata(self, standard: str, inp: Metadata) -> Dict[str, Any]:
        """Get the meta data for a given file path."""

        try:
            return self._read_metadata(standard, inp)
        except Exception as error:
            print(inp.path, inp.metadata)
            raise ValueError(error) from error

    async def translate(
        self, standard: str, data: Dict[str, Any], uri: str
    ) -> Dict[str, Any]:
        # 1) apply facets + defaults
        out: Dict[str, Any] = {}
        fac = self.facets
        defs = self.defaults
        dia = self.dialect[standard]
        for field, raw_key in fac.items():
            if raw_key == "__file_name__":
                val = uri
            else:
                key = dia.facets.get(field, raw_key)
                val = data.get(key, dia.defaults.get(field, defs.get(field)))
            out[field] = val

        # 2) apply global special
        for fld, rule in self.special.items():
            if rule.type == "conditional":
                cond = eval(rule.condition, {}, {"data": data})
                out[fld] = rule.true if cond else rule.false
            elif rule.type == "function":
                out[fld] = eval(rule.call, {"data": data, "self": self})
        # 3) apply dialect special
        for fld, rule in dia.special.items():
            if rule.type == "method":
                args = [
                    uri if a == "__file_name__" else data[a] for a in rule.args
                ]
                out[fld] = getattr(self, rule.method)(
                    *args
                )  # assume sync for brevity
            elif rule.type == "function":
                out[fld] = eval(rule.call, {"data": data, "self": self})
            elif rule.type == "conditional":
                cond = eval(rule.condition, {}, {"data": data})
                out[fld] = rule.true if cond else rule.false

        return out
