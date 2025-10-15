"""API for loading crawler configuration."""

from __future__ import annotations

import os
import re
import textwrap
from copy import deepcopy
from datetime import datetime
from enum import Enum, StrEnum
from fnmatch import fnmatch
from pathlib import Path
from typing import (
    Annotated,
    Any,
    Dict,
    List,
    Literal,
    Optional,
    Tuple,
    Union,
    cast,
)
from urllib.parse import urlsplit
from warnings import catch_warnings

import tomli
import tomlkit
import xarray
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)
from tomlkit.container import OutOfOrderTableProxy
from tomlkit.items import Table

from ..utils import (
    MetadataCrawlerException,
    convert_str_to_timestamp,
    load_plugins,
)
from .mixin import TemplateMixin
from .storage_backend import Metadata, MetadataType


class BaseType(str, Enum):
    """Basic types."""

    string = "string"
    integer = "integer"
    float = "float"
    timestamp = "timestamp"


class Types(str, Enum):
    """Types supported by the config."""

    string = "string"
    integer = "integer"
    float = "float"
    timestamp = "timestamp"
    daterange = "timestamp[2]"
    path = "string"
    uri = "string"
    dataset = "string"
    fmt = "string"
    storage = "string"
    bbox = "float[4]"


class SchemaField(BaseModel):
    """BaseModel defining the metadata schema."""

    key: str
    type: str
    required: bool = False
    default: Optional[Any] = None
    length: Optional[int] = None
    base_type: BaseType = BaseType.string
    multi_valued: bool = True
    indexed: bool = True
    name: Optional[str] = None
    unique: bool = False

    @field_validator("type")
    @classmethod
    def parse_type(cls, v: str) -> str:
        """Parse the data types.

        Accepts
        ^^^^^^^
          - 'string', 'integer', 'float', 'timestamp' -> length=None
          - 'float[2]', 'int[5]' -> length=number
          - 'string[]'  -> length=None, multi_valued semantics
          - 'daterange' [timestamp, timestamp]
        """
        f_type = getattr(getattr(Types, v, None), "value", v)
        m = re.fullmatch(r"({})(\[(\d*)\])?".format("|".join(BaseType)), f_type)
        if not m:
            raise MetadataCrawlerException(f"invalid type spec {v!r}")
        base, _, num = m.groups()
        setattr(
            cls,
            "__parsed_type",
            {"base": base, "length": int(num) if num else None},
        )
        return v

    @model_validator(mode="after")
    def _set_parsed(self) -> "SchemaField":
        parsed = getattr(self, "__parsed_type")
        self.base_type = BaseType(parsed["base"])
        self.length = parsed["length"]
        self.name = self.name or self.key
        return self

    @staticmethod
    def get_time_range(
        time_stamp: Optional[Union[str, List[str]]],
    ) -> List[datetime]:
        """Convert a from to time range to a begin or end time step."""
        time_stamp = time_stamp or ""
        if isinstance(time_stamp, str):
            start_str, _, end_str = (
                time_stamp.replace(":", "").replace("_", "-").partition("-")
            )
            time_stamp = [start_str or "fx", end_str or "fx"]
        for n, ts in enumerate(time_stamp):
            if hasattr(ts, "isoformat"):
                time_stamp[n] = ts.isoformat()
            time_stamp[n] = str(ts) or "fx"
        start = convert_str_to_timestamp(
            time_stamp[0], alternative="0001-01-01T00:00"
        )
        end = convert_str_to_timestamp(
            time_stamp[-1], alternative="9999-12-31T23:59"
        )
        return [start, end]


class MetadataSource(StrEnum):
    """Representation of how the metadata should be retrieved."""

    storage = "storage"  # via intake/fdb5/etc.
    path = "path"  # parse via specs_dir/specs_file
    data = "data"  # read attributes from the file itself


class VarAttrRule(BaseModel):
    """How to read attributes from variables."""

    var: str
    attr: str  # attribute name on DataArray.attrs
    default: Any = None


class StatRule(BaseModel):
    """How to apply statistics."""

    stat: Literal["min", "max", "minmax", "range", "bbox"]
    var: Optional[str] = None  # for numeric stats on a single var
    coords: Optional[Union[List[str], str]] = None  # for time range, etc.
    lat: Optional[str] = None  # convenience keys for bbox
    lon: Optional[str] = None
    default: Any = None


class ConfigMerger:
    """Load the system and user TOML, merges user -> system under.

    Merging the config preserves comments/formatting, and lets you inspect or
    write the result.
    """

    def __init__(
        self,
        user_path: Optional[
            Union[Path, str, Dict[str, Any], tomlkit.TOMLDocument]
        ] = None,
    ):
        # parse both documents
        system_path = Path(__file__).parent / "drs_config.toml"
        self._system_doc = tomlkit.parse(system_path.read_text(encoding="utf-8"))
        _config: str = ""
        if user_path is not None:
            if isinstance(user_path, (str, Path)) and os.path.isdir(user_path):
                _config = (
                    Path(user_path).expanduser().absolute() / "drs_config.toml"
                ).read_text(encoding="utf-8")
            elif isinstance(user_path, (str, Path)) and os.path.isfile(user_path):
                _config = (
                    Path(user_path)
                    .expanduser()
                    .absolute()
                    .read_text(encoding="utf-8")
                )
            elif isinstance(user_path, (str, Path)):
                _config = str(user_path)
            else:
                _config = tomlkit.dumps(user_path)
        if _config:
            try:
                self._user_doc = tomlkit.parse(_config)
            except Exception:
                raise MetadataCrawlerException(
                    "Could not load config path."
                ) from None
            self._merge_tables(self._system_doc, self._user_doc)

    def _merge_tables(
        self,
        base: Union[tomlkit.TOMLDocument, Table, OutOfOrderTableProxy],
        override: Union[Table, tomlkit.TOMLDocument, OutOfOrderTableProxy],
    ) -> None:

        for key, value in override.items():
            if key not in base:
                base[key] = value
                continue
            if isinstance(value, (Table, OutOfOrderTableProxy)):
                self._merge_tables(cast(Table, base[key]), value)
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
    search_path: Union[str, Path]

    def model_post_init(self, __context: Any = None) -> None:
        """Apply rules after init."""
        self.search_path = str(self.search_path)


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
            raise MetadataCrawlerException(
                (
                    f"Number of dir parts for {rel_path.parent} do not match "
                    f"- needs: {len(self.dir_parts)} has: {len(dir_parts)}"
                )
            ) from None
        if len(file_parts) == len(self.file_parts):
            _parts = dict(zip(self.file_parts, file_parts))
        elif (
            len(file_parts) == len(self.file_parts) - 1 and "fx" in rel_path.name
        ):
            _parts = dict(zip(self.file_parts[:-1], file_parts))
        else:
            raise MetadataCrawlerException(
                (
                    f"Number of file parts for {rel_path.name} do not match "
                    f"- needs: {len(self.file_parts)} has: {len(file_parts)})"
                )
            )
        data.update({k: v for (k, v) in _parts.items() if k not in data})
        data.pop("_", None)
        return data


class DataSpecs(BaseModel):
    """BaseModel for the configuration."""

    globals: Dict[str, str] = Field(default_factory=dict)
    var_attrs: Dict[str, VarAttrRule] = Field(default_factory=dict)
    stats: Dict[str, StatRule] = Field(default_factory=dict)
    read_kws: Dict[str, Any] = Field(default_factory=dict)

    def _set_global_attributes(
        self, dset: "xarray.Dataset", out: Dict[str, Any]
    ) -> None:

        for facet, attr in self.globals.items():
            if attr == "__variable__":
                out[facet] = list(getattr(dset, "data_vara", dset.variables))
            else:
                out[facet] = dset.attrs.get(attr)

    def _set_variable_attributes(
        self, dset: "xarray.Dataset", out: Dict[str, Any]
    ) -> None:
        data_vars = list(getattr(dset, "data_vars", dset.variables))

        def get_val(
            rule: VarAttrRule, vnames: Union[str, List[str]]
        ) -> List[Any]:
            if isinstance(vnames, str):
                vnames = [dv for dv in data_vars if fnmatch(dv, vnames)]
            attr_list: List[Any] = []
            for vname in vnames:
                default = (rule.default or "").replace("__name__", vname) or vname
                attr = (rule.attr or "").replace("__name__", vname) or vname
                if vname in dset:
                    attr_list.append(dset[vname].attrs.get(attr, default))
                else:
                    attr_list.append(default)
            return attr_list

        for facet, rule in self.var_attrs.items():
            resolved: Union[str, List[str]] = rule.var
            vals = get_val(rule, resolved)
            if len(vals) == 1:
                out[facet] = vals[0]
            else:
                out[facet] = vals

    def _apply_stats_rules(
        self, dset: "xarray.Dataset", out: Dict[str, Any]
    ) -> None:

        for facet, rule in self.stats.items():
            coords: Optional[List[str]] = None
            if rule.coords:
                coords = (
                    rule.coords
                    if isinstance(rule.coords, list)
                    else [rule.coords]
                )
            match rule.stat:
                case "bbox":
                    lat = rule.lat or (coords[0] if coords else "lat")
                    lon = rule.lon or (
                        coords[1] if coords and len(coords) > 1 else "lon"
                    )
                    out[facet] = rule.default
                    if lat in dset and lon in dset:
                        latv = dset[lat].values
                        lonv = dset[lon].values
                        out[facet] = [
                            float(lonv.min()),
                            float(lonv.max()),
                            float(latv.min()),
                            float(latv.max()),
                        ]

                case "range":
                    coord = coords[0] if coords else None
                    out[facet] = rule.default
                    if coord and coord in dset.coords:
                        arr = dset.coords[coord].values
                        out[facet] = [arr.min(), arr.max()]

                case "min" | "max" | "minmax":

                    coord = coords[0] if coords else None
                    var_name = rule.var if rule.var else coord
                    out[facet] = rule.default
                    if var_name and var_name in dset:
                        arr = dset[var_name].values
                        if rule.stat == "min":
                            out[facet] = arr.min()
                        elif rule.stat == "max":
                            out[facet] = arr.max()
                        else:
                            out[facet] = [arr.min(), arr.max()]

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
    drs_format: str = "freva"
    fs_type: str = "posix"
    defaults: Dict[str, Any] = Field(default_factory=dict)
    storage_options: Dict[str, Any] = Field(default_factory=dict)
    glob_pattern: str = "*.*"
    inherits_from: str = Field(default_factory=str)

    @field_validator("storage_options", mode="after")
    @classmethod
    def _render_storage_options(
        cls, storage_options: Dict[str, Any]
    ) -> Dict[str, Any]:
        tmpl = TemplateMixin()
        return cast(Dict[str, Any], tmpl.render_templates(storage_options, {}))

    def model_post_init(self, __context: Any = None) -> None:
        """Apply rules after init."""
        storage_plugins = load_plugins("metadata_crawler.storage")
        try:
            self.backend = storage_plugins[self.fs_type](**self.storage_options)
        except KeyError:
            raise NotImplementedError(
                f"Backend not available. `{self.fs_type}` extension missing?"
            ) from None


class ConditionalRule(BaseModel):
    """Define conditional rules."""

    type: Literal["conditional"] = "conditional"
    condition: str
    true: Any
    false: Any


class CallRule(BaseModel):
    """Define caller rules."""

    type: Literal["call"] = "call"
    call: str


class LookupRule(BaseModel):
    """Define lookup table rules."""

    type: Literal["lookup"] = "lookup"
    tree: List[str] = Field(default_factory=list)
    attribute: str
    standard: Optional[str] = None


SpecialRule = Annotated[
    Union[ConditionalRule, CallRule, LookupRule], Field(discriminator="type")
]


class Dialect(BaseModel):
    """Settings for a DRS Format."""

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

    @field_validator("sources", mode="after")
    @classmethod
    def _validate_sources(cls, srcs: List[str]) -> List[str]:
        # ensure only allowed sources are present
        names = {name.upper() for name in MetadataSource.__members__.keys()}
        values = {m.value for m in MetadataSource}
        invalid = [s for s in srcs if s.upper() not in names and s not in values]
        if invalid:
            allowed = sorted(values | {n.lower() for n in names})
            raise MetadataCrawlerException(
                f"Invalid metadata source(s): {invalid!r}. Allowed: {allowed}"
            )
        return srcs


class DRSConfig(BaseModel, TemplateMixin):
    """BaseModel model for the entire user config."""

    datasets: Dict[str, Datasets]
    index_schema: Dict[str, SchemaField] = Field(...)
    suffixes: List[str] = Field(default_factory=list)
    storage_options: Dict[str, Any] = Field(default_factory=dict)
    defaults: Dict[str, Any] = Field(default_factory=dict)
    special: Dict[str, SpecialRule] = Field(default_factory=dict)
    dialect: Dict[str, Dialect]

    def model_post_init(self, __context: Any = None) -> None:
        """Apply special rules after init."""
        self._defaults: Dict[str, Any] = {}
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
            for k, _def in (dset.defaults or {}).items():
                self._defaults[key].setdefault(k, _def)
            for k, _def in self.dialect[dset.drs_format].defaults.items():
                self._defaults[key].setdefault(k, _def)
            for k, _def in self.defaults.items():
                self._defaults[key].setdefault(k, _def)
        self.prep_template_env()
        for standard in self.dialect:
            for key in self.special:
                self.dialect[standard].special.setdefault(key, self.special[key])

    @model_validator(mode="before")
    @classmethod
    def _dump_(cls, values: Any) -> Any:
        setattr(cls, "_model_dict", values)
        return values

    @model_validator(mode="before")
    def _resolve_inheritance(cls, values: Any) -> Any:
        """Apply inheritance.

        After loading raw TOML into dicts, but before model instantiation, merge
        any dialects that declare `inherits_from`.
        """
        if not isinstance(values, dict):
            return values  # pragma: no cover

        def _deep_merge(a: Dict[str, Any], b: Dict[str, Any]) -> None:
            for k, v in b.items():
                if k in a and isinstance(a[k], dict) and isinstance(v, dict):
                    if not v:
                        a[k] = {}
                    else:
                        _deep_merge(a[k], v)
                else:
                    a[k] = v

        for key in ("dialect", "datasets"):
            raw = values.get(key, {})
            merged = deepcopy(raw)
            for name, cfg in raw.items():
                parent = cfg.get("inherits_from")
                if parent:
                    if parent not in merged:
                        raise MetadataCrawlerException(
                            f"'{name}' inherits from unknown " f"'{parent}'"
                        )
                    # take parent base, then overlay this dialect
                    base = deepcopy(
                        merged[parent]
                    )  # shallow copy of parent raw dict
                    # remove inherits_from to avoid cycles
                    child = deepcopy(cfg)
                    child.pop("inherits_from", None)
                    # deep-merge child into base
                    _deep_merge(base, child)
                    base["inherits_from"] = parent
                    merged[name] = base

            values[key] = merged
        return values

    @model_validator(mode="before")
    def _ensure_dialects(cls, values: Any) -> Any:
        """Ensure every dialect is a Dialect model."""
        if not isinstance(values, dict):
            return values  # pragma: no cover

        raw = values.get("dialect", {})
        values["dialect"] = {k: v for k, v in raw.items()}
        return values

    def _apply_special_rules(
        self,
        standard: str,
        drs_type: str,
        inp: Metadata,
        specials: Dict[str, SpecialRule],
    ) -> None:
        data = {**inp.metadata, **{"file": inp.path, "uri": inp.path}}

        for facet, rule in specials.items():
            result: Any = None
            if inp.metadata.get(facet):
                continue
            match rule.type:
                case "conditional":
                    _rule = textwrap.dedent(rule.condition or "").strip()
                    s_cond = self.render_templates(_rule, data)
                    cond = eval(
                        s_cond, {}, getattr(self, "_model_dict", {})
                    )  # nosec
                    result = rule.true if cond else rule.false
                case "lookup":
                    args = cast(List[str], self.render_templates(rule.tree, data))

                    result = self.datasets[standard].backend.lookup(
                        inp.path,
                        self.render_templates(rule.attribute, data),
                        rule.standard or drs_type,
                        *args,
                        **self.dialect[standard].data_specs.read_kws,
                    )
                case "call":
                    _call = textwrap.dedent(rule.call or "").strip()
                    result = eval(
                        self.render_templates(_call, data),
                        {},
                        getattr(self, "_model_dict", {}),
                    )  # nosec
            if result:
                inp.metadata[facet] = result

    def _metadata_from_path(self, path: str, standard: str) -> Dict[str, Any]:
        """Extract the metadata from the path."""
        drs_type = self.datasets[standard].drs_format
        root_path = strip_protocol(
            self.datasets[standard].backend.path(
                self.datasets[standard].root_path
            )
        )
        _path = strip_protocol(self.datasets[standard].backend.path(path))
        rel_path = _path.with_suffix("").relative_to(root_path)
        return self.dialect[drs_type].path_specs.get_metadata_from_path(rel_path)

    @classmethod
    def load(
        cls,
        config_path: Optional[
            Union[Path, str, Dict[str, Any], tomlkit.TOMLDocument]
        ] = None,
    ) -> DRSConfig:
        """Load a drs config from file."""
        cfg = tomli.loads(ConfigMerger(config_path).dumps())
        settings = cfg.pop("drs_settings")
        try:
            return cls(datasets=cfg, **settings)
        except ValidationError as e:
            msgs = []
            for err in e.errors():
                loc = ".".join(str(x) for x in err["loc"])
                msgs.append(f"{loc}: {err['msg']}")
            raise MetadataCrawlerException(
                "DRSConfig validation failed:\n" + "\n".join(msgs)
            ) from None

    def max_directory_tree_level(
        self, search_dir: str | Path, drs_type: str
    ) -> Tuple[int, bool]:
        """Get the maximum level for descending into directories.

        When searching for files in a directory we can only traverse the directory
        search tree until the version level is reached. This level is set as a hard
        threshold. If the drs type has no version we can indeed go all the way down
        to the file level.
        """
        root_path = strip_protocol(
            self.datasets[drs_type].backend.path(
                self.datasets[drs_type].root_path
            )
        )
        search_dir = strip_protocol(
            self.datasets[drs_type].backend.path(search_dir)
        )
        standard = self.datasets[drs_type].drs_format
        version = cast(
            str, self.dialect[standard].facets.get("version", "version")
        )
        is_versioned = True
        try:
            version_idx = self.dialect[standard].path_specs.dir_parts.index(
                version
            )
        except ValueError:
            # No version given
            version_idx = len(self.dialect[standard].path_specs.dir_parts)
            is_versioned = False
        if root_path == search_dir:
            current_pos = 0
        else:
            current_pos = len(search_dir.relative_to(root_path).parts)
        return version_idx - current_pos, is_versioned

    def is_complete(self, data: Dict[str, Any], standard: str) -> bool:
        """Check if all metadata that can be collected was collected."""
        if not data:
            return False
        complete = True
        preset = {**self._defaults[standard], **self.dialect[standard].special}
        facets = (
            k for k, v in self.index_schema.items() if not v.key.startswith("__")
        )
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
                    with catch_warnings(action="ignore", category=RuntimeWarning):
                        with self.datasets[standard].backend.open_dataset(
                            inp.path, **self.dialect[standard].data_specs.read_kws
                        ) as ds:
                            inp.metadata.update(
                                self.dialect[
                                    standard
                                ].data_specs.extract_from_data(ds)
                            )
        self._apply_special_rules(
            standard, drs_type, inp, self.dialect[standard].special
        )
        return self._translate(standard, inp)

    def read_metadata(self, standard: str, inp: MetadataType) -> Dict[str, Any]:
        """Get the meta data for a given file path."""
        return self._read_metadata(
            standard,
            Metadata(path=inp["path"], metadata=inp["metadata"].copy()),
        )

    def _translate(self, standard: str, inp: Metadata) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        # locals to cut attribute lookups
        defs = self._defaults[standard]
        dia = self.dialect[standard]
        facets_get = dia.facets.get
        backend = self.datasets[standard].backend
        mget = inp.metadata.get
        defs_get = defs.get
        path = inp.path
        fmt = path.rsplit(".", 1)[1] if "." in path else ""

        precomputed: Dict[str, Any] = {
            "path": backend.path(path),
            "uri": backend.uri(path),
            "storage": backend.fs_type(path),
            "dataset": standard,
            "fmt": fmt,
        }
        val: Any = ""
        out_set = out.__setitem__
        for field, schema in self.index_schema.items():
            if schema.indexed is False:
                continue

            stype = schema.type

            # Fast path for simple, precomputed types
            if stype in precomputed and stype != "daterange":
                val = precomputed[stype]

            elif stype == "daterange":
                src = mget(field) or defs_get(field)
                val = schema.get_time_range(src)

            else:
                # Resolve metadata key via facets once; default to field name
                key = cast(str, facets_get(schema.key, field))
                val = mget(key) or defs_get(key)

            # Preserve your current semantics: fall back to schema.default on falsey
            val = val or schema.default

            # Multi-valued normalization
            if (
                (schema.multi_valued or schema.length)
                and val
                and not isinstance(val, list)
            ):
                val = [val]

            out_set(field, val)
        return out
