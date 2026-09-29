"""Define types for the secrets and config engine."""

from datetime import date, time, datetime


from typing import Dict, TypeAlias, Union, List, Mapping, Sequence

TomlScalar: TypeAlias = Union[str, int, float, bool, datetime, date, time]
"""A single TOML value. TOML has no null, so there is no None."""

TomlValue: TypeAlias = Union[TomlScalar, List["TomlValue"], Dict[str, "TomlValue"]]
"""Any TOML value, nested arbitrarily deep."""

TomlTable: TypeAlias = Dict[str, TomlValue]
"""A TOML table, e.g. the settings of one store."""

StoresConfig: TypeAlias = Dict[str, TomlTable]
"""A parsed stores.toml or secrets.toml: store name -> table."""

ConfigValue: TypeAlias = Union[
    TomlScalar, Sequence["ConfigValue"], Mapping[str, "ConfigValue"]
]
"""Read-only counterpart of TomlValue, for function parameters."""
