"""Config and Secrets store engine."""

import os
from contextlib import contextmanager
from copy import deepcopy
from importlib.resources import files
from pathlib import Path
import rtoml
from platformdirs import user_config_dir
from typing import Literal, Dict, Optional, Union, Iterator, cast

from ..api.config import ConfigMerger
from ..types import StoresConfig
from .store_config import ConfigFiles
from .vault import load_secrets

__all__ = ["ConfigFiles", "read_configfiles"]

TemplateName = Literal["connections", "secrets"]
_ENV_VARS: Dict[TemplateName, str] = {
    "connections": "MDC_CONFIG_PATH",
    "secrets": "MDC_SECRETS_PATH",
}
_MODES: Dict[TemplateName, int] = {"connections": 0o640, "secrets": 0o600}
LOCATION_KEYS = ("uri", "url", "path")


def default_path(name: TemplateName) -> Path:
    """The file to use: the environment variable, else the user config dir."""
    env = os.getenv(_ENV_VARS[name])
    if env:
        return Path(env).expanduser()
    return Path(user_config_dir("metadata-crawler")) / f"{name}.toml"


def init_template(
    name: TemplateName, target: Optional[Path] = None, force: bool = False
) -> Path:
    """Write a commented template to *target*; never overwrite unless *force*."""
    target = target or default_path(name)
    if target.exists() and not force:
        raise FileExistsError(f"{target} already exists, use force to replace it")
    target.parent.mkdir(parents=True, exist_ok=True, mode=0o750)
    template = (
        files("metadata_crawler").joinpath(f"connections/{name}.toml").read_bytes()
    )
    mode = _MODES[name]
    fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)
    with os.fdopen(fd, "wb") as stream:
        stream.write(template)
    os.chmod(target, mode)
    return target


def resolve_relative_locations(config: StoresConfig, base: Path) -> None:
    """Make local locations in config entries independent of the cwd."""
    for entry in config.values():
        if not isinstance(entry, dict):
            continue
        for key in LOCATION_KEYS:
            value = entry.get(key)
            if not isinstance(value, str) or "://" in value:
                continue
            expanded = os.path.expanduser(os.path.expandvars(value))
            if not os.path.isabs(expanded):
                entry[key] = str((base / expanded).resolve())


def merge_secrets(config: StoresConfig, secrets: StoresConfig) -> None:
    """Add credentials to the configured stores, in place.

    A store gets the secrets table named in its ``secrets`` key, otherwise the
    table with its own name. Secrets tables never become stores themselves.
    """
    for name, entry in config.items():
        if not isinstance(entry, dict):
            continue
        ref = entry.get("secrets")
        table_name = str(ref) if ref else name
        table = secrets.get(table_name)
        if table is None:
            if ref:
                raise ValueError(
                    f"store {name!r} refers to secrets table {table_name!r}, "
                    "which the secrets file doesn't have"
                )
            continue
        entry.update(table)


def _read(
    store_path: Optional[Union[str, Path]] = None,
    secrets_path: Optional[Union[str, Path]] = None,
) -> StoresConfig:
    """Read the connections and secrets; missing default files count as empty."""
    store_file = (
        Path(store_path).expanduser() if store_path else default_path("connections")
    )
    secrets_file = (
        Path(secrets_path).expanduser() if secrets_path else default_path("secrets")
    )
    # Explicitly requested files must exist; a missing default file is fine.
    for requested, path in ((store_path, store_file), (secrets_path, secrets_file)):
        if requested and not path.is_file():
            raise FileNotFoundError(f"no such config file: {path}")

    parsed_config: StoresConfig = {}
    if store_file.is_file():
        try:
            parsed_config = dict(rtoml.loads(store_file.read_text()))
        except (rtoml.TomlParsingError, rtoml.TomlSerializationError) as error:
            raise ValueError(f"{store_file}: {error}") from None
    resolve_relative_locations(parsed_config, store_file.parent)

    parsed_secrets = load_secrets(secrets_file) if secrets_file.is_file() else {}
    merge_secrets(parsed_config, parsed_secrets)
    return parsed_config


@contextmanager
def read_configfiles(
    store_path: Optional[Union[str, Path]] = None,
    secrets_path: Optional[Union[str, Path]] = None,
) -> Iterator[ConfigFiles]:
    """Create a context manager to load config and secrets once.

    Parameters
    ^^^^^^^^^^

    store_path:
        Path to the metadata crawler config file defining fixed settings.
    secrets_path:
        Path to the metadata crawler secrets file defining fixed secrets.

    Returns
    ^^^^^^^
    StoresConfig:
        Merged dictionary holding the configurations and secrets.

    Example
    ^^^^^^^

    .. code-block:: python

        import metadata_crawler as mdc
        with mdc.read_configfile(
          store_path="~/.config/metadata-crawler/stores.toml"
        ) as config:
           print(config.get("test-postgres"))
           print(config.get("test-solr"))
           mdc.add("~/data/drs-config.toml", store="test-postgres")
           mdc.index("solr", "/tmp/catalog-1.yml", server="test-solr")

    """
    config: Optional[ConfigFiles] = None
    try:
        config = ConfigFiles(_read(store_path, secrets_path))
        yield config
    finally:
        if config is not None:
            config.close()
