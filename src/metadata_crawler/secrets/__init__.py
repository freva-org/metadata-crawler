"""Config and Secrets store engine."""

import os
from typing import Literal
from importlib.resources import files
from pathlib import Path
from typing import Optional

import rtoml
from platformdirs import user_config_dir

from ..types import StoresConfig
from .vault import load_secrets
from ..api.config import ConfigMerger


def init_template(store_type: Literal["secrets", "stores"] = "stores") -> Path:
    """Create the secrets file from the template, readable only by the user."""
    target = Path(user_config_dir("metadata-crawler")) / f"{store_type}.toml"
    if target.is_file():
        return target
    target.parent.mkdir(parents=True, exist_ok=True, mode=0o750)
    template = (
        files("metadata_crawler").joinpath(f"secrets/{store_type}.toml").read_bytes()
    )
    mod = {"stores": 0o640, "secrets": 0o600}[store_type]
    fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mod)
    with os.fdopen(fd, "wb") as stream:
        stream.write(template)
    os.chmod(target, mod)
    return target


def read(
    store_path: Optional[Path] = None, secrets_path: Optional[Path] = None
) -> StoresConfig:
    """Read the metadata config and secert files."""
    store_path = store_path or Path(
        os.getenv("MCD_CONFIG_PATH", init_template("stores"))
    )
    secrets_path = secrets_path or Path(
        os.getenv("MCD_CONFIG_PATH", init_template("secrets"))
    )
    try:
        parsed_config: StoresConfig = rtoml.loads(store_path.read_text())
    except (rtoml.TomlParsingError, rtoml.TomlSerializationError) as error:
        raise ValueError(f"{store_path}: {error}") from None
    parsed_secrets = load_secrets(secrets_path)
    ConfigMerger.merge_tables(parsed_config, parsed_secrets)
    return dict(parsed_config)
