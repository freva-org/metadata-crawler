"""Load the secrets file, optionally encrypted with age."""

from __future__ import annotations

import base64
import getpass
import os
import stat
import sys
from pathlib import Path
from typing import Any, Dict, List

import rtoml

from ..types import StoresConfig

_ARMOR_HEADER = b"-----BEGIN AGE ENCRYPTED FILE-----"
_DEFAULT_IDENTITIES = ("~/.ssh/id_ed25519", "~/.ssh/id_rsa")


class SecretsError(Exception):
    """The secrets file can't be read or decrypted."""


def _check_permissions(path: Path) -> None:
    mode = path.stat().st_mode
    if mode & (stat.S_IRWXG | stat.S_IRWXO):
        raise SecretsError(
            f"{path} is accessible by other users, run: chmod 600 {path}"
        )


def _uses_passphrase(data: bytes) -> bool:
    """Passphrase-encrypted age files have a single scrypt stanza."""
    if data.lstrip().startswith(_ARMOR_HEADER):
        body = b"".join(
            line
            for line in data.strip().splitlines()[1:-1]
            if not line.startswith(b"-----")
        )
        data = base64.b64decode(body)
    return b"\n-> scrypt " in data.split(b"\n---", 1)[0]


def _load_identities() -> List[Any]:
    import pyrage

    configured = os.environ.get("MDC_AGE_IDENTITY")
    candidates = [configured] if configured else list(_DEFAULT_IDENTITIES)
    identities: List[Any] = []
    for candidate in candidates:
        path = Path(candidate).expanduser()
        if not path.is_file():
            if configured:
                raise SecretsError(f"MDC_AGE_IDENTITY: {path} does not exist")
            continue
        content = path.read_bytes()
        if b"AGE-SECRET-KEY-" in content:
            for line in content.decode().splitlines():
                if line.startswith("AGE-SECRET-KEY-"):
                    identities.append(pyrage.x25519.Identity.from_str(line.strip()))
            continue
        try:
            identities.append(pyrage.ssh.Identity.from_buffer(content))
        except pyrage.IdentityError as error:
            if configured:
                raise SecretsError(f"{path}: {error}") from None
    return identities


def _passphrase() -> str:
    value = os.environ.get("MDC_SECRETS_PASSPHRASE")
    if value:
        return value
    if sys.stdin.isatty():
        return getpass.getpass("Passphrase for the metadata-crawler secrets: ")
    raise SecretsError(
        "The secrets file is passphrase protected: set MDC_SECRETS_PASSPHRASE"
    )


def decrypt(data: bytes) -> bytes:
    """Try to decrypt a data store."""
    try:
        import pyrage
    except ImportError:
        raise SecretsError(
            'Reading encrypted secrets needs: pip install "metadata-crawler[vault]"'
        ) from None
    try:
        if _uses_passphrase(data):
            return bytes(pyrage.passphrase.decrypt(data, _passphrase()))
        identities = _load_identities()
        if not identities:
            raise SecretsError(
                "No identity to decrypt the secrets file, set MDC_AGE_IDENTITY"
            )
        return bytes(pyrage.decrypt(data, identities))
    except pyrage.DecryptError as error:
        raise SecretsError(f"Could not decrypt the secrets file: {error}") from None


def encrypted_path(secrets_file: Path) -> Path:
    """Get the age encrypted variant of *secrets_file*: ``secrets.toml.age``."""
    if secrets_file.suffix == ".age":
        return secrets_file
    return secrets_file.with_name(f"{secrets_file.name}.age")


def load_secrets(secrets_file: Path) -> StoresConfig:
    """Read *secrets_file*, preferring its encrypted ``.age`` variant.

    If both ``secrets.toml.age`` and ``secrets.toml`` exist the encrypted file
    wins: a plaintext file next to it is most likely a stale leftover from
    editing.
    """
    for path in encrypted_path(secrets_file), secrets_file:
        if path.is_file():
            break
    else:
        return {}
    _check_permissions(path)
    data = path.read_bytes()
    if path.suffix == ".age":
        data = decrypt(data)
    try:
        parsed: Dict[str, Any] = rtoml.loads(data.decode("utf-8"))
    except (rtoml.TomlParsingError, rtoml.TomlSerializationError) as error:
        raise SecretsError(f"{path}: {error}") from None
    return {name: table for name, table in parsed.items() if isinstance(table, dict)}
