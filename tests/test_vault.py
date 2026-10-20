"""Tests for reading plain and age-encrypted secrets files."""

from __future__ import annotations

import builtins
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pytest

from metadata_crawler.connections import vault
from metadata_crawler.connections.vault import (
    SecretsError,
    _check_permissions,
    _uses_passphrase,
    decrypt,
    load_secrets,
)

pyrage = pytest.importorskip("pyrage")
serialization = pytest.importorskip("cryptography.hazmat.primitives.serialization")
ed25519 = pytest.importorskip("cryptography.hazmat.primitives.asymmetric.ed25519")

PLAINTEXT = b'[prod]\nusername = "u"\npassword = "pw"\n'
EXPECTED: Dict[str, Any] = {"prod": {"username": "u", "password": "pw"}}


@pytest.fixture(autouse=True)
def no_ambient_keys(config_home: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Never pick up the developer's real ~/.ssh keys or passphrase."""


@pytest.fixture(scope="session")
def by_passphrase() -> bytes:
    """Passphrase ("pp") encryption is slow on purpose (scrypt): do it once."""
    return bytes(pyrage.passphrase.encrypt(PLAINTEXT, "pp"))


@pytest.fixture(scope="session")
def by_passphrase_armored() -> bytes:
    return bytes(pyrage.passphrase.encrypt(PLAINTEXT, "pp", armored=True))


def _ssh_key(
    tmp_path: Path, passphrase: bytes = b""
) -> Tuple[Path, Any]:  # (private key file, recipient)
    key = ed25519.Ed25519PrivateKey.generate()
    encryption = (
        serialization.BestAvailableEncryption(passphrase)
        if passphrase
        else serialization.NoEncryption()
    )
    private = key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.OpenSSH, encryption
    )
    public = key.public_key().public_bytes(
        serialization.Encoding.OpenSSH, serialization.PublicFormat.OpenSSH
    )
    path = tmp_path / ("id_protected" if passphrase else "id_ed25519")
    path.write_bytes(private)
    path.chmod(0o600)
    return path, pyrage.ssh.Recipient.from_str(public.decode())


def _write(path: Path, data: bytes, mode: int = 0o600) -> Path:
    path.write_bytes(data)
    path.chmod(mode)
    return path


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class TestPermissions:
    @pytest.mark.parametrize("mode", [0o600, 0o400, 0o700])
    def test_private_is_fine(self, tmp_path: Path, mode: int) -> None:
        _check_permissions(_write(tmp_path / "s.toml", b"", mode))

    @pytest.mark.parametrize("mode", [0o640, 0o604, 0o660, 0o644, 0o610])
    def test_readable_by_others(self, tmp_path: Path, mode: int) -> None:
        with pytest.raises(SecretsError, match="chmod 600"):
            _check_permissions(_write(tmp_path / "s.toml", b"", mode))


class TestUsesPassphrase:
    def test_passphrase(self, by_passphrase: bytes) -> None:
        assert _uses_passphrase(by_passphrase)

    def test_passphrase_armored(self, by_passphrase_armored: bytes) -> None:
        assert by_passphrase_armored.startswith(b"-----BEGIN AGE ENCRYPTED FILE-----")
        assert _uses_passphrase(by_passphrase_armored)

    def test_identity(self) -> None:
        recipient = pyrage.x25519.Identity.generate().to_public()
        assert not _uses_passphrase(pyrage.encrypt(b"x", [recipient]))
        assert not _uses_passphrase(pyrage.encrypt(b"x", [recipient], armored=True))


# ---------------------------------------------------------------------------
# decrypt
# ---------------------------------------------------------------------------


class TestDecryptPassphrase:
    def test_from_environment(
        self, monkeypatch: pytest.MonkeyPatch, by_passphrase: bytes
    ) -> None:
        monkeypatch.setenv("MDC_SECRETS_PASSPHRASE", "pp")
        assert decrypt(by_passphrase) == PLAINTEXT

    def test_armored(
        self, monkeypatch: pytest.MonkeyPatch, by_passphrase_armored: bytes
    ) -> None:
        monkeypatch.setenv("MDC_SECRETS_PASSPHRASE", "pp")
        assert decrypt(by_passphrase_armored) == PLAINTEXT

    def test_prompt_with_a_terminal(
        self, monkeypatch: pytest.MonkeyPatch, by_passphrase: bytes
    ) -> None:
        prompts: List[str] = []

        def getpass(prompt: str) -> str:
            prompts.append(prompt)
            return "pp"

        monkeypatch.setattr(sys.stdin, "isatty", lambda: True, raising=False)
        monkeypatch.setattr(vault.getpass, "getpass", getpass)
        assert decrypt(by_passphrase) == PLAINTEXT
        assert len(prompts) == 1

    def test_no_passphrase_without_a_terminal(
        self, monkeypatch: pytest.MonkeyPatch, by_passphrase: bytes
    ) -> None:
        monkeypatch.setattr(sys.stdin, "isatty", lambda: False, raising=False)
        with pytest.raises(SecretsError, match="MDC_SECRETS_PASSPHRASE"):
            decrypt(by_passphrase)

    def test_wrong_passphrase(
        self, monkeypatch: pytest.MonkeyPatch, by_passphrase: bytes
    ) -> None:
        monkeypatch.setenv("MDC_SECRETS_PASSPHRASE", "wrong")
        with pytest.raises(SecretsError, match="Could not decrypt"):
            decrypt(by_passphrase)


class TestDecryptIdentity:
    def test_age_key_file(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        identity = pyrage.x25519.Identity.generate()
        key = _write(tmp_path / "key.txt", f"# created: now\n{identity}\n".encode())
        monkeypatch.setenv("MDC_AGE_IDENTITY", str(key))
        data = pyrage.encrypt(PLAINTEXT, [identity.to_public()])
        assert decrypt(data) == PLAINTEXT

    def test_ssh_key(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        key, recipient = _ssh_key(tmp_path)
        monkeypatch.setenv("MDC_AGE_IDENTITY", str(key))
        assert decrypt(pyrage.encrypt(PLAINTEXT, [recipient])) == PLAINTEXT

    def test_ssh_key_armored(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        key, recipient = _ssh_key(tmp_path)
        monkeypatch.setenv("MDC_AGE_IDENTITY", str(key))
        data = pyrage.encrypt(PLAINTEXT, [recipient], armored=True)
        assert decrypt(data) == PLAINTEXT

    def test_default_ssh_key(self, config_home: Path) -> None:
        ssh_dir = Path.home() / ".ssh"
        ssh_dir.mkdir()
        key, recipient = _ssh_key(ssh_dir)
        assert key == ssh_dir / "id_ed25519"
        assert decrypt(pyrage.encrypt(PLAINTEXT, [recipient])) == PLAINTEXT

    def test_no_identity(self, config_home: Path) -> None:
        recipient = pyrage.x25519.Identity.generate().to_public()
        with pytest.raises(SecretsError, match="MDC_AGE_IDENTITY"):
            decrypt(pyrage.encrypt(PLAINTEXT, [recipient]))

    def test_configured_identity_missing(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("MDC_AGE_IDENTITY", str(tmp_path / "nope"))
        recipient = pyrage.x25519.Identity.generate().to_public()
        with pytest.raises(SecretsError, match="does not exist"):
            decrypt(pyrage.encrypt(PLAINTEXT, [recipient]))

    def test_configured_protected_ssh_key(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        pytest.importorskip("bcrypt")  # needed to write a protected OpenSSH key
        key, recipient = _ssh_key(tmp_path, passphrase=b"keypass")
        monkeypatch.setenv("MDC_AGE_IDENTITY", str(key))
        with pytest.raises(SecretsError, match=str(key)):
            decrypt(pyrage.encrypt(PLAINTEXT, [recipient]))

    def test_unusable_default_key_is_skipped(self, config_home: Path) -> None:
        """A protected ~/.ssh/id_ed25519 must not hide a usable id_rsa."""
        ssh_dir = Path.home() / ".ssh"
        ssh_dir.mkdir()
        _write(ssh_dir / "id_ed25519", b"not a key")
        recipient = pyrage.x25519.Identity.generate().to_public()
        with pytest.raises(SecretsError, match="No identity"):
            decrypt(pyrage.encrypt(PLAINTEXT, [recipient]))

    def test_wrong_identity(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        key, _ = _ssh_key(tmp_path)
        monkeypatch.setenv("MDC_AGE_IDENTITY", str(key))
        other = pyrage.x25519.Identity.generate().to_public()
        with pytest.raises(SecretsError, match="Could not decrypt"):
            decrypt(pyrage.encrypt(PLAINTEXT, [other]))


def test_decrypt_without_pyrage(monkeypatch: pytest.MonkeyPatch) -> None:
    real_import = builtins.__import__

    def fake_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "pyrage":
            raise ImportError(name)
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    with pytest.raises(SecretsError, match=r"metadata-crawler\[vault\]"):
        decrypt(b"age-encryption.org/v1\n")


# ---------------------------------------------------------------------------
# load_secrets
# ---------------------------------------------------------------------------


class TestLoadSecrets:
    def test_plaintext(self, tmp_path: Path) -> None:
        assert load_secrets(_write(tmp_path / "s.toml", PLAINTEXT)) == EXPECTED

    def test_encrypted_next_to_the_requested_file(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, by_passphrase: bytes
    ) -> None:
        monkeypatch.setenv("MDC_SECRETS_PASSPHRASE", "pp")
        _write(tmp_path / "s.toml.age", by_passphrase)
        assert load_secrets(tmp_path / "s.toml") == EXPECTED

    def test_encrypted_file_directly(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, by_passphrase: bytes
    ) -> None:
        monkeypatch.setenv("MDC_SECRETS_PASSPHRASE", "pp")
        age = _write(tmp_path / "s.toml.age", by_passphrase)
        assert load_secrets(age) == EXPECTED

    def test_encrypted_file_is_preferred(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, by_passphrase: bytes
    ) -> None:
        """As documented in the template: the .age file wins if both exist."""
        monkeypatch.setenv("MDC_SECRETS_PASSPHRASE", "pp")
        _write(tmp_path / "s.toml", b'[prod]\npassword = "stale plaintext"\n')
        _write(tmp_path / "s.toml.age", by_passphrase)
        assert load_secrets(tmp_path / "s.toml") == EXPECTED

    def test_missing(self, tmp_path: Path) -> None:
        assert load_secrets(tmp_path / "s.toml") == {}

    def test_permissions_are_checked_for_encrypted_files(
        self, tmp_path: Path, by_passphrase: bytes
    ) -> None:
        age = _write(tmp_path / "s.toml.age", by_passphrase, 0o644)
        with pytest.raises(SecretsError, match="chmod 600"):
            load_secrets(age)

    def test_invalid_toml(self, tmp_path: Path) -> None:
        with pytest.raises(SecretsError, match="s.toml"):
            load_secrets(_write(tmp_path / "s.toml", b"[broken\n"))

    def test_top_level_values_are_ignored(self, tmp_path: Path) -> None:
        data = b'version = 1\n[prod]\npassword = "pw"\n'
        assert load_secrets(_write(tmp_path / "s.toml", data)) == {
            "prod": {"password": "pw"}
        }
