import fsspec
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

from metadata_crawler.api.stores import IndexStore  # adjust import
from metadata_crawler import add


class TestBackendSelection:
    """ "Tests for the BackendEnum."""

    def test_wrong_backend_type(
        self, drs_config_path: Path, cat_file: Path, data_dir: Path
    ) -> None:
        """Test the right behaviour for selecting the wrong backend."""
        with pytest.raises(ValueError):
            add(
                drs_config_path,
                store=cat_file,
                backend="foo",
                data_object=[data_dir / "observations"],
            )


class TestGetFs:
    """Tests for IndexStore.get_fs."""

    # ---- protocol detection & is_local flag -------------------------------

    def test_local_path_returns_local_fs(self) -> None:
        fs, is_local = IndexStore.get_fs("/tmp/some/path.yml")
        assert isinstance(fs, fsspec.implementations.local.LocalFileSystem)
        assert is_local is True

    def test_relative_path_returns_local_fs(self) -> None:
        fs, is_local = IndexStore.get_fs("some/relative/path.yml")
        assert isinstance(fs, fsspec.implementations.local.LocalFileSystem)
        assert is_local is True

    def test_file_protocol_returns_local_fs(self) -> None:
        fs, is_local = IndexStore.get_fs("file:///tmp/path.yml")
        assert isinstance(fs, fsspec.implementations.local.LocalFileSystem)
        assert is_local is True

    def test_s3_returns_not_local(self) -> None:
        fs, is_local = IndexStore.get_fs("s3://bucket/key.yml")
        assert is_local is False

    def test_https_returns_not_local(self) -> None:
        fs, is_local = IndexStore.get_fs("https://example.com/data.yml")
        assert is_local is False

    def test_http_returns_not_local(self) -> None:
        fs, is_local = IndexStore.get_fs("http://example.com/data.yml")
        assert is_local is False

    # ---- S3 anon behavior -------------------------------------------------

    def test_s3_no_creds_sets_anon_true(self) -> None:
        with patch("fsspec.filesystem") as mock_fs:
            mock_fs.return_value = MagicMock()
            IndexStore.get_fs("s3://bucket/key.yml")
            mock_fs.assert_called_once_with("s3", anon=True)

    def test_s3_with_key_no_anon(self) -> None:
        with patch("fsspec.filesystem") as mock_fs:
            mock_fs.return_value = MagicMock()
            IndexStore.get_fs(
                "s3://bucket/key.yml",
                key="K",
                secret="S",
                endpoint_url="https://s3.example.com",
            )
            _, kwargs = mock_fs.call_args
            assert "anon" not in kwargs
            assert kwargs["key"] == "K"
            assert kwargs["secret"] == "S"

    def test_s3_explicit_anon_false_respected(self) -> None:
        with patch("fsspec.filesystem") as mock_fs:
            mock_fs.return_value = MagicMock()
            IndexStore.get_fs("s3://bucket/key.yml", anon=False)
            _, kwargs = mock_fs.call_args
            assert kwargs["anon"] is False

    def test_s3_explicit_anon_true_with_key_passes_both(self) -> None:
        """User explicitly sets anon=True alongside creds — forwarded as-is."""
        with patch("fsspec.filesystem") as mock_fs:
            mock_fs.return_value = MagicMock()
            IndexStore.get_fs("s3://bucket/key.yml", key="K", secret="S", anon=True)
            _, kwargs = mock_fs.call_args
            assert kwargs["anon"] is True
            assert kwargs["key"] == "K"

    # ---- storage_options forwarded ----------------------------------------

    def test_https_storage_options_forwarded(self) -> None:
        with patch("fsspec.filesystem") as mock_fs:
            mock_fs.return_value = MagicMock()
            IndexStore.get_fs(
                "https://example.com/data.yml",
                client_kwargs={"timeout": 30},
            )
            mock_fs.assert_called_once_with(
                "https",
                client_kwargs={"timeout": 30},
            )

    def test_local_storage_options_forwarded(self) -> None:
        with patch("fsspec.filesystem") as mock_fs:
            mock_fs.return_value = MagicMock()
            IndexStore.get_fs("/tmp/path.yml", auto_mkdir=True)
            mock_fs.assert_called_once_with("file", auto_mkdir=True)

    def test_s3_endpoint_forwarded(self) -> None:
        with patch("fsspec.filesystem") as mock_fs:
            mock_fs.return_value = MagicMock()
            IndexStore.get_fs(
                "s3://bucket/key.yml",
                key="K",
                secret="S",
                endpoint_url="https://s3.eu-dkrz-1.dkrz.cloud",
            )
            _, kwargs = mock_fs.call_args
            assert kwargs["endpoint_url"] == "https://s3.eu-dkrz-1.dkrz.cloud"

    # ---- return type ------------------------------------------------------

    def test_returns_tuple(self) -> None:
        result = IndexStore.get_fs("/tmp/path.yml")
        assert isinstance(result, tuple)
        assert len(result) == 2
