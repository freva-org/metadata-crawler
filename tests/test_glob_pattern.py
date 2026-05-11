from pathlib import Path
from unittest.mock import MagicMock, patch

from metadata_crawler.api.metadata_stores import CatalogueReader, IndexStore


class TestRglobFiles:
    """Tests for CatalogueReader.rglob_stores."""

    # ---- local filesystem: single file ------------------------------------

    def test_single_file_returns_path(self, tmp_path: Path) -> None:
        f = tmp_path / "data.yml"
        f.write_text("hello")
        result = CatalogueReader.rglob_stores(str(f))
        assert result == [str(f)]

    def test_single_file_non_yml_returns_path(self, tmp_path: Path) -> None:
        """A concrete path is returned as-is regardless of extension."""
        f = tmp_path / "data.json"
        f.write_text("{}")
        result = CatalogueReader.rglob_stores(str(f))
        assert result == [str(f)]

    def test_nonexistent_path_returns_path(self, tmp_path: Path) -> None:
        missing = tmp_path / "does_not_exist.yml"
        result = CatalogueReader.rglob_stores(str(missing))
        assert result == [str(missing)]

    def test_safe_fallback(self, tmp_path: Path) -> None:

        result = CatalogueReader.rglob_stores(str(tmp_path))
        assert result == [str(tmp_path)]

    # ---- local filesystem: directory --------------------------------------

    def test_directory_finds_yml_recursively(self, tmp_path: Path) -> None:
        (tmp_path / "a.yml").write_text("")
        (tmp_path / "b.txt").write_text("")
        sub = tmp_path / "sub"
        sub.mkdir()
        (sub / "c.yml").write_text("")
        (sub / "d.json").write_text("")

        result = CatalogueReader.rglob_stores(str(tmp_path), backend="intake")
        assert sorted(result) == sorted(
            [
                str(tmp_path / "a.yml"),
                str(sub / "c.yml"),
            ]
        )

    def test_empty_directory_returns_empty(self, tmp_path: Path) -> None:
        d = tmp_path / "empty"
        d.mkdir()
        result = CatalogueReader.rglob_stores(str(d), backend="intake")
        assert result == []

    def test_directory_no_yml_returns_empty(self, tmp_path: Path) -> None:
        (tmp_path / "readme.txt").write_text("")
        (tmp_path / "data.json").write_text("")
        result = CatalogueReader.rglob_stores(str(tmp_path), backend="intake")
        assert result == []

    def test_trailing_slash_stripped(self, tmp_path: Path) -> None:
        (tmp_path / "a.yml").write_text("")
        result = CatalogueReader.rglob_stores(str(tmp_path) + "/", backend="intake")
        assert len(result) == 1
        assert result[0].endswith("a.yml")

    # ---- local filesystem: glob patterns ----------------------------------

    def test_glob_star(self, tmp_path: Path) -> None:
        (tmp_path / "data_mon.yml").write_text("")
        (tmp_path / "data_tue.yml").write_text("")
        (tmp_path / "other.txt").write_text("")

        pattern = str(tmp_path / "data_*.yml")
        result = CatalogueReader.rglob_stores(pattern, backend="intake")
        assert sorted(result) == sorted(
            [
                str(tmp_path / "data_mon.yml"),
                str(tmp_path / "data_tue.yml"),
            ]
        )

    def test_glob_question_mark(self, tmp_path: Path) -> None:
        (tmp_path / "data_a.yml").write_text("")
        (tmp_path / "data_b.yml").write_text("")
        (tmp_path / "data_ab.yml").write_text("")

        pattern = str(tmp_path / "data_?.yml")
        result = CatalogueReader.rglob_stores(pattern, backend="intake")
        assert sorted(result) == sorted(
            [
                str(tmp_path / "data_a.yml"),
                str(tmp_path / "data_b.yml"),
            ]
        )

    def test_glob_brackets(self, tmp_path: Path) -> None:
        (tmp_path / "data_a.yml").write_text("")
        (tmp_path / "data_b.yml").write_text("")
        (tmp_path / "data_c.yml").write_text("")

        pattern = str(tmp_path / "data_[ab].yml")
        result = CatalogueReader.rglob_stores(pattern, backend="intake")
        assert sorted(result) == sorted(
            [
                str(tmp_path / "data_a.yml"),
                str(tmp_path / "data_b.yml"),
            ]
        )

    def test_glob_doublestar(self, tmp_path: Path) -> None:
        sub = tmp_path / "sub"
        sub.mkdir()
        (tmp_path / "top.yml").write_text("")
        (sub / "nested.yml").write_text("")

        pattern = str(tmp_path / "**/*.yml")
        result = CatalogueReader.rglob_stores(pattern, backend="intake")
        assert str(sub / "nested.yml") in result

    def test_glob_no_match_returns_empty(self, tmp_path: Path) -> None:
        pattern = str(tmp_path / "*.yml")
        result = CatalogueReader.rglob_stores(pattern, backend="intake")
        assert result == []

    # ---- S3 filesystem (mocked) -------------------------------------------

    def test_s3_glob_pattern(self) -> None:
        mock_fs = MagicMock()
        mock_fs.glob.return_value = [
            "freva/scratch/metadata_crawler/xces/data_user1.yml",
            "freva/scratch/metadata_crawler/xces/data_user2.yml",
        ]
        mock_fs.unstrip_protocol.side_effect = lambda p: f"s3://{p}"

        with patch.object(IndexStore, "get_fs", return_value=(mock_fs, False)):
            result = CatalogueReader.rglob_stores(
                "s3://freva/scratch/metadata_crawler/xces/*.yml",
                backend="intake",
            )

        mock_fs.glob.assert_called_once()
        assert len(result) == 2

    def test_s3_directory(self) -> None:
        mock_fs = MagicMock()
        mock_fs.unstrip_protocol.side_effect = lambda p: f"s3://{p}"
        mock_fs.isdir.return_value = True
        mock_fs.find.return_value = [
            "/freva/scratch/xces/data.yml",
            "/freva/scratch/xces/metadata.json",
            "/freva/scratch/xces/sub/other.yml",
        ]

        with patch.object(IndexStore, "get_fs", return_value=(mock_fs, False)):
            result = CatalogueReader.rglob_stores(
                "s3://freva/scratch/xces", backend="intake"
            )

        assert sorted(result) == [
            "s3:///freva/scratch/xces/data.yml",
            "s3:///freva/scratch/xces/sub/other.yml",
        ]

    def test_s3_single_file(self) -> None:
        mock_fs = MagicMock()
        mock_fs.unstrip_protocol.side_effect = lambda p: f"s3://{p}"
        mock_fs.isdir.return_value = False

        with patch.object(IndexStore, "get_fs", return_value=(mock_fs, False)):
            result = CatalogueReader.rglob_stores(
                "s3://freva/scratch/xces/data_latest.yml",
                backend="intake",
            )

        assert result == ["s3://freva/scratch/xces/data_latest.yml"]

    # ---- HTTP filesystem (fully qualified path) ---------------------------

    def test_https_path_returned_as_is(self) -> None:
        mock_fs = MagicMock()
        mock_fs.unstrip_protocol.side_effect = lambda p: p
        mock_fs.isdir.return_value = False

        with patch.object(IndexStore, "get_fs", return_value=(mock_fs, False)):
            url = "https://s3.eu-dkrz-1.dkrz.cloud/freva/metadata_crawler/general/data_latest.yml"
            result = CatalogueReader.rglob_stores(url)

        assert result == [url]

    def test_https_isdir_raises_not_implemented(self) -> None:
        mock_fs = MagicMock()
        mock_fs.unstrip_protocol.side_effect = lambda p: p
        mock_fs.isdir.side_effect = NotImplementedError

        with patch.object(IndexStore, "get_fs", return_value=(mock_fs, False)):
            url = "https://example.com/data"
            result = CatalogueReader.rglob_stores(url, backend="intake")

        assert result == [url]

    # ---- storage_options forwarded ----------------------------------------

    def test_storage_options_forwarded(self) -> None:
        mock_fs = MagicMock()
        mock_fs.unstrip_protocol.side_effect = lambda p: f"s3://{p}"
        mock_fs.isdir.return_value = False

        with patch.object(
            IndexStore, "get_fs", return_value=(mock_fs, False)
        ) as mock_get:
            CatalogueReader.rglob_stores(
                "s3://bucket/path.yml",
                key="K",
                secret="S",
                endpoint_url="https://s3.example.com",
            )

        mock_get.assert_called_once_with(
            "s3://bucket/path.yml",
            key="K",
            secret="S",
            endpoint_url="https://s3.example.com",
        )
