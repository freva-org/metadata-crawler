"""Test crawling intake catalogues."""

import json
import os
from pathlib import Path
from tempfile import NamedTemporaryFile

import intake
import mock
import numpy as np
import pandas as pd
import pytest

from metadata_crawler import add
from metadata_crawler.backends.intake import IntakePath
from metadata_crawler.cli import walk_catalogue


def test_crawl_intake_catalogue(
    drs_config_path: Path,
    cat_file: Path,
    data_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test if we can crawl intake catalogues."""
    monkeypatch.chdir(data_dir.parent)
    with mock.patch.dict(os.environ, {"MDC_INTERACTIVE": "1"}, clear=True):
        inp = data_dir / "intake" / "catalog" / "catalog.yaml"
        add(
            cat_file,
            drs_config_path,
            data_object=[inp],
            verbosity=10,
            batch_size=3,
            n_procs=1,
            catalogue_backend="jsonlines",
        )
    assert cat_file.exists()
    cat = intake.open_catalog(cat_file)
    assert len(cat.latest.read()) > 0


def test_walk_intake_catalogue(data_dir: Path) -> None:
    """Test walking the catalogue."""
    cat_file = data_dir / "intake" / "catalog" / "dkrz_dyamond-winter_disk.json"
    num = walk_catalogue(cat_file)
    assert num > 100


def test_intake_utils() -> None:
    p = IntakePath()
    with NamedTemporaryFile(suffix=".json") as temp_d:
        Path(temp_d.name).write_text(json.dumps({"foo": "bar"}))
        assert p._is_esm_catalogue(temp_d.name) is False
        Path(temp_d.name).write_text(json.dumps({"esmcat": "bar"}))
        assert p._is_esm_catalogue(temp_d.name) is True
        lines = "first" + 30 * "foo\n" + "esmcat"
        print(lines)
        Path(temp_d.name).write_text(lines)
        assert p._is_esm_catalogue(temp_d.name) is False
    with NamedTemporaryFile(suffix=".foo") as temp_d:
        Path(temp_d.name).write_text(json.dumps({"esmcat": "bar"}))
        assert p._is_esm_catalogue(temp_d.name) is False
    df = pd.DataFrame(
        {
            "array": [np.array([1, 2])],
            "nan": [pd.NaT],
            "string": ["foo"],
            "int": [3],
            "list": [[1, 2]],
        }
    )
    assert isinstance(p._to_py(p), IntakePath)
    assert p._to_py(df["nan"][0]) is None
    assert isinstance(p._to_py(df["array"][0]), list)
    assert isinstance(p._to_py(df["string"][0]), str)
    assert isinstance(p._to_py(df["int"][0]), int)
    assert isinstance(p._to_py(df["list"][0]), list)
