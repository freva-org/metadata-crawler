"""Test crawling metadata from posix fs."""

import os
from pathlib import Path

import intake
import numpy as np
import pytest

from metadata_crawler import add
from metadata_crawler.api.config import DRSConfig
from metadata_crawler.utils import EmptyCrawl, MetadataCrawlerException


def test_crawl_local_obs(
    drs_config_path: Path, cat_file: Path, data_dir: Path
) -> None:
    """Test crawling the local observations."""
    _lens = []
    add(
        cat_file,
        drs_config_path,
        n_procs=1,
        batch_size=3,
        catalogue_backend="jsonlines",
        data_object=[data_dir / "observations"],
    )
    assert cat_file.exists()
    _lens.append(len(intake.open_catalog(cat_file).latest.read()))
    add(
        cat_file,
        drs_config_path,
        n_procs=1,
        batch_size=3,
        data_set=["obs-fs"],
        catalogue_backend="jsonlines",
    )
    assert cat_file.exists()
    _lens.append(len(intake.open_catalog(cat_file).latest.read()))
    assert _lens[0] == _lens[-1]
    with pytest.raises(MetadataCrawlerException):
        add(cat_file)


def test_crawl_cordex(drs_config_path: Path, cat_file: Path) -> None:
    """Test for domain specs in cordex."""
    add(
        cat_file,
        drs_config_path.parent,
        data_set="cordex-fs",
    )
    assert cat_file.exists()
    cfg = DRSConfig.load(drs_config_path)
    cat = intake.open_catalog(cat_file).latest.read()[0]
    assert "product" in cat
    assert "EUR-11" in cat["product"]
    assert "bbox" in cat
    np.testing.assert_allclose(
        np.array(cat["bbox"]), np.array(cfg.dialect["cordex"].domains["EUR-11"])
    )


def test_crawl_local_cmip6(
    drs_config_path: Path, cat_file: Path, data_dir: Path
) -> None:
    """Test carawling local cmip6."""
    _data_dir = data_dir / "model" / "global" / "cmip6"
    _lens = []
    add(
        cat_file,
        drs_config_path,
        n_procs=10,
        batch_size=20_000,
        data_object=[_data_dir],
    )
    assert cat_file.exists()
    _lens.append(len(intake.open_catalog(cat_file).latest.read()))
    add(
        cat_file,
        drs_config_path,
        n_procs=1,
        batch_size=3,
        data_set=["cmip6-fs"],
    )
    assert cat_file.exists()
    _lens.append(len(intake.open_catalog(cat_file).latest.read()))
    assert _lens[0] == _lens[-1]
    with pytest.raises(MetadataCrawlerException):
        add(cat_file, drs_config_path, data_set=["zzz"])


def test_crawl_empty_set(drs_config_path: Path, cat_file: Path) -> None:
    """Test the behaviour of crawling non existing data(sets)."""

    with pytest.raises(MetadataCrawlerException):
        add("foo.yml", "foo.toml")
    with pytest.raises(MetadataCrawlerException):
        add("foo.yml", drs_config_path, data_set=["zzz"])
    with pytest.raises(MetadataCrawlerException):
        add("foo.yml", drs_config_path, data_set=["nextgems_zarr"])
    with pytest.raises(MetadataCrawlerException):
        add("foo.yml", drs_config_path, data_object=["/foo"])
    with pytest.raises(EmptyCrawl):
        add(cat_file, drs_config_path, data_set=["fool"])


def test_crawl_single_files(
    drs_config_path: Path, cat_file: Path, data_dir: Path
) -> None:
    """Test if we can ingest a single file."""
    add(
        cat_file,
        drs_config_path,
        data_store_prefix=str(drs_config_path.parent / "foo"),
        verbosity=5,
        data_object=[
            os.path.join(
                data_dir,
                "intake",
                "work",
                "bm1235",
                "k202181",
                "ngc4008a",
                "ngc4008a_P1D_1.zarr",
            )
        ],
    )
    cat = intake.open_catalog(cat_file)
    assert len(cat.latest.read()) > 0
    _file = os.path.join(
        data_dir,
        "intake",
        "work",
        "bm1235",
        "a270046",
        "cycle3",
        "tco2559l137",
        "hzfy",
        "hres",
        "intel.levante.openmpi",
        "lvt.intel.sp",
        "Cycle3_012020",
        "atmoce_y.fesom.2020_cropped_restricted_compressed.nc",
    )
    add(
        cat_file,
        drs_config_path,
        data_object=[_file],
        verbosity=5,
    )
    cat = intake.open_catalog(cat_file)
    assert len(cat.latest.read()) > 0


def test_crawl_thresh_fail(drs_config_path: Path, cat_file: Path) -> None:
    """Test if we can't crawl under a certain threshold."""
    with pytest.raises(EmptyCrawl):
        add(
            cat_file,
            drs_config_path,
            data_set=["obs-fs-missing"],
            catalogue_backend="jsonlines",
            fail_under=10,
        )
    assert not cat_file.exists()
