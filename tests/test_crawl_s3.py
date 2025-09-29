"""Test crawling s3 stores."""

from pathlib import Path
from typing import Dict

import intake

from metadata_crawler import add


def test_crawl_s3_obs(
    drs_config_path: Path,
    storage_options: Dict[str, str],
) -> None:
    """Test crawling s3."""
    cat_file = "s3://test/metadata_crawler/tests/data.yml"
    add(
        cat_file,
        drs_config_path,
        data_store_prefix="s3://test/metadata_crawler/tests/metadata",
        batch_size=3,
        n_procs=1,
        data_set=["obs-s3"],
        storage_options=storage_options,
    )
    cat = intake.open_catalog(cat_file, storage_options=storage_options)
    assert len(cat.latest.read()) > 0


def test_crawl_s3_dir(
    drs_config_path: Path,
    storage_options: Dict[str, str],
) -> None:
    """Test crawling a flat directory."""
    cat_file = "s3://test/metadata_crawler/tests/data-flat.yml"
    inp_dir = (
        "s3://test/data/obs/observations/grid/CPC/CPC/cmorph/"
        "30min/atmos/30min/r1i1p1/v20210618/pr"
    )
    add(
        cat_file,
        drs_config_path,
        data_store_prefix="s3://test/metadata_crawler/tests/metadata",
        batch_size=3,
        n_procs=1,
        data_object=[inp_dir],
        storage_options=storage_options,
    )
    cat = intake.open_catalog(cat_file, storage_options=storage_options)
    assert len(cat.latest.read()) > 0


def test_crawl_s3_single_file(
    drs_config_path: Path,
    storage_options: Dict[str, str],
) -> None:
    """Test crawling a flat directory."""
    cat_file = "s3://test/metadata_crawler/tests/data-flat.yml"
    inp_file = (
        "s3://test/data/obs/observations/grid/CPC/CPC/cmorph"
        "/30min/atmos/30min/r1i1p1/v20210618/pr"
        "/pr_30min_CPC_cmorph_r1i1p1_201609020000-201609020030.nc"
    )
    add(
        cat_file,
        drs_config_path,
        data_store_prefix="s3://test/metadata_crawler/tests/metadata",
        batch_size=3,
        n_procs=1,
        data_object=[inp_file],
        storage_options=storage_options,
    )
    cat = intake.open_catalog(cat_file, storage_options=storage_options)
    assert len(cat.latest.read()) > 0


def test_crawl_s3_cmip6(
    drs_config_path: Path, storage_options: Dict[str, str]
) -> None:
    cat_file = "s3://test/metadata_crawler/tests/cmip6-s3.yml"

    add(
        cat_file,
        drs_config_path,
        data_store_prefix="s3://test/metadata_crawler/tests/cmip6-s3",
        batch_size=3,
        n_procs=1,
        data_set=["cmip6-s3"],
        verbosity=5,
        storage_options=storage_options,
    )
    cat = intake.open_catalog(cat_file, storage_options=storage_options)
    assert len(cat.latest.read()) > 0
    # There are versioned datasets so latest should not have all the entries
    assert len(cat.latest.read()) < len(cat.files.read())


def test_crawl_single_s3_file(
    drs_config_path: Path,
    cat_file: Path,
) -> None:
    """Test if we can crawl a single file on s3."""
    file = (
        "s3://test/data/model/global/cmip6/CMIP6/CMIP/CSIRO-ARCCSS/"
        "ACCESS-CM2/amip/r1i1p1f1/Amon/ua/gn/v20191108/"
        "ua_Amon_ACCESS-CM2_amip_r1i1p1f1_gn_197001-201512.nc"
    )
    add(
        cat_file,
        drs_config_path,
        n_procs=1,
        batch_size=3,
        data_object=[file],
    )
    assert cat_file.is_file()
    cat = intake.open_catalog(cat_file)
    assert len(cat.latest.read()) == len(cat.files.read()) == 1


def test_crawl_s3_zarr(drs_config_path: Path, cat_file: Path) -> None:
    """Zarr stores are directories but are treated as files."""

    add(
        cat_file,
        drs_config_path,
        data_set=["nextgems-s3"],
        n_procs=1,
        batch_size=3,
    )
    assert cat_file.is_file()
    cat = intake.open_catalog(cat_file)
    assert len(cat.latest.read()) > 0
