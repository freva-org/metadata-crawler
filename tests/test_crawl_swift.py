"""Test crawling swift stores."""

from pathlib import Path
from tempfile import TemporaryDirectory

import intake
import pytest

from metadata_crawler import add
from metadata_crawler.backends.swift import SwiftPath


def test_crawl_obs(
    drs_config_path: Path,
    cat_file: Path,
) -> None:
    """Test crawling swift obs."""

    add(
        cat_file,
        drs_config_path,
        batch_size=3,
        n_procs=1,
        data_set=["obs-swift"],
        catalogue_backend="jsonlines",
    )
    assert cat_file.exists()
    cat = intake.open_catalog(cat_file)
    assert len(cat.latest.read()) > 0


def test_crawl_cmip6(
    drs_config_path: Path,
    cat_file: Path,
) -> None:

    add(
        cat_file,
        drs_config_path,
        batch_size=3,
        n_procs=1,
        data_set=["cmip6-swift"],
    )
    assert cat_file.exists()
    cat = intake.open_catalog(cat_file)
    assert len(cat.latest.read()) > 0
    # There are versioned datasets so latest should not have all the entries
    assert len(cat.latest.read()) < len(cat.files.read())


def test_crawl_single_file(
    drs_config_path: Path,
    cat_file: Path,
) -> None:
    """Test if we can crawl a single file on s3."""
    file = (
        "http://localhost:8081/v1/AUTH_test/test/model/global/cmip6/CMIP6/CMIP"
        "/CSIRO-ARCCSS/ACCESS-CM2/amip/r1i1p1f1/Amon/ua/gn/v20191108/"
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


def test_crawl_zarr(drs_config_path: Path, cat_file: Path) -> None:
    """Zarr stores are directories but are treated as files."""

    add(
        cat_file,
        drs_config_path,
        data_set=["nextgems-swift"],
        n_procs=1,
        batch_size=3,
        verbosity=4,
    )
    assert cat_file.is_file()
    cat = intake.open_catalog(cat_file)
    assert len(cat.latest.read()) > 0


@pytest.mark.asyncio
async def test_crawl_no_auth(drs_config_path: Path) -> None:
    """No file available."""
    p = SwiftPath(
        os_storage_url="http://localhost:8081/v1/AUTH_test/test",
    )
    assert p.os_auth_url
    with pytest.raises(ValueError):
        await p.is_file("/foo")
    p = SwiftPath()
    assert p.os_auth_url == ""
    with pytest.raises(RuntimeError):
        await p.is_dir("/foo")
    p = SwiftPath(
        os_storage_url="http://localhost:8081/v1/AUTH_test/test",
        os_password="testing",
        os_project_id="test2",
        os_user_id="tester",
    )
    num = 0
    with pytest.raises(ValueError):
        async for file in p.iterdir("/model/nextgems"):
            num += 1
    assert num == 0


@pytest.mark.asyncio
async def test_crawl_no_file(drs_config_path: Path) -> None:
    """No auth available."""
    p = SwiftPath(
        os_storage_url="http://localhost:8081/v1/AUTH_test/test",
        os_password="testing",
        os_project_id="test",
        os_user_id="tester",
    )
    num = 0
    async for file in p.iterdir("/foo/bar"):
        num += 1
    assert num == 0
