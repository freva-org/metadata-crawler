"""Setup for the tests."""

import multiprocessing as mp
from pathlib import Path
from queue import Queue
from tempfile import TemporaryDirectory
from threading import Thread
from typing import Any, Dict, Iterator

import numpy as np
import pandas as pd
import pytest
import requests
import toml
import xarray as xr
from pymongo import MongoClient


class ThreadContext:
    """Fake the mp.get_context with threads."""

    def SimpleQueue(self) -> Queue[Any]:
        """Alias for Queue."""
        return Queue()

    def Queue(self) -> Queue[Any]:
        """Alias for Queue."""
        return Queue()

    def Process(self, *args: Any, **kwargs: Any) -> Thread:
        """Alias for Thread."""
        return Thread(*args, **kwargs)

    def Value(slef, *args: Any, **kwargs: Any) -> Any:
        return mp.Value("i", 0)


@pytest.fixture(autouse=True)
def mock_subprocess(monkeypatch) -> Iterator[None]:
    """Multiprocess -> Thread."""
    import metadata_crawler.api.metadata_stores as stores_mod
    import metadata_crawler.utils as md_utils

    monkeypatch.setattr(
        stores_mod.mp,
        "get_context",
        lambda method="spawn": ThreadContext(),
        raising=True,
    )
    monkeypatch.setattr(
        md_utils.mp,
        "get_context",
        lambda method="spawn": ThreadContext(),
        raising=True,
    )
    yield


@pytest.fixture(scope="function")
def metadata() -> Dict[str, Any]:
    return [
        {
            "file": "/foo1.nc",
            "uri": "file:///foo1.nc",
            "project": ["CMIP6"],
            "product": ["CMIP"],
            "institute": None,
            "model": ["ACCESS-CM2"],
            "experiment": ["amip"],
            "time_frequency": ["mon"],
            "realm": ["atmos"],
            "cmor_table": ["Amon"],
            "ensemble": ["r1i1p1f1"],
            "variable": ["ua"],
            "grid_label": ["gn"],
            "version": "v20191108",
            "driving_model": None,
            "rcm_name": None,
            "rcm_version": None,
            "dataset": "cmip6-fs",
            "format": "nc",
            "grid_id": None,
            "level_type": ["2d"],
            "user": None,
            "fs_type": "posix",
            "time_aggregation": ["mean"],
        },
        {
            "file": "/foo2.nc",
            "uri": "file:///foo2.nc",
            "project": ["CMIP6"],
            "product": ["CMIP"],
            "institute": None,
            "model": ["MPI-ESM1-2-LR"],
            "experiment": ["amip"],
            "time_frequency": ["mon"],
            "realm": ["atmos"],
            "cmor_table": ["Amon"],
            "ensemble": ["r2i1p1f1"],
            "variable": ["ua"],
            "grid_label": ["gn"],
            "version": "v20190815",
            "driving_model": None,
            "rcm_name": None,
            "rcm_version": None,
            "dataset": "cmip6-fs",
            "format": "nc",
            "grid_id": None,
            "level_type": ["2d"],
            "user": None,
            "fs_type": "posix",
            "time_aggregation": ["mean"],
        },
    ]


@pytest.fixture(scope="session")
def data_dir() -> Iterator[Path]:
    """Define the directory where the data is stored."""
    this_dir = Path(__file__).parent
    yield (this_dir.parent / "data").absolute()


@pytest.fixture(scope="function")
def mongo_server() -> Iterator[Dict[str, str]]:
    server = "mongodb://mongo:secret@localhost:27017"
    db = "metadata"
    with MongoClient(server) as client:
        _db = client[db]
        for col in ("latest", "files"):
            _db[col].delete_many({})
    try:
        yield {"url": server, "database": db}
    finally:
        with MongoClient(server) as client:
            db = client[db]
            for col in ("latest", "files"):
                db[col].delete_many({})


@pytest.fixture(scope="function")
def solr_server() -> Iterator[str]:
    solr_server = "http://localhost:8983"
    for core in ("files", "latest"):
        res = requests.post(
            f"{solr_server}/solr/{core}/update/json?commit=true",
            json={"delete": {"query": "*:*"}},
        )
        res.raise_for_status()
    try:
        yield solr_server
    finally:
        for core in ("files", "latest"):
            res = requests.post(
                f"{solr_server}/solr/{core}/update/json?commit=true",
                json={"delete": {"query": "*:*"}},
            )
            res.raise_for_status()


@pytest.fixture(scope="session")
def storage_options() -> Dict[str, str]:
    """Define s3 connection options."""
    return {
        "endpoint_url": "http://localhost:9000",
        "key": "minioadmin",
        "secret": "minioadmin",
    }


@pytest.fixture(scope="session")
def drs_config_path(data_dir: Path) -> Iterator[Path]:
    """Define the main config path."""
    config_file = data_dir.parent / "drs_config.toml"
    assert config_file.is_file()  # Smoke test
    config = toml.loads(config_file.read_text())
    for key, cfg in config.items():
        if not hasattr(cfg, "get"):
            continue
        if cfg.get("root_path") and ":" not in cfg["root_path"]:
            p = Path(cfg["root_path"])
            if not p.is_absolute():
                cfg["root_path"] = str((data_dir.parent / p).absolute())
    with TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir) / "drs_config.toml"
        with temp_path.open("w") as stream:
            toml.dump(config, stream)
        yield temp_path


@pytest.fixture(scope="function")
def cat_file() -> Iterator[Path]:
    with TemporaryDirectory() as temp_dir:
        yield Path(temp_dir) / "data.yml"


@pytest.fixture(scope="session")
def dataset() -> Iterator[xr.Dataset]:
    lon = xr.DataArray(
        np.linspace(-120, -30, 20),
        name="lon",
        dims=("lon",),
        coords={
            "long_name": "Longitude",
            "short_name": "lon",
            "units": "degress_east",
            "axis": "X",
        },
    )
    lat = xr.DataArray(
        np.linspace(-40, 10, 50),
        name="lat",
        dims=("lat",),
        coords={
            "long_name": "Latitude",
            "short_name": "lat",
            "units": "degress_north",
            "axis": "Y",
        },
    )
    time = xr.DataArray(
        pd.date_range("2020-01-01", "2025-12-31", freq="1YE"),
        name="time",
        dims=("time",),
        attrs={"long_name": "Time", "short_name": "time", "axis": "T"},
    )
    tas = xr.DataArray(
        np.random.rayleigh(size=(len(time), len(lat), len(lon))),
        name="tas",
        attrs={
            "long_name": "2M Air temperature",
            "short_name": "tas",
            "units": "degC",
            "member": "foo",
            "grid_label": "gn",
        },
        coords={"time": time, "lat": lat, "lon": lon},
    )
    yield xr.Dataset(
        {"tas": tas},
        attrs={
            "product": "foo",
            "frequency": "1yr",
            "institute_id": "bar",
            "experiment": "random",
        },
    )


@pytest.fixture(scope="session")
def zarr_data(dataset: xr.Dataset) -> Iterator[Path]:
    with TemporaryDirectory() as temp_dir:
        out = Path(temp_dir) / "tas.zarr"
        dataset.to_zarr(out)
        yield out
