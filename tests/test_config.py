"""Testing the config module."""

import json
import os
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

import intake
import mock
import pytest
import toml
import xarray as xr
from jinja2 import Template

from metadata_crawler import add, index
from metadata_crawler.api.config import DRSConfig
from metadata_crawler.api.metadata_stores import DateTimeDecoder, DateTimeEncoder
from metadata_crawler.backends.posix import PosixPath
from metadata_crawler.backends.swift import SwiftPath
from metadata_crawler.utils import MetadataCrawlerException

CONFIG = """[bar]
root_path = "{{path | default('/foo')}}"
drs_format = "bar"
fs_type = "{{ fs_type | default('posix') }}"
[bar.defaults]
project = "observations"
[drs_settings.dialect.bar]
sources    = {{ sources | default(["data"]) }}
inherits_from = "{{inherits | default('freva')}}"
[drs_settings.dialect.bar.data_specs.read_kws]
engine = "zarr"
[drs_settings.dialect.bar.data_specs.globals]
institute = "institution_id"
experiment = "experiment_id"
ensemble = "ensemble"
grid_label = "grid_label"
data_vars = "__variable__"
[drs_settings.dialect.bar.data_specs.var_attrs]
variable = {{vars}}
ensemble = {var = "tas", attr = "member"}
grid_id = {var = "tas", attr = "grid_id", defualt = "foo"}
[drs_settings.dialect.bar.data_specs.stats]
bbox = {stat = "bbox", coords = ["lat", "lon"]}
time = {stat = "minmax", var = "time"}
min = {stat = "min", var = "tas" }
max = {stat = "max", var = "tas" }
nan = {stat = "min", var = "foo", default = 0}
time_range = {stat = "range", coords = "time" }
[drs_settings.dialect.bar.special.model]
type = "call"
call = "float({% raw %}{{ max }}{% endraw %})-float({% raw %}{{ min }}{% endraw %})"
[drs_settings.storage_options]
anon = true
[drs_settings.defaults]
cmor_table = "obs"
[drs_settings.dialect.bar.path_specs]
dir_parts = ["foo"]
file_parts = ["foo", "bar"]
"""


def test_bad_config() -> None:
    wrong_config = Template(CONFIG).render(
        vars='{vars = "foo", attrs = "long_name", default = "__name__" }',
    )
    with pytest.raises(MetadataCrawlerException):
        DRSConfig.load(wrong_config)

    wrong_config = Template(CONFIG).render(
        vars='{var = "foo", attr = "long_name", default = "__name__" }',
        inherits="mohh",
    )
    with pytest.raises(MetadataCrawlerException):
        DRSConfig.load(wrong_config)

    wrong_config = Template(CONFIG).render(
        vars='{var = "foo", attr = "long_name", default = "__name__" }',
        fs_type="foo",
    )
    with pytest.raises(NotImplementedError):
        DRSConfig.load(wrong_config)
    wrong_config = Template(CONFIG).render(
        vars='{var = "foo", attr = "long_name", default = "__name__" }',
        sources='["foo"]',
    )
    with pytest.raises(MetadataCrawlerException):
        DRSConfig.load(wrong_config)


def test_read_zarr_data(zarr_data: Path) -> None:
    conf = Template(CONFIG).render(
        vars="{var = '{vars}', attr = 'short_name', default = '__name__' }",
        path=str(zarr_data),
    )
    cfg = toml.loads(conf)
    config = DRSConfig.load(cfg)
    data = config.read_metadata("bar", {"path": str(zarr_data), "metadata": {}})
    assert "model" in data and isinstance(data["model"], list)
    conf = Template(CONFIG).render(
        vars="{var = '{vars}', attr = 'short_name', default = '__name__' }",
        path=str(zarr_data),
        sources='["data", "path"]',
    )
    config = DRSConfig.load(conf)
    assert "path" in config.dialect["bar"].sources
    with pytest.raises(MetadataCrawlerException):
        data = config.dialect["bar"].path_specs.get_metadata_from_path(
            Path("foo/muh_bar_zup.nc")
        )
    assert (
        len(
            config.index_schema["time_aggregation"].get_time_range(
                [datetime.now()]
            )
        )
        == 2
    )


def test_get_search():
    """Test getting the right settings."""
    conf = Template(CONFIG).render(
        vars="{var = '{vars}', attr = 'short_name', default = '__name__' }",
    )
    from metadata_crawler.run import _get_search

    assert len(_get_search(conf)) == 1
    with pytest.raises(ValueError):
        index("foo", conf)


def test_benchmark_settings(drs_config_path: Path, cat_file: Path) -> None:
    """Test some benchmark settings."""
    env = os.environ.copy()
    with mock.patch.dict(os.environ, {"MDC_MAX_FILES": "5"}, clear=False):
        add(
            cat_file,
            drs_config_path,
            n_procs=1,
            batch_size=3,
            catalogue_backend="jsonlines",
            data_set=["obs-fs"],
        )
    os.environ = env
    assert cat_file.exists()
    len(intake.open_catalog(cat_file).latest.read()) < 10


@pytest.mark.asyncio
async def test_posix() -> None:
    """Test posix behaviour."""
    p = PosixPath()
    _dirs = [f async for f in p.iterdir(".")]
    assert len(_dirs)
    _dirs = [f async for f in p.iterdir("/foo/bar")]
    assert not (len(_dirs))


def test_datetime_decoder_encoder() -> None:
    """Test the Datetime Encoding/Decoding."""
    now = datetime.now()
    out = json.dumps(
        {
            "now": now,
            "foo": ["bar"],
        },
        cls=DateTimeEncoder,
    )
    assert "now" in out
    assert now.isoformat() in out

    out1 = json.loads(out.encode(), cls=DateTimeDecoder)
    assert "now" in out1
    assert out1["now"] == now


def test_reading_data_attributes(
    drs_config_path: Path, dataset: xr.Dataset, zarr_data: Path
) -> None:
    """Test reading different data attributes."""
    p = PosixPath()
    cfg = DRSConfig.load(drs_config_path)

    with TemporaryDirectory() as temp_dir:
        nc_file = Path(temp_dir) / "nc_file.nc"
        dataset.to_netcdf(nc_file, engine="h5netcdf")
        assert p.read_attr("tas.grid_label", nc_file) == "gn"
        assert p.read_attr("product", nc_file) == "foo"
        assert p.read_attr("product", nc_file, engine="h5netcdf") == "foo"
        assert p.read_attr("tas.grid_label", nc_file, engine="h5netcdf") == "gn"

    assert p.read_attr("product", zarr_data) == "foo"
    assert p.read_attr("tas.grid_label", zarr_data) == "gn"
    sw = SwiftPath(**cfg.datasets["obs-swift"].storage_options)
    path = (
        "/observations/grid/CPC/CPC/cmorph/30min/atmos/30min/r1i1p1/"
        "v20210618/pr/pr_30min_CPC_cmorph_r1i1p1_201609020000-201609020030.nc"
    )
    assert sw.read_attr("ensemble", path) == "r1i1p1"
