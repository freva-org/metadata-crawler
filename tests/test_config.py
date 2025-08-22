"""Testing the config module."""

from datetime import datetime
from pathlib import Path

import pytest
import toml
from jinja2 import Template

from metadata_crawler.api.config import DRSConfig
from metadata_crawler.api.storage_backend import Metadata

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
    with pytest.raises(ValueError):
        DRSConfig.load(wrong_config)

    wrong_config = Template(CONFIG).render(
        vars='{var = "foo", attr = "long_name", default = "__name__" }',
        inherits="mohh",
    )
    with pytest.raises(ValueError):
        DRSConfig.load(wrong_config)

    wrong_config = Template(CONFIG).render(
        vars='{var = "foo", attr = "long_name", default = "__name__" }',
        fs_type="foo",
    )
    with pytest.raises(NotImplementedError):
        DRSConfig.load(wrong_config)
    wrong_config = Template(CONFIG).render(
        vars='{var = "foo", attr = "long_name", default = "__name__" }',
        sources="foo",
    )
    with pytest.raises(ValueError):
        DRSConfig.load(wrong_config)


def test_read_zarr_data(zarr_data: Path) -> None:
    conf = Template(CONFIG).render(
        vars="{var = '{vars}', attr = 'short_name', default = '__name__' }",
        path=str(zarr_data),
    )
    cfg = toml.loads(conf)
    config = DRSConfig.load(cfg)
    data = config.read_metadata("bar", Metadata(path=str(zarr_data)))
    assert "model" in data and isinstance(data["model"], list)
    conf = Template(CONFIG).render(
        vars="{var = '{vars}', attr = 'short_name', default = '__name__' }",
        path=str(zarr_data),
        sources='["data", "path"]',
    )
    config = DRSConfig.load(conf)
    assert "path" in config.dialect["bar"].sources
    with pytest.raises(ValueError):
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
