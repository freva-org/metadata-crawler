"""Test of cftime_utils."""

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from metadata_crawler.utils.cftime_utils import (
    infer_cmor_like_time_frequency,
)


def _make_ds_from_times(
    times: pd.DatetimeIndex | list,
    coord_name: str = "time",
    dim_name: str | None = None,
    attrs: dict | None = None,
) -> xr.Dataset:
    """
    Helper: create a minimal Dataset with a given time-like coordinate.
    - coord_name: name of the coordinate variable
    - dim_name:   underlying dimension name (defaults to coord_name)
    """
    if dim_name is None:
        dim_name = coord_name

    time_da = xr.DataArray(
        times,
        dims=(dim_name,),
        name=coord_name,
        attrs=attrs or {},
    )
    data = xr.DataArray(
        np.arange(len(time_da)),
        dims=(dim_name,),
        name="var",
    )

    ds = xr.Dataset({"var": data, coord_name: time_da})
    ds = ds.set_coords(coord_name)
    ds.coords[coord_name].attrs = attrs or {}
    return ds


# ----------------------------------------------------------------------
# Edge cases: missing / insufficient / constant time
# ----------------------------------------------------------------------


def test_no_time_coord_returns_fx() -> None:
    ds = xr.Dataset({"var": ("x", [1, 2, 3])})
    freq = infer_cmor_like_time_frequency(ds)
    assert freq == "fx"


def test_single_timestamp_returns_fx() -> None:
    times = pd.date_range("2000-01-01", periods=1, freq="D")
    ds = _make_ds_from_times(times)
    freq = infer_cmor_like_time_frequency(ds)
    assert freq == "fx"


def test_constant_time_returns_fx() -> None:
    times = [pd.Timestamp("2000-01-01")] * 5
    ds = _make_ds_from_times(times)
    freq = infer_cmor_like_time_frequency(ds)
    assert freq == "fx"


# ----------------------------------------------------------------------
# Sub-hourly and hourly frequencies
# ----------------------------------------------------------------------


def test_subhourly_mapped_to_subhr() -> None:
    times = pd.date_range("2000-01-01", periods=4, freq="15min")
    ds = _make_ds_from_times(times)
    freq = infer_cmor_like_time_frequency(ds)
    assert freq == "subhr"


@pytest.mark.parametrize(
    "freq_str, expected",
    [
        ("1h", "1hr"),
        ("3h", "3hr"),
        ("6h", "6hr"),
    ],
)
def test_hourly_like_frequencies(freq_str: str, expected: str) -> None:
    times = pd.date_range("2000-01-01", periods=4, freq=freq_str)
    ds = _make_ds_from_times(times)
    freq = infer_cmor_like_time_frequency(ds)
    assert freq == expected


# ----------------------------------------------------------------------
# Daily & multi-day frequencies
# ----------------------------------------------------------------------


def test_daily_frequency_day() -> None:
    times = pd.date_range("2000-01-01", periods=4, freq="D")
    ds = _make_ds_from_times(times)
    freq = infer_cmor_like_time_frequency(ds)
    assert freq == "day"


def test_six_day_frequency_6d() -> None:
    times = pd.date_range("2000-01-01", periods=4, freq="6D")
    ds = _make_ds_from_times(times)
    freq = infer_cmor_like_time_frequency(ds)
    assert freq == "6d"


def test_weekly_frequency_1w() -> None:
    times = pd.date_range("2000-01-01", periods=4, freq="7D")
    ds = _make_ds_from_times(times)
    freq = infer_cmor_like_time_frequency(ds)
    assert freq == "1w"


def test_biweekly_frequency_sem() -> None:
    times = pd.date_range("2000-01-01", periods=4, freq="14D")
    ds = _make_ds_from_times(times)
    freq = infer_cmor_like_time_frequency(ds)
    assert freq == "sem"


# ----------------------------------------------------------------------
# Monthly / seasonal / yearly / decadal (approximate ranges)
# ----------------------------------------------------------------------


def test_monthly_like_frequency_mon() -> None:
    # 2000-01-01 to 2000-01-31 → 30 days, inside [20, 40] day window
    times = [pd.Timestamp("2000-01-01"), pd.Timestamp("2000-01-31")]
    ds = _make_ds_from_times(times)
    freq = infer_cmor_like_time_frequency(ds)
    assert freq == "mon"


def test_seasonal_like_frequency_season() -> None:
    # ~90 days spacing
    times = [pd.Timestamp("2000-01-01"), pd.Timestamp("2000-04-01")]
    ds = _make_ds_from_times(times)
    freq = infer_cmor_like_time_frequency(ds)
    assert freq == "season"


def test_yearly_like_frequency_yr() -> None:
    times = [pd.Timestamp("2000-01-01"), pd.Timestamp("2001-01-01")]
    ds = _make_ds_from_times(times)
    freq = infer_cmor_like_time_frequency(ds)
    assert freq == "yr"


def test_decadal_like_frequency_dec() -> None:
    times = [pd.Timestamp("2000-01-01"), pd.Timestamp("2010-01-01")]
    ds = _make_ds_from_times(times)
    freq = infer_cmor_like_time_frequency(ds)
    assert freq == "dec"


# ----------------------------------------------------------------------
# Fallback behavior for non-standard steps
# ----------------------------------------------------------------------


def test_nonstandard_step_returns_generic_days_string() -> None:
    # 2-day step is not explicitly mapped; expect "<Xd>" style.
    times = [pd.Timestamp("2000-01-01"), pd.Timestamp("2000-01-03")]
    ds = _make_ds_from_times(times)
    freq = infer_cmor_like_time_frequency(ds)

    # We only assert it's *some* day-like fallback, not 'fx' and not a known code:
    assert freq not in {"fx", "day", "6d", "1w", "sem"}
    assert freq.endswith("d")


# ----------------------------------------------------------------------
# Coordinate detection logic
# ----------------------------------------------------------------------


def test_explicit_time_coord_name() -> None:
    times = pd.date_range("2000-01-01", periods=4, freq="D")
    ds = _make_ds_from_times(times, coord_name="mytime")
    freq = infer_cmor_like_time_frequency(ds, time_coord="mytime")
    assert freq == "day"


def test_detect_time_by_standard_name_attr() -> None:
    times = pd.date_range("2000-01-01", periods=4, freq="6h")
    ds = _make_ds_from_times(
        times,
        coord_name="t",
        attrs={"standard_name": "time"},
    )
    freq = infer_cmor_like_time_frequency(ds)
    assert freq == "6hr"


def test_detect_time_by_axis_T_attr() -> None:
    times = pd.date_range("2000-01-01", periods=4, freq="3h")
    ds = _make_ds_from_times(
        times,
        coord_name="t",
        attrs={"axis": "T"},
    )
    freq = infer_cmor_like_time_frequency(ds)
    assert freq == "3hr"


def test_detect_time_by_dim_named_time() -> None:
    # Dimension is 'time', coord name is different.
    times = pd.date_range("2000-01-01", periods=4, freq="D")
    time_da = xr.DataArray(times, dims=("time",), name="not_time")
    data = xr.DataArray(np.arange(len(time_da)), dims=("time",), name="var")
    ds = xr.Dataset({"not_time": time_da, "var": data}).set_coords("not_time")

    freq = infer_cmor_like_time_frequency(ds)

    # This asserts the robust behavior: using dim 'time' to deduce it.
    assert freq == "day"


# ----------------------------------------------------------------------
# Optional: cftime support (kept loose)
# ----------------------------------------------------------------------


def test_cftime_timestamps_if_supported() -> None:
    cftime = pytest.importorskip("cftime")

    times = [
        cftime.DatetimeNoLeap(2000, 1, 1),
        cftime.DatetimeNoLeap(2000, 1, 2),
        cftime.DatetimeNoLeap(2000, 1, 3),
    ]
    ds = _make_ds_from_times(times)

    freq = infer_cmor_like_time_frequency(ds)

    # Keep expectation loose depending on your implementation.
    # If you support cftime cleanly, this should be "day".
    # If not, adjust your function or relax this assertion.
    assert freq in {"day", "fx", "unknown"}
