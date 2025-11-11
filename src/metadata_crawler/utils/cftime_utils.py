"""Utilities to convert time delta to CMOR time frequencies."""

from __future__ import annotations

import datetime as dt
from typing import Any, Optional

import numpy as np
import pandas as pd
import xarray as xr


def _seconds_from_timedelta(delta: Any) -> float:
    """Normalize timedelta-like objects to seconds (float)."""
    if hasattr(delta, "to_numpy"):
        delta = delta.to_numpy()

    if isinstance(delta, np.timedelta64):
        return delta.astype("timedelta64[ns]").astype("int64") / 1e9
    if isinstance(delta, dt.timedelta):
        return delta.total_seconds()
    if isinstance(delta, pd.Timedelta):
        return delta.total_seconds()

    try:
        return float(delta)
    except Exception as exc:  # pragma: no cover
        raise TypeError(f"Unrecognized timedelta type: {type(delta)}") from exc


def _near(
    value: float, target: float, rel: float = 0.01, abs_tol: float = 60.0
) -> bool:
    """Compare two float values within relative or absolute tolerance."""
    return abs(value - target) <= max(abs_tol, abs(target) * rel)


def _map_seconds_to_cmor_like_frequency(sec: float) -> str:
    """
    Map a timestep in seconds to a CMOR-like frequency string.

    Returns one of:
        subhr, 1hr, 3hr, 6hr,
        day, 6d, 1w, sem,
        mon, season, yr, dec,
        <Xd> (generic fallback),
        unknown
    """
    day = 86400.0
    hour = 3600.0

    if sec <= 0:
        return "unknown"

    # Sub-hourly
    if sec < 0.5 * hour:
        return "subhr"

    # Hourly
    if _near(sec, hour):
        return "1hr"
    if _near(sec, 3 * hour):
        return "3hr"
    if _near(sec, 6 * hour):
        return "6hr"

    # Daily
    if _near(sec, day):
        return "day"

    # Multi-day (requested extras)
    if _near(sec, 6 * day):
        return "6d"
    if _near(sec, 7 * day):
        return "1w"
    if _near(sec, 14 * day):
        return "sem"

    # Monthly-ish
    if 20 * day <= sec <= 40 * day:
        return "mon"

    # Seasonal-ish (~3 months)
    if 80 * day <= sec <= 100 * day:
        return "season"

    # Yearly-ish
    if 350 * day <= sec <= 380 * day:
        return "yr"

    # Decadal-ish
    if 9 * 365 * day <= sec <= 11 * 365 * day:
        return "dec"

    # Fallback: express as days
    return f"{sec / day:.3g}d"


def _find_time_coord(
    ds: xr.Dataset,
    time_coord: Optional[str] = None,
) -> Optional[xr.DataArray]:
    """Best-effort detection of the time coordinate."""
    # 1) Explicit
    if time_coord is not None:
        if time_coord in ds.coords:
            return ds.coords[time_coord]
        if time_coord in ds.variables:
            return ds[time_coord]
        return None

    # 2) Coordinate literally named "time"
    if "time" in ds.coords:
        return ds.coords["time"]

    # 3) Coordinate with standard_name="time" or axis="T"
    for coord in ds.coords.values():
        std_name = coord.attrs.get("standard_name", "").lower()
        axis = coord.attrs.get("axis", "")
        if std_name == "time" or axis == "T":
            return coord

    # 4) Any coord whose dim is named "time"
    time_like_coords: list[xr.DataArray] = []
    for coord in ds.coords.values():
        if any(dim.lower() == "time" for dim in coord.dims):
            time_like_coords.append(coord)
    if time_like_coords:
        return time_like_coords[0]

    # 5) As last resort: any variable (coord or not) that looks time-like
    for var in ds.variables.values():
        if any(dim.lower() == "time" for dim in var.dims):
            # Require datetime-like or object (for cftime) to avoid bogus matches
            if np.issubdtype(var.dtype, np.datetime64) or var.dtype == "O":
                return var

    return None


def infer_cmor_like_time_frequency(
    ds: xr.Dataset,
    time_coord: Optional[str] = None,
) -> str:
    """
    Infer a CMOR-like time frequency from the first two valid time entries.

    Parameters
    ----------
    ds:
        Open xarray Dataset.
    time_coord:
        Optional explicit name of the time coordinate.

    Returns
    -------
    freq : str
        One of:
          - 'fx'        : no/insufficient/constant time
          - 'subhr'
          - '1hr', '3hr', '6hr'
          - 'day'
          - '6d', '1w', 'sem'
          - 'mon', 'season', 'yr', 'dec'
          - '<Xd>'      : generic days fallback
          - 'unknown'   : invalid/negative step
    """
    t = _find_time_coord(ds, time_coord=time_coord)

    if t is None:
        return "fx"

    # Ensure 1D along its primary dim
    if t.ndim != 1:
        main_dim = t.dims[0]
        t = t.isel({main_dim: slice(None)})

    if t.size < 2:
        return "fx"

    # Extract values
    vals = np.asarray(t.values).ravel()

    # Try via pandas for datetime64 / cftime that can be coerced
    dt_like = pd.to_datetime(vals, errors="coerce")
    valid = dt_like[~dt_like.isna()]

    if valid.size >= 2:
        uniq = np.unique(valid)
        if uniq.size < 2:
            return "fx"
        uniq.sort()
        delta = uniq[1] - uniq[0]
    else:
        # Fallback for non-coercible types: keep non-null, sort, diff
        non_null = [v for v in vals if v is not None]
        if len(non_null) < 2:
            return "fx"
        non_null = sorted(non_null)
        if non_null[0] == non_null[1]:
            return "fx"
        delta = non_null[1] - non_null[0]

    sec = _seconds_from_timedelta(delta)
    freq = _map_seconds_to_cmor_like_frequency(sec)
    return freq
