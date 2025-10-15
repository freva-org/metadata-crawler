"""Test general utilities."""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from typing import Optional

import mock
import pytest

from metadata_crawler.utils import IndexProgress, convert_str_to_timestamp


@pytest.mark.parametrize("interactive", [None, True, False])
@pytest.mark.parametrize("total", [10, 0])
def test_progess_bar(interactive: Optional[bool], total: int) -> None:
    """Test the interacitve progess bar."""
    max_it = 10
    with mock.patch.dict(os.environ, {"MDC_LOG_INTERVAL": "0"}, clear=True):
        p = IndexProgress(interactive=interactive, total=total)
        try:
            p.start()
            for i in range(max_it):
                p.update(1)
        finally:
            p.stop()
        assert p._done == max_it


def test_empty_string_returns_alternative() -> None:
    alt = "2000-01-01"
    dt = convert_str_to_timestamp("", alternative=alt)
    assert dt == datetime.fromisoformat(alt)


def test_non_digit_string_returns_alternative() -> None:
    alt = "1999-12-31"
    dt = convert_str_to_timestamp("fx", alternative=alt)
    assert dt == datetime.fromisoformat(alt)


def test_year_only_branch_falls_back_to_alternative() -> None:
    alt = "1980-01-01"
    dt = convert_str_to_timestamp("1999", alternative=alt)
    assert dt.year == 1999


def test_year_month_branch_falls_back_to_alternative() -> None:
    alt = "1970-01-01"
    dt = convert_str_to_timestamp("202203", alternative=alt)
    assert dt.year == 2022
    assert dt.month == 3


def test_year_dayofyear_exact_7_digits() -> None:
    dt = convert_str_to_timestamp("2022203")
    assert dt == datetime(2022, 7, 22)


def test_full_date_8_digits() -> None:
    dt = convert_str_to_timestamp("20220131")
    assert dt == datetime(2022, 1, 31)


def test_datetime_digits_only_drop_seconds() -> None:
    dt = convert_str_to_timestamp("20220131123456")
    assert dt == datetime(2022, 1, 31, 12, 34)


def test_datetime_with_T_and_minutes() -> None:
    dt = convert_str_to_timestamp("2022-01-31T1234")
    assert dt == datetime(2022, 1, 31, 12, 34)


def test_len_gt_8_without_T_and_hour_only_falls_back_to_alternative() -> None:
    alt = "1900-01-01"
    dt = convert_str_to_timestamp("2022013112", alternative=alt)
    assert dt != datetime.fromisoformat(alt)


@pytest.mark.xfail(
    sys.version_info < (3, 13),
    reason="Python <3.13 may reject 'YYYY-MM-DDT%H' in datetime.fromisoformat",
)
def test_with_T_and_hour_only_uses_fallback_to_hours() -> None:
    dt = convert_str_to_timestamp("2022-03-04T7")
    assert dt == datetime(2022, 3, 4, 7, 0)


# tests/test_convert_str_to_timestamp.py


@pytest.mark.parametrize(
    "time_str, alternative, expected",
    [
        ("", "0001-01-01", datetime.fromisoformat("0001-01-01")),
        ("fx", "0001-01-01", datetime.fromisoformat("0001-01-01")),
        ("2022", "0001-01-01", datetime.fromisoformat("2022-01-01T00:00")),
        ("202201", "1999-12-31", datetime.fromisoformat("2022-01-31T00:00")),
        ("2022203", "0001-01-01", datetime(2022, 1, 1) + timedelta(days=203 - 1)),
        ("20220101", "0001-01-01", datetime.fromisoformat("2022-01-01")),
        ("2022010112", "0001-01-01", datetime.fromisoformat("2022-01-01T12")),
        (
            "202201011234",
            "0001-01-01",
            datetime.fromisoformat("2022-01-01T12:34"),
        ),
        (
            "20220101123456",
            "0001-01-01",
            datetime.fromisoformat("2022-01-01T12:34"),
        ),
        (
            "2022-07-22T12:34",
            "0001-01-01",
            datetime.fromisoformat("2022-07-22T12:34"),
        ),
        ("2022-01-01T", "0001-01-01", datetime.fromisoformat("2022-01-01T00")),
    ],
)
def test_convert_str_to_timestamp(time_str, alternative, expected):
    assert convert_str_to_timestamp(time_str, alternative) == expected


def test_custom_alternative_used_on_failure():
    alt = "1999-12-31"
    assert convert_str_to_timestamp("nonsense", alt) == datetime.fromisoformat(
        alt
    )
