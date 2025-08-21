"""Test general utilities."""

import sys
from datetime import datetime

import pytest

from metadata_crawler.api.config import DRSConfig
from metadata_crawler.utils import convert_str_to_timestamp


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
    assert dt == datetime.fromisoformat(alt)


def test_year_month_branch_falls_back_to_alternative() -> None:
    alt = "1970-01-01"
    dt = convert_str_to_timestamp("202203", alternative=alt)
    assert dt == datetime.fromisoformat(alt)


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
