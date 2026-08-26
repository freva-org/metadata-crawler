"""Tests for ``SchemaField.get_time_range``.

The postcondition that matters downstream is monotonicity: solr rejects a
``daterange`` whose start lies after its end, so an inverted input has to be
repaired here rather than surfacing as a failed ingest.
"""

from datetime import datetime
from typing import Any, List, Union

import numpy as np
import pytest

from metadata_crawler.api.config import SchemaField

MIN = datetime(1, 1, 1, 0, 0)
MAX = datetime(9999, 12, 31, 23, 59)


def rng(time_stamp: Union[str, List[Any], None]) -> List[datetime]:
    return SchemaField.get_time_range(time_stamp)


class TestMonotonicity:
    """A range is never returned with its bounds the wrong way round."""

    @pytest.mark.parametrize(
        "time_stamp",
        [
            "20200101-20191231",
            "200001-199912",
            "2020-2019",
            "20200101T1200-20200101T0600",
            "1970010100-1969123123",
            ["2020-12-31", "2020-01-01"],
            ["9999-12-31", "2020-01-01"],
            [datetime(2021, 1, 1), datetime(2020, 1, 1)],
            [np.datetime64("2021-05-01T00:00:00"), np.datetime64("2020-05-01")],
        ],
    )
    def test_inverted_input_is_repaired(
        self, time_stamp: Union[str, List[Any]]
    ) -> None:
        start, end = rng(time_stamp)
        assert start <= end

    @pytest.mark.parametrize(
        "time_stamp",
        [
            "",
            "fx",
            "2020",
            "20200101-20201231",
            ["fx", "fx"],
            ["2020-01-01"],
            ["2020-01-01", "2020-12-31"],
            [datetime(2020, 1, 1), datetime(2021, 1, 1)],
            None,
        ],
    )
    def test_ordered_input_stays_ordered(
        self, time_stamp: Union[str, List[Any], None]
    ) -> None:
        start, end = rng(time_stamp)
        assert start <= end

    def test_swap_widens_rather_than_mirrors(self) -> None:
        """The repaired range is the same interval, not a reflected one.

        Swapping the two *results* would leave the defaulted time of day on
        the wrong end (start 23:59, end 00:00) and narrow the interval.  The
        bounds are re-converted instead, so the day still runs 00:00..23:59.
        """
        assert rng(["2020-12-31", "2020-01-01"]) == rng(
            ["2020-01-01", "2020-12-31"]
        )
        assert rng("20200101-20191231") == rng("20191231-20200101")

    def test_repaired_range_covers_both_bounds(self) -> None:
        start, end = rng("20200101T1200-20200101T0600")
        assert start == datetime(2020, 1, 1, 6, 0)
        assert end == datetime(2020, 1, 1, 12, 0)

    def test_open_start_is_not_swapped(self) -> None:
        """``fx`` on the left is an open start, not an inverted range."""
        assert rng(["fx", "2020"]) == [MIN, datetime(2020, 12, 31, 23, 59)]

    def test_open_end_is_not_swapped(self) -> None:
        assert rng(["2020", "fx"]) == [datetime(2020, 1, 1, 0, 0), MAX]


class TestFallbacks:
    """Unparseable bounds widen the range instead of failing."""

    @pytest.mark.parametrize("time_stamp", ["", "fx", None, ["fx", "fx"]])
    def test_unparseable_becomes_the_full_range(
        self, time_stamp: Union[str, List[Any], None]
    ) -> None:
        assert rng(time_stamp) == [MIN, MAX]

    def test_year_only_opens_the_end(self) -> None:
        assert rng("2020") == [datetime(2020, 1, 1, 0, 0), MAX]

    def test_single_element_list_spans_the_day(self) -> None:
        assert rng(["2020-01-01"]) == [
            datetime(2020, 1, 1, 0, 0),
            datetime(2020, 1, 1, 23, 59),
        ]


class TestInputHandling:
    """Accepted input shapes and their side effects."""

    def test_datetimes_are_accepted(self) -> None:
        assert rng([datetime(2020, 1, 1), datetime(2020, 6, 1)]) == [
            datetime(2020, 1, 1, 0, 0),
            datetime(2020, 6, 1, 0, 0),
        ]

    def test_numpy_datetimes_are_accepted(self) -> None:
        assert rng(
            [
                np.datetime64("2020-01-01T00:00:00"),
                np.datetime64("2020-06-01T12:00:00"),
            ]
        ) == [datetime(2020, 1, 1, 0, 0), datetime(2020, 6, 1, 12, 0)]

    def test_day_precision_end_widens_to_end_of_day(self) -> None:
        """``np.datetime64('2020-06-01')`` carries no time, so the end opens."""
        assert rng(
            [np.datetime64("2020-01-01"), np.datetime64("2020-06-01")]
        ) == [datetime(2020, 1, 1, 0, 0), datetime(2020, 6, 1, 23, 59)]

    def test_underscore_and_colon_separators(self) -> None:
        assert rng("2020:01:01_2020:12:31") == rng("20200101-20201231")

    def test_caller_list_is_not_mutated(self) -> None:
        """``get_time_range`` is handed values straight out of the metadata."""
        source = [datetime(2021, 1, 1), datetime(2020, 1, 1)]
        expected = list(source)
        rng(source)
        assert source == expected

    def test_always_returns_two_values(self) -> None:
        for time_stamp in ("", "2020", ["2020-01-01"], ["a", "b", "c"]):
            assert len(rng(time_stamp)) == 2
