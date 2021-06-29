import pandas as pd
from pandas._testing import assert_frame_equal
import sys
import numpy as np

from cmapingest import data


def test_removeMissings():
    test_df = pd.DataFrame(
        {
            "lat": [-90, 90, "", -12.34],
            "lon": [-180, 180, 134.2, ""],
            "var": [1, 2, 3, 4],
        }
    )
    expected_df = pd.DataFrame({"lat": [-90, 90], "lon": [-180, 180], "var": [1, 2]})
    func_df = data.removeMissings(test_df, ["lat", "lon"])
    assert_frame_equal(
        func_df, expected_df, check_dtype=False, obj="removeMissings test failed"
    )


def test_format_time_col():
    test_df = pd.DataFrame(
        {
            "time": [
                "2018-03-15T05:41:00",
                "2018-03-15 05:41:00",
                "2018-03-15",
                "2018/03/15",
            ]
        }
    )
    expected_df = pd.DataFrame(
        {
            "time": [
                "2018-03-15T05:41:00",
                "2018-03-15T05:41:00",
                "2018-03-15T00:00:00",
                "2018-03-15T00:00:00",
            ]
        }
    )
    func_df = data.format_time_col(test_df, "time")
    assert_frame_equal(
        func_df,
        expected_df,
        check_datetimelike_compat=True,
        check_dtype=False,
        obj="test_format_time_col test failed",
    )


def test_sort_values():
    test_df = pd.DataFrame(
        {"time": ["2015-01-01", "2000-01-11"], "lon": [-180, -170], "lat": [-90, -80]}
    )
    expected_df = pd.DataFrame(
        {"time": ["2000-01-11", "2015-01-01"], "lon": [-170, -180], "lat": [-80, -90]}
    )
    func_df = data.sort_values(test_df, ["time", "lon", "lat"])
    assert func_df.reset_index(drop=True, inplace=True) == expected_df.reset_index(
        drop=True, inplace=True
    ), "test_sort_values test failed"


def test_ST_columns():
    test_df = pd.DataFrame(
        {"time": ["2010-01-01", "2020-01-11"], "lat": [-90, -80], "lon": [-180, -170]}
    )
    expected_list = ["time", "lat", "lon"]
    func_list = data.ST_columns(test_df)
    assert func_list == expected_list, "test ST_columns test failed."
