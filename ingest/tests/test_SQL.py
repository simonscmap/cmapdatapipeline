import pandas as pd
from pandas._testing import assert_frame_equal
import sys
import numpy as np

from cmapingest import common as cmn
from cmapingest import SQL


def test_build_SQL_suggestion_df():
    """What testing, ie. problems might arise with the SQL suggestion df?:
    That dtypes are correct:
    -possible breaks - time column wrong format?
    -object detection
        -string to object or numeric to object
        -numeric broken by '' or by np.NaN? Errors = ignore/coerce?

        1st test. does obviously numeric go to numeric - both float and int.
    -Can we exclude '' and ' ' and np.nan and "nan" and "NaN" and "NAN"?

    """

    test_df = pd.DataFrame({"float_int_mix": [1, 2.0, "", " ", np.nan]})
    expected_df = pd.DataFrame({"column_name": ["float_int_mix"], "dtype": ["float64"]})
    func_df = SQL.build_SQL_suggestion_df(test_df)
    assert list(func_df) == list(expected_df), "Build SQL suggestion DF test failed"
