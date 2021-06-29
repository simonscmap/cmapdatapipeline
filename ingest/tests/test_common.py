import pandas as pd
from pandas._testing import assert_frame_equal
import sys
import numpy as np

from cmapingest import common as cmn


def test_strip_whitespace_headers():
    test_df = pd.DataFrame({" prefix_space": [""], "suffix_space     ": [""]})
    expected_df = pd.DataFrame({"prefix_space": [""], "suffix_space": [""]})
    func_df = cmn.strip_whitespace_headers(test_df)
    assert list(func_df) == list(expected_df), "Whitespace strip test failed."


def test_nanToNA():
    test_df = pd.DataFrame({"no_nans": ["1", 3], "nans": ["2", np.nan]})
    expected_df = pd.DataFrame({"no_nans": ["1", 3], "nans": ["2", ""]})
    func_df = cmn.nanToNA(test_df)
    assert_frame_equal(func_df, expected_df, obj="nan to None test failed")


def test_lowercase_List():
    test_list = ["Asdf", "SeaGlider", "CMAP"]
    expected_test_list = ["asdf", "seaglider", "cmap"]
    func_list = cmn.lowercase_List(test_list)
    assert func_list == expected_test_list, "lowercase_List test failed"


def test_getColBounds():
    test_df = pd.DataFrame({"col_1": [2, "", 4, 8]})
    expected_min_single, expected_max_single = [2], [8]
    expected_min_mult, expected_max_mult = [2, 2, 2, 2, 2], [8, 8, 8, 8, 8]
    func_min_single, func_max_single = cmn.getColBounds(test_df, "col_1")
    func_min_mult, func_max_mult = cmn.getColBounds(test_df, "col_1", 5)
    assert expected_min_single == func_min_single, """getColBounds min single failed"""
    assert expected_max_single == func_max_single, """getColBounds max single failed"""
    assert expected_min_mult == func_min_mult, """getColBounds min mult failed"""
    assert expected_max_mult == func_max_mult, """getColBounds max mult failed"""


def test_getDatasetID_DS_Name():
    input_ds_name = "Flombaum"
    ds_ID_func = cmn.getDatasetID_DS_Name(input_ds_name, server="Rainier")
    ds_ID_expected = 83
    assert ds_ID_func == ds_ID_expected, "test get datasetID from dataset name failed."


def test_getDatasetID_Tbl_Name():
    input_tblName = "tblFlombaum"
    ds_ID_func = cmn.getDatasetID_Tbl_Name(input_tblName, server="Rainier")
    ds_ID_expected = 83
    assert ds_ID_func == ds_ID_expected, "test get datasetID from table name failed."


def test_getKeywordsIDDataset():
    input_DS_id = 83
    keyword_id_list_func = cmn.getKeywordsIDDataset(input_DS_id, server="Rainier")
    keyword_id_list_expected = [1126, 1127]
    assert (
        keyword_id_list_func == keyword_id_list_expected
    ), "test get keywords from datasetID failed."


def test_getTableName_Dtypes():
    input_tblName = "tblFlombaum"
    expected_df = pd.DataFrame(
        {
            "COLUMN_NAME": [
                "time",
                "lat",
                "lon",
                "depth",
                "prochlorococcus_abundance_flombaum",
                "synechococcus_abundance_flombaum",
                "year",
                "month",
                "week",
                "dayofyear",
            ],
            "DATA_TYPE": [
                "date",
                "float",
                "float",
                "float",
                "float",
                "float",
                "smallint",
                "tinyint",
                "tinyint",
                "smallint",
            ],
        }
    )
    func_df = cmn.getTableName_Dtypes(input_tblName, server="Rainier")
    assert_frame_equal(func_df, expected_df, obj="get tablename datatypes test failed.")


def test_getCruiseDetails():
    input_cruisename = "KOK1606"
    expected_df = pd.DataFrame(
        columns=[
            "ID",
            "Nickname",
            "Name",
            "Ship_Name",
            "Start_Time",
            "End_Time",
            "Lat_Min",
            "Lat_Max",
            "Lon_Min",
            "Lon_Max",
            "Chief_Name",
            "Cruise_Series",
        ]
    )
    expected_df.loc[len(expected_df), :] = [
        589,
        "Gradients_1",
        "KOK1606",
        "R/V Kaimikai O Kanaloa",
        "2016-04-20 00:04:37",
        "2016-04-20 00:04:37",
        21.4542,
        37.8864,
        -158.3355,
        -157.858,
        "Virginia Armbrust",
        1,
    ]
    # right is script, left is DB

    func_df = cmn.getCruiseDetails(input_cruisename, server="Rainier")
    assert_frame_equal(
        func_df, expected_df, check_dtype=False, obj="get cruise details test failed."
    )


def test_findVarID():
    input_datasetID = 83
    input_Short_Name = "prochlorococcus_abundance_flombaum"
    VarID_expected = 1126
    varID_func = cmn.findVarID(input_datasetID, input_Short_Name, server="Rainier")
    assert varID_func == VarID_expected, "find variable ID test failed."


def test_verify_cruise_lists():
    test_df = pd.DataFrame({"cruise_names": ["KOK1606", "cruise_not_in_database"]})
    expected_list_matched = ["kok1606"]
    expected_list_unmatched = ["cruise_not_in_database"]
    func_match_list, func_unmatched_list = cmn.verify_cruise_lists(
        test_df, server="Rainier"
    )
    assert func_match_list == expected_list_matched, "verify_cruise_lists match failed."
    assert (
        func_unmatched_list == expected_list_unmatched
    ), "verify_cruise_lists unmatched failed."


def test_get_cruise_IDS():
    test_cruise_name_list = ["kok1606", "mgl1704"]
    expected_ID_list = [589, 593]
    func_ID_list = cmn.get_cruise_IDS(test_cruise_name_list, server="Rainier")
    assert func_ID_list == expected_ID_list, "Get cruise IDs test failed."


def test_exclude_val_from_col():
    test_series = pd.Series([1, "", " ", np.nan, "nan", "NaN", "NAN"])
    exclude_list = ["", " ", np.nan, "nan", "NaN", "NAN"]
    expected_series = pd.Series([1])
    func_series = cmn.exclude_val_from_col(test_series, exclude_list)
    assert list(func_series) == list(
        expected_series
    ), "exclude val from col test failed."


def test_tableInDB():
    test_tableName_exists = "tblMakes"
    test_dataset_name_nonexist = "tblKITTEN_GROWTH_RATE_MODEL"
    exists_bool = cmn.tableInDB(test_tableName_exists)
    nonexist_bool = cmn.tableInDB(test_dataset_name_nonexist)
    assert exists_bool == True, "tableInDB exist test failed."
    assert nonexist_bool == False, "tableInDBs nonexists test failed."


#
def test_datasetINtblDatasets():
    test_dataset_name_exists = "Flombaum"
    test_dataset_name_nonexist = "KITTEN_GROWTH_RATE_MODEL"
    exists_bool = cmn.datasetINtblDatasets(test_dataset_name_exists)
    nonexist_bool = cmn.datasetINtblDatasets(test_dataset_name_nonexist)
    assert exists_bool == True, "datasetINtblDatasets exist test failed."
    assert nonexist_bool == False, "datasetINtblDatasets nonexists test failed."


def test_length_of_tbl():
    tableName = "tblMakes"
    len_table_func = cmn.length_of_tbl(tableName)
    assert len_table_func == "3"


def test_double_chars_in_col():
    test_df = pd.DataFrame({"col_to_double": ["nomatchingchars", "={[]}:';"]})
    expected_df = pd.DataFrame(
        {"col_to_double": ["nomatchingchars", "=={{[[]]}}::'';;"]}
    )
    func_df = cmn.double_chars_in_col(
        test_df, "col_to_double", ["=", "{", "[", "]", "}", ":", "'", ";"]
    )
    assert_frame_equal(func_df, expected_df, obj="double_chars_in_col test failed")
