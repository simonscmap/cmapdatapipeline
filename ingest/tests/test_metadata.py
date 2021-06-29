import pandas as pd
from pandas._testing import assert_frame_equal
import sys
import numpy as np
import geopandas
from shapely.geometry import Point

from cmapingest import common as cmn
from cmapingest import metadata


def test_ID_Var_Map():
    test_series = pd.Series(["In-Situ", "CTD", " ", "unknownsensors"])
    expected_series = pd.Series([2.0, 5.0, 0, 0])
    func_series = metadata.ID_Var_Map(test_series, "Sensor", "tblSensors")
    print(func_series)
    assert list(func_series) == list(expected_series), "ID_Var_Map test failed."


"""Tests for Ocean Region Classification Functions"""


def test_geopandas_load_gpkg():
    input_dataframe = pd.DataFrame({"lat": [10, 20, 30], "lon": [-180, -170, -160]})
    expected_geodataframe = geopandas.GeoDataFrame(
        {
            "lat": [10, 20, 30],
            "lon": [-180, -170, -160],
            "geometry": [Point(-180, 10), Point(-170, 20), Point(-160, 30)],
        }
    )
    func_geodataframe = metadata.geopandas_load_gpkg(input_dataframe)

    assert_frame_equal(
        func_geodataframe,
        expected_geodataframe,
        check_dtype=False,
        obj="geopandas_load_gpkg test failed",
    )


# def test_load_gpkg_ocean_region():
#    """ how to test... make sure geodataframe is geodataframe. Make sure has geometry cols? IDK"""


def test_classified_gdf_to_list():
    test_gdf = geopandas.GeoDataFrame(
        {
            "NAME": ["North Atlantic", "North Atlantic", "Arctic Ocean"],
            "lat": [10, 20, 30],
            "lon": [-180, -170, -160],
            "geometry": [Point(10, -180), Point(20, -170), Point(30, -160)],
        }
    )
    expected_list = ["North Atlantic", "Arctic Ocean"]
    func_list = metadata.classified_gdf_to_list(test_gdf)
    assert func_list == expected_list, "test_classified_gdf_to_list test failed.."


def test_classify_gdf_with_gpkg_regions():
    """testing idea: create simple polygon, see if points within are picked up."""


def classify_gdf_with_gpkg_regions(data_gdf, region_gdf):
    """Takes sparse data geodataframe and classifies it to an ocean region

    Args:
        data_gdf (geopandas geodataframe): A geodataframe created from the input CMAP dataframe.
        region_gdf (geopandas geodataframe): A geodataframe created from ocean region gpkg.
    """
    classified_gdf = sjoin(data_gdf, region_gdf, how="left")
    return classified_gdf


# def test_ocean_region_classification(df):
#     """This function geographically classifes a sparse dataset into a specific ocean region

#     Args:
#         df (Pandas DataFrame): Input CMAP formatted DataFrame (ST Index: time,lat,lon,<depth>)

#     Returns:
#         ????? list of regions? or series or...

#     """
#     """

#     1. call function that uses geopandas to load gpkg as classifier map
#     2. calls function to classify df points in classifier map
#     3. formats output??
#     """
#     gdf = geopandas_load_gpkg(df)
