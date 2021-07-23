import sys
import os
import glob
import pandas as pd
import geopandas as gpd
from geopandas.tools import sjoin
from tqdm import tqdm

import credentials as cr


import pycmap

pycmap.API(cr.api_key)


import vault_structure as vs
import transfer
import data
import DB
import metadata
import SQL
import mapping
import stats
import common as cmn
import cruise

"""
###############################################
###############################################
     Ocean Region Classification Functions
###############################################
###############################################
"""


def if_exists_dataset_region(dataset_name, server):
    """Checks if dataset ID is already in tblDatasets_Regions

    Args:
        dataset_name (string): The short name of the dataset in CMAP tblDatasets.
        server (string): Valid CMAP server name : ex. rainier
    Returns: Boolean
    """
    ds_ID = cmn.getDatasetID_DS_Name(dataset_name)
    cur_str = """SELECT * FROM [Opedia].[dbo].[tblDataset_Regions] WHERE [Dataset_ID] = {Dataset_ID}""".format(
        Dataset_ID=ds_ID
    )
    query_return = DB.dbRead(cur_str, server)
    if query_return.empty:
        bool_return = False
    else:
        bool_return = True
    return bool_return


def geopandas_load_gpkg(input_df):
    """

    Args:
        input_df (Pandas DataFrame): CMAP formatted DataFrame
    Returns:
        gdf (Geopandas GeoDataFrame): Geopandas formatted DataFrame. ie. geometry column.
    """
    gdf = gpd.GeoDataFrame(
        input_df, geometry=gpd.points_from_xy(input_df.lon, input_df.lat)
    )
    return gdf


def load_gpkg_ocean_region(input_gpkg_fpath):
    """Uses Geopandas to load input geopackage (gpkg)

    Args:
        input_gpkg_fpath (Geopackage - .gpkg): Input Geopackage containing geometries used for ocean region classifcation.
    Returns:
        gpkg_region (GeoDataFrame): Outputs geodataframe of ocean region geometries.
    """
    gdf = gpd.read_file(input_gpkg_fpath)
    return gdf


def classify_gdf_with_gpkg_regions(data_gdf, region_gdf):
    """Takes sparse data geodataframe and classifies it to an ocean region

    Args:
        data_gdf (geopandas geodataframe): A geodataframe created from the input CMAP dataframe.
        region_gdf (geopandas geodataframe): A geodataframe created from ocean region gpkg.
    """
    classified_gdf = sjoin(data_gdf, region_gdf, how="left", op="within")
    # This line removes any rows where null exists. This might be due to points to close to land.
    classified_gdf_nonull = classified_gdf[~classified_gdf["NAME"].isnull()]
    return classified_gdf_nonull


def classified_gdf_to_list(classified_gdf):
    """Takes a classified/joined geodataframe and returns a set of ocean regions

    Args:
        classified_gdf (geopandas geodataframe): The joined geodataframe that contains points as well as regions.
    Returns:
        region_set : A unique list of regions belonging to that dataset.
    """
    region_set = list(set(classified_gdf["NAME"]))
    return region_set


def ocean_region_classification(data_df):
    """This function geographically classifes a sparse dataset into a specific ocean region

    Args:
        df (Pandas DataFrame): Input CMAP formatted DataFrame (ST Index: time,lat,lon,<depth>)
    """

    data_gdf = geopandas_load_gpkg(data_df)
    region_gdf = load_gpkg_ocean_region(
        vs.spatial_data + "World_Seas_IHO_v1_simplified/World_Seas_Simplifed.gpkg"
    )
    classified_gdf = classify_gdf_with_gpkg_regions(data_gdf, region_gdf)
    region_set = classified_gdf_to_list(classified_gdf)
    print("Dataset matched to the following Regions: ", region_set)
    return region_set


def insert_into_tblDataset_Regions(region_set, dataset_name, server):
    dataset_ID = cmn.getDatasetID_DS_Name(dataset_name)
    region_ID_list = cmn.get_region_IDS(region_set)
    for region_ID in region_ID_list:
        query = (dataset_ID, region_ID)
        DB.lineInsert(
            server,
            "[opedia].[dbo].[tblDataset_Regions]",
            "(Dataset_ID, Region_ID)",
            query,
        )


def insert_into_tblCruise_Regions(region_set, server):
    region_ID_list = cmn.get_region_IDS(region_set)
    for region_ID in region_ID_list:
        query = (cruise_ID, region_ID)
        DB.lineInsert(
            server,
            "[opedia].[dbo].[tblCruise_Regions]",
            "(Cruise_ID, Region_ID)",
            query,
        )
