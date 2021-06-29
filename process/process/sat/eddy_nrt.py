"""flow orientation of eddies -1 is Cyclonic and 1 is Anticyclonic"""
from tqdm import tqdm
import pandas as pd
import numpy as np
import vaex
import h5py
import dask
import xarray as xr

from cmapingest import vault_structure as vs
from cmapingest import common as cmn
from cmapingest import data

""" Cleans and Transforms the AVISO+ Eddy NRT data into the CMAP format for ingestion"""


eddy_data_path = vs.collected_data + "AVISO_eddy_NRT/"


cyclonic_eddy = (
    xr.open_dataset(
        eddy_data_path + "eddy_trajectory_nrt_3.0exp_cyclonic_20180101_20200712.nc"
    )
    .sel(NbSample=0)
    .to_dataframe()
)
cyclonic_eddy["eddy_polarity"] = -1
anticyclonic_eddy = (
    xr.open_dataset(
        eddy_data_path + "eddy_trajectory_nrt_3.0exp_anticyclonic_20180101_20200712.nc"
    )
    .sel(NbSample=0)
    .to_dataframe()
)
anticyclonic_eddy["eddy_polarity"] = 1

combined_eddy = pd.concat([cyclonic_eddy, anticyclonic_eddy])

combined_eddy["eddy_age"] = ""

combined_eddy["year"] = combined_eddy["time"].dt.year
combined_eddy["month"] = combined_eddy["time"].dt.month
combined_eddy["week"] = combined_eddy["time"].dt.weekofyear
combined_eddy["dayofyear"] = combined_eddy["time"].dt.dayofyear


for track in tqdm(combined_eddy["track"].unique()):
    combined_eddy["eddy_age"][combined_eddy["track"] == track] = (
        combined_eddy[combined_eddy["track"] == track]["time"].max()
        - combined_eddy[combined_eddy["track"] == track]["time"].min()
    ).days

combined_eddy = combined_eddy[
    [
        "time",
        "latitude",
        "longitude",
        "eddy_polarity",
        "eddy_age",
        "amplitude",
        "effective_contour_height",
        "effective_contour_latitude",
        "effective_contour_longitude",
        "effective_contour_shape_error",
        "effective_radius",
        "inner_contour_height",
        "latitude_max",
        "longitude_max",
        "num_contours",
        "observation_flag",
        "observation_number",
        "speed_average",
        "speed_contour_height",
        "speed_contour_latitude",
        "speed_contour_longitude",
        "speed_contour_shape_error",
        "speed_radius",
        "track",
        "uavg_profile",
        "year",
        "month",
        "week",
        "dayofyear",
    ]
]
combined_eddy.rename(columns={"latitude": "lat", "longitude": "lon"}, inplace=True)
combined_eddy = data.clean_data_df(combined_eddy)
combined_eddy = data.mapTo180180(combined_eddy)


def test_eddy_age_calc(combined_eddy):
    randtrack = combined_eddy.track.sample(1).iloc[0]
    test_eddy = combined_eddy[combined_eddy["track"] == randtrack]
    test_eddy_age = test_eddy["eddy_age"].iloc[0]
    comp_eddy_age = (test_eddy["time"].max() - test_eddy["time"].min()).days
    assert comp_eddy_age == test_eddy_age, "eddy ages do not match"


test_eddy_age_calc(combined_eddy)


def write_metadata():
    vars_meta = (
        xr.open_dataset(
            eddy_data_path + "eddy_trajectory_nrt_3.0exp_cyclonic_20180101_20200712.nc"
        )
        .sel(NbSample=0)
        .var
    )
    attrs = (
        xr.open_dataset(
            eddy_data_path + "eddy_trajectory_nrt_3.0exp_cyclonic_20180101_20200712.nc"
        )
        .sel(NbSample=0)
        .attrs
    )
    f = open(eddy_data_path + "eddy_nrt_metadata.txt", "a")
    f.write(str(vars_meta))
    f.write(str("\n"))
    f.write(str(attrs))
    f.close()


write_metadata()
combined_eddy.to_hdf(eddy_data_path + "eddy_NRT.h5", key="df", mode="w")
combined_eddy.to_csv(vs.staging + "data/" + "eddy_NRT.csv", sep=",", index=False)
