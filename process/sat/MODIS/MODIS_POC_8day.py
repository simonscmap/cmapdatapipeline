from tqdm import tqdm
import pandas as pd
import numpy as np
import vaex
import h5py
import dask
import xarray as xr
import glob
import os

from cmapingest import vault_structure as vs
from cmapingest import common as cmn
from cmapingest import data
from cmapingest import DB
from cmapingest import stats

startdate = "2004009"  # YYYYDDD
# startdate = "2019185" #YYYYDDD

enddate = "2019353"  # YYYYDDD

modis_poc_path = vs.collected_data + "MODIS_POC_8_day_data/"
modis_flist = glob.glob(modis_poc_path + "*.nc")
modis_flist_base = [os.path.basename(filename) for filename in modis_flist]
modis_file_date_list = np.sort(
    [i.strip(".nc") for i in [i.strip("MODIS_POC_8_day_") for i in modis_flist_base]]
)
modis_file_date_DT = [pd.to_datetime(i, format="%Y%j") for i in modis_file_date_list]

dates_in_range = [
    i
    for i in modis_file_date_DT
    if pd.to_datetime(startdate, format="%Y%j")
    < i
    < pd.to_datetime(enddate, format="%Y%j")
]


files_in_range = []
for i in modis_flist_base:
    fdate = pd.to_datetime(i.strip("MODIS_POC_8_day_").strip(".nc"), format="%Y%j")
    if (
        pd.to_datetime(startdate, format="%Y%j")
        < fdate
        < pd.to_datetime(enddate, format="%Y%j")
    ):
        files_in_range.append(i)

vs.leafStruc(vs.satellite + "tblModis_POC")
for fil in tqdm(files_in_range):
    print(fil)
    timecol = pd.to_datetime(
        i.strip("MODIS_POC_8_day_").strip(".nc"), format="%Y%j"
    ).strftime("%Y-%m-%d")
    xdf = xr.open_dataset(modis_poc_path + fil, autoclose=true)
    df = data.netcdf4_to_pandas(modis_poc_path + fil, "poc")
    df["time"] = pd.to_datetime(
        fil.strip("MODIS_POC_8_day_").strip(".nc"), format="%Y%j"
    ).strftime("%Y-%m-%d")
    df["year"] = pd.to_datetime(df["time"]).dt.year
    df["month"] = pd.to_datetime(df["time"]).dt.month
    df["week"] = pd.to_datetime(df["time"]).dt.weekofyear
    df["dayofyear"] = pd.to_datetime(df["time"]).dt.dayofyear
    df = data.clean_data_df(df)
    df = df[["time", "lat", "lon", "poc", "year", "month", "week", "dayofyear"]]
    df.to_csv(
        vs.satellite + "tblModis_POC/rep/" + fil.strip(".nc") + ".csv",
        sep=",",
        index=False,
    )
    DB.toSQLbcp(
        vs.satellite + "tblModis_POC/rep/" + fil.strip(".nc") + ".csv",
        "tblModis_POC",
        "Rainier",
    )
    stats.buildLarge_Stats(
        df,
        df.time.iloc[0] + "_8_day",
        "tblModis_POC",
        "satellite",
        transfer_flag="normal",
    )
    xdf.close()


""" every file:
    1. import into xarray
    2. flatten to pandas df
    3. clean pandas df
    4. export pandas to /vault"""
