from tqdm import tqdm
import pandas as pd
import numpy as np
import dropbox
import xarray as xr
import glob
import os

from cmapingest import vault_structure as vs
from cmapingest import common as cmn
from cmapingest import data
from cmapingest import transfer
from cmapingest import stats


# startdate = "2010001" #YYYYDDD
startdate = "2013134"  # YYYYDDD

enddate = "2014365"  # YYYYDDD
tableName = "tblModis_PAR"
branch_path = cmn.vault_struct_retrieval("satellite")

modis_par_path = vs.collected_data + "MODIS_PAR_daily_data/"
modis_flist = glob.glob(modis_par_path + "*.nc")
modis_flist_base = [os.path.basename(filename) for filename in modis_flist]
modis_file_date_list = np.sort(
    [i.strip(".L3m_DAY_PAR_par_9km.nc") for i in modis_flist_base]
)

files_in_range = []
for i in modis_flist_base:
    strpted_time = i.replace(".L3m_DAY_PAR_par_9km.nc", "").replace("A", "")
    zfill_time = strpted_time[:4] + strpted_time[4:].zfill(3)
    fdate = pd.to_datetime(zfill_time, format="%Y%j")
    if (
        pd.to_datetime(startdate, format="%Y%j")
        <= fdate
        <= pd.to_datetime(enddate, format="%Y%j")
    ):
        files_in_range.append(i)


vs.makedir(vs.satellite + "tblModis_PAR")
for fil in tqdm(files_in_range):
    timecol = pd.to_datetime(
        fil.replace(".L3m_DAY_PAR_par_9km.nc", "").replace("A", ""), format="%Y%j"
    ).strftime("%Y-%m-%d")
    xdf = xr.open_dataset(modis_par_path + fil)
    df = data.netcdf4_to_pandas(modis_par_path + fil, "par")
    df["time"] = timecol
    df["year"] = pd.to_datetime(df["time"]).dt.year
    df["month"] = pd.to_datetime(df["time"]).dt.month
    df["week"] = pd.to_datetime(df["time"]).dt.weekofyear
    df["dayofyear"] = pd.to_datetime(df["time"]).dt.dayofyear

    df = df[["time", "lat", "lon", "par", "year", "month", "week", "dayofyear"]]
    stats.buildLarge_Stats(
        df, timecol, "tblModis_PAR", "satellite", transfer_flag="dropbox"
    )

    df = data.clean_data_df(df)
    # df.to_hdf(vs.dbx_download_transfer +fil.replace('.nc','.h5'),key='tblModis_PAR')
    df.to_parquet(vs.dbx_download_transfer + fil.replace(".nc", ".parquet"))

    transfer.dropbox_file_transfer(
        vs.dbx_download_transfer + fil.replace(".nc", ".parquet"),
        branch_path.split("Vault")[1]
        + tableName
        + "/rep/"
        + fil.replace(".nc", ".parquet"),
    )
    os.remove(vs.dbx_download_transfer + fil.replace(".nc", ".parquet"))
