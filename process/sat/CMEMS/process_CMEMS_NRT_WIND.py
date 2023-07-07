import sys
sys.path.append("cmapdata/ingest")

import vault_structure as vs
import common as cmn
import DB
import data

import pandas as pd
import xarray as xr
import glob
import numpy as np
from tqdm import tqdm

# fil = vs.download_transfer+'cmems_obs-oc_glo_bgc-plankton_nrt_l4-multi-4km_P1M_1671036353108.nc'

NRT_wind_dir = vs.collected_data + "sat/CMEMS_NRT_Wind/"

flist = np.sort(glob.glob(NRT_wind_dir + "*.nc"))
fil = flist[0]
for fil in tqdm(flist):
    xdf = xr.open_dataset(fil)
    df = xdf.to_dataframe().reset_index()
    df["time"] = pd.to_datetime(df["time"].astype(str), format="%Y-%m-%d %H:%M:%S")
    df["hour"] = df["time"].dt.hour
    df = data.add_day_week_month_year_clim(df)
    df = df[
        [
            "time",
            "lat",
            "lon",
            "wind_speed",
            "eastward_wind",
            "northward_wind",
            "wind_stress",
            "surface_downward_eastward_stress",
            "surface_downward_northward_stress",
            "wind_speed_rms",
            "eastward_wind_rms",
            "northward_wind_rms",
            "wind_vector_curl",
            "wind_vector_divergence",
            "wind_stress_curl",
            "wind_stress_divergence",
            "sampling_length",
            "surface_type",
            "height",
            "hour",
            "year",
            "month",
            "week",
            "dayofyear",
        ]
    ]
    DB.toSQLbcp_wrapper(df, "tblWind_NRT", "Rossby")
    df.to_csv(
        vs.satellite + "tblWind_NRT/nrt/" + fil.strip(".nc") + ".csv",
        sep=",",
        index=False)
