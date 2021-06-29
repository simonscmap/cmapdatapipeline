from cmapingest import vault_structure as vs
from cmapingest import common as cmn
from cmapingest import DB
from cmapingest import data 

import pandas as pd
import xarray as xr
import glob 
import numpy as np
from tqdm import tqdm 


NRT_wind_dir = vs.collected_data + "sat/CMEMS_NRT_Wind/"

flist = np.sort(glob.glob(NRT_wind_dir + '*.nc'))
for fil in tqdm(flist):
    xdf = xr.open_dataset(fil)
    df = xdf.to_dataframe().reset_index()
    df['time'] = pd.to_datetime(df['time'].astype(str),format="%Y-%m-%d %H:%M:%S")
    df['hour'] = df['time'].dt.hour
    df = data.add_day_week_month_year_clim(df)
    df = df[['time','lat','lon','wind_speed','eastward_wind','northward_wind','wind_stress','surface_downward_eastward_stress','surface_downward_northward_stress','wind_speed_rms','eastward_wind_rms','northward_wind_rms','wind_vector_curl','wind_vector_divergence','wind_stress_curl','wind_stress_divergence','sampling_length','surface_type','height','hour','year','month','week','dayofyear']]
    DB.toSQLbcp_wrapper(df, 'tblWind_NRT', "Rossby")


