import sys
import os

import pandas as pd
import numpy as np
import xarray as xr


sys.path.append("ingest")
import vault_structure as vs
import credentials as cr
import DB 
import data_checks as dc


tbl_s = 'tblGeotraces_Sensor'
ns = f'{vs.cruise}{tbl_s}/raw/GEOTRACES_IDP2021_Seawater_Sensor_Data_v1.nc' 

xs = xr.open_dataset(ns)
xs.dims


tbl_w = 'tblGeotraces_Seawater'
nw = f'{vs.cruise}{tbl_w}/raw/GEOTRACES_IDP2021_Seawater_Discrete_Sample_Data_v1.nc' 

xw = xr.open_dataset(nw)
xw.dims

cruise_groups = xw.groupby("metavar5").groups
xw.metavar5.values.tolist()

s_no_samples = xs.drop_dims('N_SAMPLES')
w_no_samples = xw.drop_dims('N_SAMPLES')

s_no_samples.data_vars
w_no_samples.data_vars

df_s = s_no_samples.to_dataframe().reset_index()
df_w = w_no_samples.to_dataframe().reset_index()

# df_w['lat_round'] = df_w['latitude'].round(3)
# df_w['lon_round'] = df_w['longitude'].round(3)

# df_s['lat_round'] = df_s['latitude'].round(3)
# df_s['lon_round'] = df_s['longitude'].round(3)


df_w['lat_round'] = df_w['latitude'].round(2)
df_w['lon_round'] = df_w['longitude'].round(2)

df_s['lat_round'] = df_s['latitude'].round(2)
df_s['lon_round'] = df_s['longitude'].round(2)

df_s.shape
df_w.shape

df_join = pd.merge(df_s, df_w, how ='outer', on = ['lat_round','lon_round','metavar1','metavar5'])

df_join.date_time_x.count() #3646, 3554, 3523
df_join.date_time_y.count() #1658, 926, 428

df_join.to_excel(vs.download_transfer +'GeotracesSensorSeawater_Join.xlsx', index=False)

print(df_s.columns)