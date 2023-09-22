import os
import sys
import glob
import pandas as pd
import numpy as np
import xarray as xr

sys.path.append("ingest")

import vault_structure as vs
import data

cruise_name = 'AE1318'
# vs.cruise_leaf_structure(f'{vs.r2r_cruise}{cruise_name}')
raw_folder = f'{vs.r2r_cruise}{cruise_name}/raw/'

traj_folder = f'{vs.r2r_cruise}{cruise_name}/raw/'
nav_flist = glob.glob(raw_folder+'/samos/netcdf/*.nc')


x = xr.open_dataset(fil)
x.history
var_names = []
col_names = []
for varname, da in x.data_vars.items():
    print(f"#########{varname}")
    print(f"LONG NAME: {da.attrs['long_name']}")
    for dx in da.attrs.keys():
        print(dx)


combined_df_list = []
for fil in nav_flist:
    ## remove -999 fill values
    x = xr.open_dataset(fil, mask_and_scale=True)
    df = x.to_dataframe().reset_index()
    for col, dtype in df.dtypes.items():
        if dtype == 'object':
            print(col)
            df[col] = df[col].str.decode("utf-8").fillna(df[col])    
    df = df.loc[df['h_num']==49]
    df = df[['time', 'lat', 'lon','TS', 'SSPS', 'CNDC']]
    df = data.mapTo180180(df)
    combined_df_list.append(df)

combined_df = pd.concat(combined_df_list, axis=0, ignore_index=True)
combined_df['time']=combined_df['time'].dt.strftime('%Y-%m-%dT%H:%M:%S')
combined_df.to_excel(f"{raw_folder}{cruise_name}_data.xlsx",index=False)        