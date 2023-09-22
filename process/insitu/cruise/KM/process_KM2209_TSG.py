import sys
import os
import pandas as pd
import numpy as np
import xarray as xr

import vault_structure as vs
import common as cmn
import data

cruise_name = 'KM2209'
# vs.cruise_leaf_structure(vs.r2r_cruise+cruise_name)

### Data downloaded from SAMOS: https://samos.coaps.fsu.edu/html/cruise_data_availability.php?submit=search&sort=overall&data=1&cruises%5b%5d=KM2209
cruise_data = f"{vs.r2r_cruise}{cruise_name}/raw/samos_KM2209/samos/netcdf"

flist = []
for root, dirs, files in os.walk(cruise_data):
    for f in files:
        flist.append(os.path.join(root, f))
df_import = pd.DataFrame()
for fil in flist:
    x = xr.open_dataset(fil)
    df_x = x.to_dataframe().reset_index()
    df = df_x[['time','lat','lon']]
    df = data.mapTo180180(df)
    df.shape
    df.index = pd.to_datetime(df.time)
    rs_df = df.resample('1min').mean()
    rs_df = rs_df.dropna()
    rs_df.reset_index(inplace=True)
    rs_df['time'] = rs_df['time'].dt.strftime('%Y-%m-%dT%H:%M:%S')
    rs_df.shape
    df_import = df_import.append(rs_df)

df_import = data.sort_values(df_import, ['time','lat','lon'])
## Build cruise_metadata sheet
cruise_meta = dict(Nickname='Project: SCOPE', Name=cruise_name, Ship_Name='Kilo Moana', Chief_Name='White, Angelicque',Cruise_Series='')
df_cruise_meta = pd.DataFrame(cruise_meta, index=[0])

with pd.ExcelWriter(f"{vs.r2r_cruise}{cruise_name}/raw/{cruise_name}_cruise_meta_nav_data.xlsx") as writer:  
    df_cruise_meta.to_excel(writer, sheet_name='cruise_metadata', index=False)
    df_import.to_excel(writer, sheet_name='cruise_trajectory', index=False)
