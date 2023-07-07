import os
import sys
import glob
import pandas as pd
import numpy as np
import xarray as xr
import shutil

sys.path.append("ingest")

import vault_structure as vs
import credentials as cr
import cruise
import data_checks as dc


tbl = 'tblTN412_Gradients5_uw_tsg'
# vs.leafStruc(f'{vs.cruise}{tbl}')

raw_folder = f'{vs.cruise}{tbl}/raw/'

# flist = glob.glob(raw_folder+'*.tar.gz')
# shutil.unpack_archive(flist[0], raw_folder)

cruise_name = 'TN412'
# vs.cruise_leaf_structure(f'{vs.r2r_cruise}{cruise_name}')

traj_folder = f'{vs.r2r_cruise}{cruise_name}/raw/'
nav_flist = glob.glob(raw_folder+'/samos/netcdf/*.nc')

fil = nav_flist[0]

fil = '/data/CMAP Data Submission Dropbox/Simons CMAP/vault/observation/in-situ/cruise/tblTN412_Gradients5_uw_tsg/raw//samos/netcdf/KTDQ_20230129v20001.nc'
fil = '/data/CMAP Data Submission Dropbox/Simons CMAP/vault/observation/in-situ/cruise/tblTN412_Gradients5_uw_tsg/raw//samos/netcdf/KTDQ_20230124v20001.nc'

x = xr.open_dataset(fil)
x.history
var_names = []
col_names = []
for varname, da in x.data_vars.items():
    print(f"#########{varname}")
    print(f"LONG NAME: {da.attrs['long_name']}")
    for dx in da.attrs.keys():
        print(dx)
    print(da)
    try:
        v = da.attrs['long_name']
        fl_val = da.attrs['flag_values'].tolist()
        fl_def = da.attrs['flag_meanings']
        var_names.append(v)
        col_names.append(da.attrs['standard_name'])
    except:
        print(varname)

d1 = {'var_name':[], 'long_name':[], 'dtype':[], 'units':[], 'original_units':[], 'data_precision':[], 'comment':[], 'instrument':[]}
df_varnames = pd.DataFrame(data=d1)

for varname, da in x.data_vars.items():
    dtype = da.data.dtype
    if 'long_name' in da.attrs.keys():
        long_name = da.attrs['long_name']
    else:
        long_name = None
    if 'original_units' in da.attrs.keys():
        original_units = da.attrs['original_units']
    else:
        original_units = None      
    if 'comment' in da.attrs.keys():
        comment = da.attrs['comment']
    else:
        comment = None   
    if 'units' in da.attrs.keys():
        units = da.attrs['units']
    else:
        units = None          
    if 'data_precision' in da.attrs.keys():
        data_precision = da.attrs['data_precision']
    else:
        data_precision = None     
    if 'instrument'  in da.attrs.keys():
        instrument = da.attrs['instrument']
    else:
        data_precision = None      
    d1 = {'var_name':[varname], 'long_name':[long_name], 'dtype':[dtype], 'units':[units], 'original_units':[original_units], 'data_precision':[data_precision], 'comment':[comment], 'instrument':[instrument]}
    temp_df = pd.DataFrame(data=d1)
    df_varnames = df_varnames.append(temp_df, ignore_index=True)

df_varnames.to_excel(raw_folder +f'{tbl}_Variables.xlsx', index=False)


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
    df = dc.mapTo180180(df)
    combined_df_list.append(df)

combined_df = pd.concat(combined_df_list, axis=0, ignore_index=True)
combined_df['time']=combined_df['time'].dt.strftime('%Y-%m-%dT%H:%M:%S')
combined_df.to_excel(f"{raw_folder}{tbl}_data.xlsx",index=False)
# df.to_excel(raw_folder+'test.xlsx',index=False)






# Gradients_5, TN412, R/V Thompson, Armbrust, Virginia
## Build cruise_metadata sheet
df_tblCruise, cruise_name = cruise.build_cruise_metadata_from_user_input(combined_df)
df_cruise_meta = df_tblCruise[['Nickname', 'Name', 'Ship_Name', 'Chief_Name']]
df_cruise_meta['Cruise_Series'] = 1

## Export cruise metadata and trajectory template
with pd.ExcelWriter(f"{traj_folder}{cruise_name}_cruise_meta_nav_data.xlsx") as writer:  
    df_cruise_meta.to_excel(writer, sheet_name='cruise_metadata', index=False)
    combined_df[['time','lat','lon']].to_excel(writer, sheet_name='cruise_trajectory', index=False)


