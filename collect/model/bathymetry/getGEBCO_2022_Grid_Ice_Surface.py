import sys
import os
import pandas as pd
import numpy as np
import glob
import xarray as xr
from zipfile import ZipFile

sys.path.append("ingest")

import vault_structure as vs


#https://www.gebco.net/data_and_products/gridded_bathymetry_data/
tbl = 'tblGEBCO_2022_Grid_Ice_Surface'
vs.leafStruc(vs.model+tbl)
e

raw_folder = vs.model+tbl+'/raw'
meta_folder = vs.model+tbl+'/metadata/'


zip_link = 'https://www.bodc.ac.uk/data/open_download/gebco/gebco_2022/zip/'


wget_str = f'wget --no-check-certificate "{zip_link}" -O "{raw_folder}/gebco_2022.zip"'
os.system(wget_str)
zip_list = glob.glob(raw_folder+'/*.zip')

with ZipFile(zip_list[0],"r") as zip_ref:
    zip_ref.extractall(raw_folder+'/')

nc_list = glob.glob(raw_folder+'/*.nc')

## View data attributes
x = xr.open_dataset(nc_list[0])
x.dims 
x.data_vars
x.attrs
for dv in x.data_vars:
    print(dv)

## Extract metadata
var_names = []
col_names = []
for varname, da in x.data_vars.items():
    try:
        v = da.attrs['long_name']
        fl_val = da.attrs['flag_values'].tolist()
        fl_def = da.attrs['flag_meanings']
        var_names.append(v)
        col_names.append(da.attrs['standard_name'])
    except:
        print(varname)

d1 = {'var_name':[], 'std_name':[], 'long_name':[], 'dtype':[], 'units':[], 'comment':[], 'flag_val':[], 'flag_def':[]}
df_varnames = pd.DataFrame(data=d1)

for varname, da in x.data_vars.items():
    dtype = da.data.dtype
    if 'flag_values' in da.attrs.keys():
        fl_val = da.attrs['flag_values'].tolist()
        fl_def = da.attrs['flag_meanings']
    else:
        fl_val = None
        fl_def = None
    if 'long_name' in da.attrs.keys():
        long_name = da.attrs['long_name']
    else:
        long_name = None
    if 'standard_name' in da.attrs.keys():
        std_name = da.attrs['standard_name']
    else:
        std_name = None      
    if 'comment' in da.attrs.keys():
        comment = da.attrs['comment']
    else:
        comment = None   
    if 'units' in da.attrs.keys():
        units = da.attrs['units']
    else:
        units = None          

    d1 = {'var_name':[varname], 'std_name':[std_name], 'long_name':[long_name], 'dtype':[dtype], 'units':[units], 'comment':[comment], 'flag_val':[fl_val], 'flag_def':[fl_def]}
    temp_df = pd.DataFrame(data=d1)
    df_varnames = df_varnames.append(temp_df, ignore_index=True)
df_ds = pd.DataFrame.from_dict(x.attrs.items())

writer = pd.ExcelWriter(meta_folder +'NetCDF_Metadata.xlsx', engine="xlsxwriter")
df_varnames.to_excel(writer, sheet_name="var_meta", index=False)
df_ds.to_excel(writer, sheet_name="dataset_meta", index=False)
writer.save()