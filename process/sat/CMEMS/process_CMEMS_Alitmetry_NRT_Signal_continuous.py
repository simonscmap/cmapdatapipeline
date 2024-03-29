"""
Author: Diana Haring <dharing@uw.edu>
Date: 04-21-2023

Script to run processing for continuous ingestion
"""

import sys
import os

import pandas as pd
import numpy as np
import xarray as xr
from tqdm import tqdm


sys.path.append("ingest")
sys.path.append("../../../ingest")

import vault_structure as vs
import DB 
import metadata
import data


tbl = 'tblAltimetry_NRT_Signal'
base_folder = f'{vs.satellite}{tbl}/raw/'
nrt_folder = f'{vs.satellite}{tbl}/nrt/'

os.chdir(nrt_folder)

## Pull list of newly downloaded files
qry = f"SELECT Original_Name from tblProcess_Queue WHERE Table_Name = '{tbl}' AND Path IS NULL AND Error_Str IS NULL"
flist_imp = DB.dbRead(qry,'Rainier')
flist = flist_imp['Original_Name'].str.strip().to_list()

## Compare original columns with oldest netCDF in vault
test_fil = base_folder+'nrt_global_allsat_phy_l4_20220624_20220630.nc'
tx =  xr.open_dataset(test_fil)
test_df = tx.to_dataframe().reset_index()
test_cols = test_df.columns.tolist()
## Compare data types with oldest parquet in vault 
test_fil = nrt_folder+'tblAltimetry_NRT_Signal_2022_06_24.parquet'
test_df = pd.read_parquet(test_fil)
test_dtype = test_df.dtypes.to_dict()

## Process and ingest Altimetry NetCDF
for fil in tqdm(flist):
    x = xr.open_dataset(base_folder+fil)
    df = x.to_dataframe().reset_index()
    x.close()
    if df.columns.to_list() != test_cols:
        print(f"Check columns in {fil}. New: {df.columns.to_list()}, Old: {test_cols}")
        sys.exit()    
    df = df.query('nv == 1')
    df = df[['time','latitude', 'longitude', 'sla', 'err_sla', 'ugosa', 'err_ugosa', 'vgosa', 'err_vgosa', 'adt', 'ugos', 'vgos', 'flag_ice']]
    df = df.sort_values(["time", "latitude","longitude"], ascending = (True, True,True))
    df.rename(columns={'latitude':'lat', 'longitude':'lon'}, inplace = True)
    df = data.add_day_week_month_year_clim(df)


    df['time'] = df['time'].astype('<M8[us]')
    for col in ['lat', 'lon']:
        df[col] = df[col].astype('float64')
    for col in ['year', 'month', 'dayofyear']:
        df[col] = df[col].astype('int64')


    if df.dtypes.to_dict() != test_dtype:
        print(f"Check data types in {fil}. New: {df.columns.to_list()}, Old: {test_cols}")        
        print(df.dtypes.to_dict())
        print(test_dtype)
        # metadata.tblProcess_Queue_Process_Update(fil, path, tbl, 'Opedia', 'Rainier','Dtype change')
        sys.exit()   
    fil_date = df['time'].max().strftime('%Y_%m_%d')
    path = f"{nrt_folder.split('vault/')[1]}{tbl}_{fil_date}.parquet"
    df.to_parquet(f"{nrt_folder}{tbl}_{fil_date}.parquet", index=False)
    metadata.tblProcess_Queue_Process_Update(fil, path, tbl, 'Opedia', 'Rainier')
    del df
    s3_str = f"aws s3 cp {tbl}_{fil_date}.parquet s3://cmap-vault/observation/remote/satellite/{tbl}/nrt/"
    os.system(s3_str)
    metadata.tblIngestion_Queue_Staged_Update(path, tbl, 'Opedia', 'Rainier')     

