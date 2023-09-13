"""
Author: Diana Haring <dharing@uw.edu>
Date: 02-14-2023

Script to run processing for continuous ingestion
"""

import sys
import os

import pandas as pd
import numpy as np
import xarray as xr
from multiprocessing import Pool

sys.path.append("ingest")
sys.path.append("../../../ingest")
import vault_structure as vs
import metadata 
import DB
import data_checks as dc

tbl = 'tblWind_NRT_hourly'
sat_dir = f'{vs.satellite}{tbl}/raw/'
nrt_dir = f'{vs.satellite}{tbl}/nrt/'

os.chdir(nrt_dir)

## Pull list of newly downloaded files
qry = f"SELECT Original_Name from tblProcess_Queue WHERE Table_Name = '{tbl}' AND Path IS NULL AND Error_Str IS NULL"
flist_imp = DB.dbRead(qry,'Rainier')
flist = flist_imp['Original_Name'].str.strip().to_list()

## Compare original columns with oldest netCDF in vault
test_fil = sat_dir+'cmems_obs-wind_glo_phy_nrt_l4_0.125deg_PT1H_2020070100_R20200630T12_12.nc'
tx =  xr.open_dataset(test_fil)
test_df = tx.to_dataframe().reset_index()
test_df_hash = pd.util.hash_pandas_object(test_df).sum()
test_cols = test_df.columns.tolist()
## Compare hash of newly downloaded data from same date as oldest netCDF
test_fil = sat_dir+'datacheck_2020070100.nc'
txd =  xr.open_dataset(test_fil)
test_data_df = txd.to_dataframe().reset_index()
test_data_hash = pd.util.hash_pandas_object(test_data_df).sum()
if test_df_hash !=test_data_hash:
    print("####### Data was reprocessed")
    sys.exit()
    
## Compare data types with oldest parquet in vault 
test_fil = nrt_dir+'cmems_obs-wind_glo_phy_nrt_l4_0.125deg_PT1H_2020070100_R20200630T12_12.parquet'
test_df = pd.read_parquet(test_fil)
test_dtype = test_df.dtypes.to_dict()


def proc_wind(fil):
    """Process and ingest wind NetCDF"""
    ## Equal dataframes with and without Mask and scale 
    x = xr.open_dataset(sat_dir+fil, mask_and_scale=True )
    df = x.to_dataframe().reset_index()
    if df.columns.to_list() != test_cols:
        print(f"Check columns in {fil}. New: {df.columns.to_list()}, Old: {test_cols}")
        metadata.tblProcess_Queue_Process_Update(fil, path, tbl, 'Opedia', 'Rainier','New columns')
        sys.exit()
    x.close()
    df['hour'] = df['time'].dt.hour
    df['time'] = df['time'].dt.date
    cols = df.columns.tolist()
    df = df[['time', 'lat', 'lon','hour'] + cols[3:30]]
    df = df.sort_values(["time", "lat","lon","hour"], ascending = (True, True,True,True))
    df['number_of_observations'] = df['number_of_observations'].astype('Int64')
    df['number_of_observations_divcurl'] = df['number_of_observations_divcurl'].astype('Int64')
    df = dc.add_day_week_month_year_clim(df)
    if df.dtypes.to_dict() != test_dtype:
        print(f"Check data types in {fil}. New: {df.columns.to_list()}, Old: {test_cols}")
        metadata.tblProcess_Queue_Process_Update(fil, path, tbl, 'Opedia', 'Rainier','Dtype change')
        sys.exit()   
    ### Keeping original file name to match previous ingestion   
    # fil_date = df['time'].max().strftime('%Y_%m_%d_%H')
    # path = f"{nrt_dir.split('vault/')[1]}{tbl}_{fil_date}.parquet"
    # df.to_parquet(f'{nrt_dir}{tbl}_{fil_date}.parquet', index=False)  
    path = f"{nrt_dir.split('vault/')[1]}{fil.split('.nc')[0]}.parquet"
    df.to_parquet(f'{nrt_dir}{fil.split(".nc")[0]}.parquet', index=False)      
    metadata.tblProcess_Queue_Process_Update(fil, path, tbl, 'Opedia', 'Rainier')
    s3_str = f"aws s3 cp {fil.split('.nc')[0]}.parquet s3://cmap-vault/observation/remote/satellite/tblWind_NRT_hourly/nrt/"
    os.system(s3_str)
    metadata.tblIngestion_Queue_Staged_Update(path, tbl, 'Opedia', 'Rainier')           

with Pool(processes=4) as pool:
    pool.map(proc_wind,  flist)
    pool.close() 
    pool.join()

