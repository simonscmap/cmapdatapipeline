import sys
import os

import pandas as pd
import numpy as np
import xarray as xr
from multiprocessing import Pool


sys.path.append("ingest")
sys.path.append("../../../ingest")
import vault_structure as vs
import DB
import metadata


tbl = 'tblModis_CHL_NRT'
base_folder = f'{vs.satellite}{tbl}/raw/'
nrt_folder = f'{vs.satellite}{tbl}/nrt/'

os.chdir(nrt_folder)

## Pull list of newly downloaded files
qry = f"SELECT Original_Name from tblProcess_Queue WHERE Table_Name = '{tbl}' AND Path IS NULL AND Error_Str IS NULL"
flist_imp = DB.dbRead(qry,'Rainier')
flist = flist_imp['Original_Name'].str.strip().to_list()

## Compare original columns with oldest netCDF in vault
test_fil = base_folder+'AQUA_MODIS.20230125_20230201.L3m.8D.CHL.chlor_a.9km.NRT.nc'
tx =  xr.open_dataset(test_fil)
test_keys = list(tx.keys())
test_dims = list(tx.dims)
## Compare data types with oldest parquet in vault 
test_fil = nrt_folder+'tblModis_CHL_NRT_2023_01_25.parquet'
test_df = pd.read_parquet(test_fil)
test_dtype = test_df.dtypes.to_dict()

def proc_chl(fil):
    fil_date = pd.to_datetime(
            fil.split(".",2)[1][0:8], format="%Y%m%d"
        ).strftime("%Y_%m_%d")    
    x = xr.open_dataset(base_folder+fil)
    df_keys = list(x.keys())
    df_dims =  list(x.dims)
    if df_keys != test_keys or df_dims!= test_dims:
        print(f"Check columns in {fil}. New: {df.columns.to_list()}, Old: {list(x.keys())}")
        sys.exit()         
    chl = x.drop_dims(['rgb','eightbitcolor'])
    df = chl.to_dataframe().reset_index()
    fdate = pd.to_datetime(
            fil.split(".",2)[1][0:8], format="%Y%m%d"
        )
    df.insert(0,"time", fdate)  
    df["year"] = pd.to_datetime(df["time"]).dt.year
    df["month"] = pd.to_datetime(df["time"]).dt.month
    df["week"] = pd.to_datetime(df["time"]).dt.isocalendar().week
    df["dayofyear"] = pd.to_datetime(df["time"]).dt.dayofyear

    df['time'] = df['time'].astype('<M8[us]')
    for col in ['lat', 'lon']:
        df[col] = df[col].astype('float64')
    for col in ['year', 'month', 'dayofyear']:
        df[col] = df[col].astype('int64')
        
    if df.dtypes.to_dict() != test_dtype:
        print(f"Check data types in {fil}. New: {df.columns.to_list()}")
        print(df.dtypes.to_dict())
        print(test_dtype) 
        sys.exit()      
    fil_name = os.path.basename(fil)
    path = f"{nrt_folder.split('vault/')[1]}{tbl}_{fil_date}.parquet"
    df.to_parquet(f"{nrt_folder}{tbl}_{fil_date}.parquet", index=False)      
    metadata.tblProcess_Queue_Process_Update(fil_name, path, tbl, 'Opedia', 'Rainier')
    x.close()
    chl.close()
    s3_str = f"aws s3 cp {tbl}_{fil_date}.parquet s3://cmap-vault/observation/remote/satellite/{tbl}/nrt/"
    os.system(s3_str)
    metadata.tblIngestion_Queue_Staged_Update(path, tbl, 'Opedia', 'Rainier')    


with Pool(processes=2) as pool:
    pool.map(proc_chl,  flist)
    pool.close() 
    pool.join()

# metadata.export_script_to_vault(tbl,'satellite','process/sat/MODIS/process_MODIS_CHL_8day_NRT.py','process.txt')

