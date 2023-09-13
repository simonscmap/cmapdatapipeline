from tqdm import tqdm
import pandas as pd
import numpy as np
import xarray as xr
import os
import sys


sys.path.append("ingest")
sys.path.append("../../../ingest")
import vault_structure as vs
import DB
import metadata
import data_checks as dc
import data

tbl = 'tblModis_POC_NRT'

base_folder = f'{vs.satellite}{tbl}/raw/'
nrt_folder = f'{vs.satellite}{tbl}/nrt/'


os.chdir(nrt_folder)

## Pull list of newly downloaded files
qry = f"SELECT Original_Name from tblProcess_Queue WHERE Table_Name = '{tbl}' AND Path IS NULL AND Error_Str IS NULL"
flist_imp = DB.dbRead(qry,'Rainier')
flist = flist_imp['Original_Name'].str.strip().to_list()

## Compare original columns with oldest netCDF in vault
test_fil = base_folder+'AQUA_MODIS.20230125_20230201.L3m.8D.POC.poc.9km.NRT.nc'
tx =  xr.open_dataset(test_fil)
test_keys = list(tx.keys())
test_dims = list(tx.dims)
## Compare data types with oldest parquet in vault 
test_fil = nrt_folder+'tblModis_POC_NRT_2023_01_25.parquet'
test_df = pd.read_parquet(test_fil)
test_dtype = test_df.dtypes.to_dict()

for fil in tqdm(flist):
    fil_date = pd.to_datetime(
            fil.split(".",2)[1][0:8], format="%Y%m%d"
        ).strftime("%Y_%m_%d")   
    fil_name = os.path.basename(fil)
    path = f"{nrt_folder.split('vault/')[1]}{tbl}_{fil_date}.parquet"
    xdf = xr.open_dataset(base_folder+fil)
    df_keys = list(xdf.keys())
    df_dims =  list(xdf.dims)
    if df_keys != test_keys or df_dims!= test_dims:
        print(f"Check columns in {fil}. New: {df.columns.to_list()}, Old: {list(xdf.keys())}")
        sys.exit()   
    df = data.netcdf4_to_pandas(base_folder+fil, "poc")
    df["time"] = pd.to_datetime(
            fil.split(".",2)[1][0:8], format="%Y%m%d"
        )
    df["year"] = pd.to_datetime(df["time"]).dt.year
    df["month"] = pd.to_datetime(df["time"]).dt.month
    df["week"] = pd.to_datetime(df["time"]).dt.isocalendar().week
    df["dayofyear"] = pd.to_datetime(df["time"]).dt.dayofyear
    df = df[["time", "lat", "lon", "poc", "year", "month", "week", "dayofyear"]]
    if df.dtypes.to_dict() != test_dtype:
        print(f"Check data types in {fil}. New: {df.columns.to_list()}")
        sys.exit()       
    df = dc.sort_values(df, dc.ST_columns(df))
    df.to_parquet(f"{nrt_folder}{tbl}_{fil_date}.parquet", index=False)      
    metadata.tblProcess_Queue_Process_Update(fil_name, path, tbl, 'Opedia', 'Rainier')
    xdf.close()
    s3_str = f"aws s3 cp {tbl}_{fil_date}.parquet s3://cmap-vault/observation/remote/satellite/{tbl}/nrt/"
    os.system(s3_str)
    metadata.tblIngestion_Queue_Staged_Update(path, tbl, 'Opedia', 'Rainier')      


# metadata.export_script_to_vault(tbl,'satellite','process/sat/MODIS/process_MODIS_POC_8day_cl1.py','process.txt')
