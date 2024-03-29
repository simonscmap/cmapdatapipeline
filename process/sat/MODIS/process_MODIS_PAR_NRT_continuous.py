import pandas as pd
import numpy as np
import xarray as xr
import os
import sys
from multiprocessing import Pool

sys.path.append("ingest")
sys.path.append("../../../ingest")
import vault_structure as vs
import DB
import metadata
import data_checks as dc

tbl = "tblModis_PAR_NRT"
base_folder = f'{vs.satellite}{tbl}/raw/'
nrt_folder = f'{vs.satellite}{tbl}/nrt/'

os.chdir(nrt_folder)
## Pull list of newly downloaded files
qry = f"SELECT Original_Name from tblProcess_Queue WHERE Table_Name = '{tbl}' AND Path IS NULL AND Error_Str IS NULL"
flist_imp = DB.dbRead(qry,'Rainier')
flist = flist_imp['Original_Name'].str.strip().to_list()

## Compare original columns with oldest netCDF in vault
test_fil = base_folder+'AQUA_MODIS.20230418.L3m.DAY.PAR.par.9km.NRT.nc'
tx =  xr.open_dataset(test_fil)
test_keys = list(tx.keys())
test_dims = list(tx.dims)
## Compare data types with oldest parquet in vault 
test_fil = nrt_folder+'tblModis_PAR_NRT_2023_03_01.parquet'
test_df = pd.read_parquet(test_fil)
test_dtype = test_df.dtypes.to_dict()

#https://oceandata.sci.gsfc.nasa.gov/cgi/getfile/AQUA_MODIS.20020705.L3m.DAY.PAR.par.9km.nc
#fil = flist[0]
def proc_par(fil):
    timecol = pd.to_datetime(
            fil.split(".",2)[1][0:8], format="%Y%m%d"
        ).strftime("%Y_%m_%d")   
    xdf = xr.open_dataset(base_folder+fil)
    df_keys = list(xdf.keys())
    df_dims =  list(xdf.dims)
    if df_keys != test_keys or df_dims!= test_dims:
        print(f"Check columns in {fil}. New: {df.columns.to_list()}, Old: {list(xdf.keys())}")
        sys.exit()    
    ## Option 1
    par = xdf.drop_dims(['rgb','eightbitcolor'])
    df = par.to_dataframe().reset_index()
    xdf.close()
    par.close()
    ## Option 2. Either option works
    # df = data.netcdf4_to_pandas(base_folder + fil, "par")
    df["time"] = pd.to_datetime(
            fil.split(".",2)[1][0:8], format="%Y%m%d"
        )
    df["year"] = pd.to_datetime(df["time"]).dt.year
    df["month"] = pd.to_datetime(df["time"]).dt.month
    df["week"] = pd.to_datetime(df["time"]).dt.isocalendar().week
    df["dayofyear"] = pd.to_datetime(df["time"]).dt.dayofyear
    df = df[["time", "lat", "lon", "par", "year", "month", "week", "dayofyear"]]

    df['time'] = df['time'].astype('<M8[us]')
    for col in ['lat', 'lon']:
        df[col] = df[col].astype('float64')
    for col in ['year', 'month', 'dayofyear']:
        df[col] = df[col].astype('int64')

        
    if df.dtypes.to_dict() != test_dtype:
        print(f"Check data types in {fil}.")
        print(df.dtypes.to_dict())
        print(test_dtype)             
        sys.exit()     
    df.to_parquet(f"{nrt_folder}{tbl}_{timecol}.parquet", index=False)      
    path = f"{nrt_folder.split('vault/')[1]}{tbl}_{timecol}.parquet"
    metadata.tblProcess_Queue_Process_Update(fil, path, tbl, 'Opedia', 'Rainier')
    s3_str = f"aws s3 cp {tbl}_{timecol}.parquet s3://cmap-vault/observation/remote/satellite/{tbl}/nrt/"
    os.system(s3_str)
    metadata.tblIngestion_Queue_Staged_Update(path, tbl, 'Opedia', 'Rainier')    



with Pool(processes=2) as pool:
    pool.map(proc_par,  flist)
    pool.close() 
    pool.join()


# ## Fix time dtype
# from multiprocessing import Pool
# import glob
# tbl = "tblModis_PAR_NRT"
# base_folder = f'{vs.satellite}{tbl}/raw/'
# nrt_folder = f'{vs.satellite}{tbl}/nrt/'
# flist = glob.glob(nrt_folder+'*.parquet')
# len(flist)
# flist[0]
# def to_pq(fil):
#     try:
#         df = pd.read_parquet(fil)
#         df['time'] = pd.to_datetime(df['time'])
#         df.to_parquet(fil, index=False)
#     except Exception as e:        
#         print(str(e))        
# with Pool(processes=4) as pool:
#     pool.map(to_pq,  tqdm(flist))
#     pool.close() 
#     pool.join()