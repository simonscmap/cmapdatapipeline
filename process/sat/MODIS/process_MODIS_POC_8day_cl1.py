from tqdm import tqdm
import pandas as pd
import numpy as np
import xarray as xr
import glob
import os
import sys
import shutil

sys.path.append("ingest")
from ingest import vault_structure as vs
from ingest import common as cmn
from ingest import data_checks as dc
from ingest import data
from ingest import metadata
from ingest import stats

tbl = 'tblModis_POC_cl1'

base_folder = f'{vs.satellite}{tbl}/raw/'
rep_folder = f'{vs.satellite}{tbl}/rep/'
code_folder= f'{vs.satellite}{tbl}/code/'
meta_folder= f'{vs.satellite}{tbl}/metadata/'

flist = np.sort(glob.glob(os.path.join(base_folder, f'*.nc')))



for fil in tqdm(flist):
    print(fil)
    fil_date = pd.to_datetime(
            fil.rsplit("/",1)[1].split(".",2)[1][0:8], format="%Y%m%d"
        ).strftime("%Y_%m_%d")   
    fil_name = os.path.basename(fil)
    path = f"{rep_folder.split('vault/')[1]}{tbl}_{fil_date}.parquet"
    xdf = xr.open_dataset(fil, autoclose=True)
    df = data.netcdf4_to_pandas(fil, "poc")
    df["time"] = pd.to_datetime(
            fil.rsplit("/",1)[1].split(".",2)[1][0:8], format="%Y%m%d"
        )
    df["year"] = pd.to_datetime(df["time"]).dt.year
    df["month"] = pd.to_datetime(df["time"]).dt.month
    df["week"] = pd.to_datetime(df["time"]).dt.isocalendar().week
    df["dayofyear"] = pd.to_datetime(df["time"]).dt.dayofyear
    df = df[["time", "lat", "lon", "poc", "year", "month", "week", "dayofyear"]]
    df = data.sort_values(df, data.ST_columns(df))
    null_check = dc.check_df_nulls(df, tbl, 'Rossby')
    unique_check = dc.check_df_constraint(df, tbl, 'Rossby')
    if null_check == 0 and unique_check == 0:
        df.to_parquet(f"{rep_folder}{tbl}_{fil_date}.parquet", index=False)      
        metadata.tblProcess_Queue_Process_Update(fil_name, path, tbl, 'Opedia', 'Rainier')
    else:
        metadata.tblProcess_Queue_Download_Insert(f"{fil_date}", tbl, 'Opedia', 'Rainier','Null or unique constraint issue')
    xdf.close()


metadata.export_script_to_vault(tbl,'satellite','process/sat/MODIS/process_MODIS_POC_8day_cl1.py','process.txt')

## Backfill to on prem
flist = glob.glob(rep_folder+'*.parquet')
server = 'rossby'
for fil in tqdm(flist):
    df = pd.read_parquet(fil)
    DB.toSQLbcp_wrapper(df,tbl,server)

## Fix time dtype
from multiprocessing import Pool
import glob
tbl = "tblModis_POC_cl1"
base_folder = f'{vs.satellite}{tbl}/raw/'
rep_folder = f'{vs.satellite}{tbl}/rep/'
flist = glob.glob(rep_folder+'*.parquet')
len(flist)
flist[0]
def to_pq(fil):
    try:
        df = pd.read_parquet(fil)
        df['time'] = pd.to_datetime(df['time'])
        df.to_parquet(fil, index=False)
    except Exception as e:        
        print(str(e))        
with Pool(processes=10) as pool:
    pool.map(to_pq, flist)
    pool.close() 
    pool.join()    