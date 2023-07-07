from tqdm import tqdm
import pandas as pd
import numpy as np
import xarray as xr
import glob
import os
import sys


sys.path.append("ingest")
from ingest import vault_structure as vs
from ingest import common as cmn
from ingest import data_checks as dc
from ingest import data
from ingest import metadata
from ingest import stats

tbl = 'tblModis_POC_NRT'

base_folder = f'{vs.satellite}{tbl}/raw/'
nrt_folder = f'{vs.satellite}{tbl}/nrt/'


flist = np.sort(glob.glob(os.path.join(base_folder, f'*.nc')))

fil = flist[0]

for fil in tqdm(flist):
    fil_date = pd.to_datetime(
            fil.rsplit("/",1)[1].split(".",2)[1][0:8], format="%Y%m%d"
        ).strftime("%Y-%m-%d")   
    fil_name = os.path.basename(fil)
    path = f"{nrt_folder.split('vault/')[1]}{tbl}_{fil_date.replace('-','_')}.parquet"
    xdf = xr.open_dataset(fil, autoclose=True)
    df = data.netcdf4_to_pandas(fil, "poc")
    df["time"] = fil_date
    df["year"] = pd.to_datetime(df["time"]).dt.year
    df["month"] = pd.to_datetime(df["time"]).dt.month
    df["week"] = pd.to_datetime(df["time"]).dt.isocalendar().week
    df["dayofyear"] = pd.to_datetime(df["time"]).dt.dayofyear
    df = df[["time", "lat", "lon", "poc", "year", "month", "week", "dayofyear"]]
    df = dc.sort_values(df, dc.ST_columns(df))
    df.to_parquet(f"{nrt_folder}{tbl}_{fil_date.replace('-','_')}.parquet", index=False)      
    metadata.tblProcess_Queue_Process_Update(fil_name, path, tbl, 'Opedia', 'Rainier')
    xdf.close()


metadata.export_script_to_vault(tbl,'satellite','process/sat/MODIS/process_MODIS_POC_8day_cl1.py','process.txt')
## Fix time dtype
# from multiprocessing import Pool
# import glob
# tbl = "tblModis_POC_NRT"
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
# with Pool(processes=6) as pool:
#     pool.map(to_pq,  tqdm(flist))
#     pool.close() 
#     pool.join()