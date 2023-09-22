from tqdm import tqdm
import pandas as pd
import numpy as np
import xarray as xr
import glob
import os
import sys


sys.path.append("cmapdata/ingest")
from ingest import vault_structure as vs
from ingest import common as cmn
from ingest import data
from ingest import metadata


tbl = "tblModis_PAR_cl1"
base_folder = f'{vs.satellite}{tbl}/raw/'
rep_folder = f'{vs.satellite}{tbl}/rep/'
code_folder= f'{vs.satellite}{tbl}/code/'

flist = np.sort(glob.glob(os.path.join(base_folder, f'*.nc')))

#https://oceandata.sci.gsfc.nasa.gov/cgi/getfile/AQUA_MODIS.20020705.L3m.DAY.PAR.par.9km.nc

len(flist)
fil = flist[0]
for fil in tqdm(flist):
    fil_name = os.path.basename(fil)
    timecol = pd.to_datetime(
            fil.rsplit("/",1)[1].split(".",2)[1][0:8], format="%Y%m%d"
        ).strftime("%Y-%m-%d")   
    xdf = xr.open_dataset(fil)
    ## Option 1
    par = xdf.drop_dims(['rgb','eightbitcolor'])
    df = par.to_dataframe().reset_index()
    ## Option 2. Either option works
    # df = data.netcdf4_to_pandas(base_folder + fil, "par")
    df["time"] = pd.to_datetime(
            fil.rsplit("/",1)[1].split(".",2)[1][0:8], format="%Y%m%d"
        )
    df["year"] = pd.to_datetime(df["time"]).dt.year
    df["month"] = pd.to_datetime(df["time"]).dt.month
    df["week"] = pd.to_datetime(df["time"]).dt.isocalendar().week
    df["dayofyear"] = pd.to_datetime(df["time"]).dt.dayofyear
    df = df[["time", "lat", "lon", "par", "year", "month", "week", "dayofyear"]]
    df = data.clean_data_df(df)
    df.to_parquet(f"{rep_folder}{tbl}_{timecol.replace('-','_')}.parquet", index=False)      
    path = f"{rep_folder.split('vault/')[1]}{tbl}_{timecol.replace('-','_')}.parquet"
    metadata.tblProcess_Queue_Process_Update(fil_name, path, tbl, 'Opedia', 'Rainier')
    # pq_file = rep_folder+fil.strip(".nc").replace(".","_")+".parquet"
    # df.to_parquet(pq_file, engine = 'auto', compression = None, index=False)

metadata.export_script_to_vault(tbl,'satellite','process/sat/MODIS/process_MODIS_PAR_cl1.py','process.txt')

## Fix time dtype
from multiprocessing import Pool
import glob
tbl = "tblModis_PAR_cl1"
rep_folder = f'{vs.satellite}{tbl}/rep/'
flist = glob.glob(rep_folder+'*.parquet')
def to_pq(fil):
    try:
        df = pd.read_parquet(fil)
        df['time'] = pd.to_datetime(df['time'])
        df.to_parquet(fil, index=False)
    except Exception as e:        
        print(str(e))        
with Pool(processes=20) as pool:
    pool.map(to_pq, flist)
    pool.close() 
    pool.join()   