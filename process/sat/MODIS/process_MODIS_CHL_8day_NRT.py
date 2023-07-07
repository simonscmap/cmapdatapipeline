import sys
import os

import pandas as pd
import numpy as np
import xarray as xr
import glob
from tqdm import tqdm



sys.path.append("ingest")
import vault_structure as vs
import DB
import metadata
import data_checks as dc



tbl = 'tblModis_CHL_NRT'
base_folder = f'{vs.satellite}{tbl}/raw/'
nrt_folder = f'{vs.satellite}{tbl}/nrt/'


## New naming convention: https://oceandata.sci.gsfc.nasa.gov/cgi/getfile/AQUA_MODIS.20220720_20220727.L3m.8D.CHL.chlor_a.9km.NRT.nc

flist = np.sort(glob.glob(os.path.join(base_folder, f'*.nc')))

len(flist)

# ## testing
# qry = "select * from tblmodis_chl where time = '2002-07-04'"
# df_old = DB.dbRead(qry,'Rossby')

# df_old.describe()
# df.describe()

# df_p = pd.read_parquet(rep_folder+'tblModis_CHL_cl1_2002_07_04.parquet')
# df_p.describe()

# fil_o = f'{vs.satellite}tblModis_CHL/raw/A20021852002192.L3m_8D_CHL_chlor_a_9km.nc'
# xo = xr.open_dataset(fil_o)


# d1 = {'var_name':[], 'std_name':[], 'long_name':[], 'dtype':[], 'units':[], 'comment':[], 'flag_val':[], 'flag_def':[]}
# df_varnames = pd.DataFrame(data=d1)

# for varname, da in x.data_vars.items():
#     print(da.attrs)

# for varname, da in xo.data_vars.items():
#     print(da.attrs)

fil = flist[0]
for fil in tqdm(flist):
    fil_date = pd.to_datetime(
            fil.rsplit("/",1)[1].split(".",2)[1][0:8], format="%Y%m%d"
        ).strftime("%Y_%m_%d")    
    x = xr.open_dataset(fil)
    chl = x.drop_dims(['rgb','eightbitcolor'])
    df = chl.to_dataframe().reset_index()
    fdate = pd.to_datetime(
            fil.split(".",2)[1][0:8], format="%Y%m%d"
        )
    df.insert(0,"time", fdate)
    df["time"] = pd.to_datetime(df["time"])    
    df["year"] = pd.to_datetime(df["time"]).dt.year
    df["month"] = pd.to_datetime(df["time"]).dt.month
    df["week"] = pd.to_datetime(df["time"]).dt.isocalendar().week
    df["dayofyear"] = pd.to_datetime(df["time"]).dt.dayofyear
    fil_name = os.path.basename(fil)
    path = f"{nrt_folder.split('vault/')[1]}{tbl}_{fil_date}.parquet"
    df.to_parquet(f"{nrt_folder}{tbl}_{fil_date}.parquet", index=False)      
    metadata.tblProcess_Queue_Process_Update(fil_name, path, tbl, 'Opedia', 'Rainier')
    x.close()


metadata.export_script_to_vault(tbl,'satellite','process/sat/MODIS/process_MODIS_CHL_8day_NRT.py','process.txt')

## Fix time dtype
from multiprocessing import Pool
flist = glob.glob(nrt_folder+'*.parquet')
def to_pq(fil):
    df = pd.read_parquet(fil)
    df['time'] = pd.to_datetime(df['time'])
    df.to_parquet(fil, index=False)
with Pool(processes=4) as pool:
    pool.map(to_pq,  tqdm(flist))
    pool.close() 
    pool.join()