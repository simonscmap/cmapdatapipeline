import sys
import os

sys.path.append("../../../ingest")

import vault_structure as vs
import DB

import pandas as pd
import xarray as xr
import glob 
import numpy as np
import metadata

from multiprocessing import Pool

tbl = 'tblArgo_MLD_2022_Climatology'
raw_dir = vs.float_dir+tbl+"/raw/"
rep_folder = vs.float_dir+tbl+"/rep/"

flist = np.sort(glob.glob(os.path.join(raw_dir, '*.nc')))

fil = flist[0]


xdf = xr.open_dataset(fil)
df = xdf.to_dataframe().reset_index()
df = df[['month','lat','lon','mld_da_mean', 'mld_dt_mean', 'mld_da_median', 'mld_dt_median', 'mld_da_std', 'mld_dt_std', 'mld_da_max', 'mld_dt_max', 'mlpd_da', 'mlpd_dt', 'mlt_da', 'mlt_dt', 'mls_da', 'mls_dt', 'num']]

for c in df.columns.to_list():
    if c not in ['month','lat','lon','num']:
        df.rename(columns={c:c+'_argo_clim'}, inplace=True)

df.rename(columns={'num':'num_profiles_argo_clim'}, inplace=True)  
df['month'] = df['month'].astype(int)  
df.to_parquet(f"{rep_folder}{tbl}.parquet", index=False)     

try:
    a = [df,df,df]
    b = [tbl,tbl,tbl]
    c = ['mariana','rossby','rainier']
    with Pool(processes=3) as pool:
        result = pool.starmap(DB.toSQLbcp_wrapper, zip(a,b,c))
        pool.close() 
        pool.join()
except Exception as e:        
    print(str(e))      

metadata.export_script_to_vault(tbl,'float_dir','process/insitu/float/ARGO/process_process_Argo_MLD_Climatology_2022.py','process.txt')