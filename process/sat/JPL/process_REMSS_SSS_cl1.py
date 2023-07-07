import sys
import os

import pandas as pd
import numpy as np
import xarray as xr
import glob
from tqdm import tqdm
import datetime

sys.path.append("ingest")
import vault_structure as vs
import credentials as cr
import metadata 
import data_checks as dc


tbl = 'tblSSS_NRT_cl1'
base_folder = f'{vs.satellite}{tbl}/raw/'
meta_folder = f'{vs.satellite}{tbl}/metadata/'
nrt_folder = f'{vs.satellite}{tbl}/nrt/'

flist_all = np.sort(glob.glob(os.path.join(base_folder, f'*.nc')))


for fil in tqdm(flist_all):
    x = xr.open_dataset(fil)
    x_time = x.time.values[0]
    x = x['sss_smap']
    df_raw = x.to_dataframe().reset_index()
    df_raw['time'] = x_time
    x.close()
    df = dc.add_day_week_month_year_clim(df_raw)
    df = df[['time','lat','lon','sss_smap','year','month','week','dayofyear']]
    df = df.sort_values(["time", "lat","lon"], ascending = (True, True,True))
    df = dc.mapTo180180(df)
    fil_name = os.path.basename(fil)
    fil_date = df['time'][0].strftime("%Y_%m_%d") 
    path = f"{nrt_folder.split('vault/')[1]}{tbl}_{fil_date}.parquet"
    df.to_parquet(f"{nrt_folder}{tbl}_{fil_date}.parquet", index=False)      
    metadata.tblProcess_Queue_Process_Update(fil_name, path, tbl, 'Opedia', 'Rainier')

metadata.export_script_to_vault(tbl,'satellite','process/sat/JPL/process_REMSS_SSS_cl1.py','process.txt')

