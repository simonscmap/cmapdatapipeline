import sys
import os
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'
import xarray as xr
import numpy as np
from tqdm import tqdm 
import datetime

sys.path.append("ingest")
sys.path.append("../../../ingest")

import vault_structure as vs
import DB
from pandas.util import hash_pandas_object


tbl = 'tblSST_AVHRR_OI_NRT'

sat_dir = f'{vs.satellite + tbl}/raw/'

# max_date = datetime.date(2019, 4, 28)
# end_date = datetime.date(2023, 5, 16)

delta = datetime.timedelta(days=1)

d = {'date':[], 'empty':[], 'date_match':[], 'data_match':[]}
df_checks = pd.DataFrame(data=d)

qry = "select  Original_Name  from tblProcess_Queue where Table_Name = 'tblSST_AVHRR_OI_NRT'  and error_str is  null and original_name like '2023%'"
flist_imp = DB.dbRead(qry,'Rainier')
flist = flist_imp['Original_Name'].str.strip().to_list()

for fil in tqdm(flist):
    max_date = pd.to_datetime(fil[:8],format='%Y%m%d')
    yr = max_date.year
    month = f"{max_date:%m}"
    day = f"{max_date:%d}"
    ## Get New data
    file_url = f'https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/AVHRR_OI-NCEI-L4-GLOB-v2.1/{yr}{month}{day}120000-NCEI-L4_GHRSST-SSTblend-AVHRR_OI-GLOB-v02.0-fv02.1.nc'
    save_path = f'{vs.download_transfer}SST_Test/{yr}{month}{day}120000-NCEI-L4_GHRSST-SSTblend-AVHRR_OI-GLOB-v02.0-fv02.1.1.nc'
    wget_str = f'wget --no-check-certificate "{file_url}" -O "{save_path}"'
    os.system(wget_str)
    if os.path.getsize(save_path) == 0:
        print('empty')
        d = {'date':[max_date], 'empty':[True], 'date_match':[None], 'data_match':[None]}
        temp_df = pd.DataFrame(data=d)
        df_checks = df_checks.append(temp_df, ignore_index = True)
        continue
    xdf = xr.open_dataset(save_path)
    create_date_str = xdf.attrs['date_created']
    create_date = pd.to_datetime(create_date_str,format="%Y%m%dT%H%M%SZ")
    date_match = True
    data_match = True
    if max_date.date() != create_date.date() - delta:
        date_match = False
    df_new = xdf.to_dataframe().reset_index()
    df_new = df_new.query('nv == 1')
    df_new.rename(columns={'analysed_sst':'sst'}, inplace = True)
    ## Original data in Kelvin
    df_new['sst'] = df_new['sst']- 273.15  
    del xdf
    xdf = xr.open_dataset(f'{sat_dir}{yr}{month}{day}120000-NCEI-L4_GHRSST-SSTblend-AVHRR_OI-GLOB-v02.0-fv02.1.nc')
    df_old = xdf.to_dataframe().reset_index()
    df_old = df_old.query('nv == 1')
    df_old.rename(columns={'analysed_sst':'sst'}, inplace = True)
    ## Original data in Kelvin
    df_old['sst'] = df_old['sst']- 273.15     
    if hash_pandas_object(df_new).sum()!=hash_pandas_object(df_old).sum():
        data_match = False
    d = {'date':[max_date], 'empty':[False], 'date_match':[date_match], 'data_match':[data_match]}
    temp_df = pd.DataFrame(data=d)
    df_checks = df_checks.append(temp_df, ignore_index = True)
    os.remove(save_path)
df_checks.to_excel(vs.download_transfer+'SST_Test/checks.xlsx', index=False)