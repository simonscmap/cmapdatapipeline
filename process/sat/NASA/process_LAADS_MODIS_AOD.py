import sys
import os

import pandas as pd
import numpy as np
from pyhdf.SD import SD, SDC
from pathlib import Path

import glob
from tqdm import tqdm


os.chdir('/home/exx/Documents/CMAP/cmapdata/ingest')
sys.path.append("ingest")
import vault_structure as vs
import credentials as cr
import DB 
import data_checks as dc
import stats

server = 'Rossby'
tbl = 'tblModis_AOD_REP'
base_folder = f'{vs.satellite}{tbl}/raw/MYD08_M3'
rep_folder = f'{vs.satellite}{tbl}/rep/'
code_folder= f'{vs.satellite}{tbl}/code/'


yr_list = ['2020','2021','2022']

# flist = []
# for path in Path(base_folder).rglob('*.hdf'):
#     flist.append(path)



qry = f"Select distinct time from dbo.{tbl}"
import_dates = DB.dbRead(qry,server)

flist = []
for root, dirs, files in os.walk(str(base_folder)):
    for f in files:
        if f.endswith('.hdf'):
            full_path = os.path.join(root, f)
            flist.append(full_path)
f = full_path
len(flist)
for f in tqdm(flist):
    file_date = f.rsplit('.',4)[1].replace('A','')
    check_date =pd.to_datetime(file_date, format = '%Y%j').date()
    if len( import_dates.loc[import_dates['time']==check_date]) == 1:
        print(f'{check_date} already imported')
        continue
    file = SD(f, SDC.READ)
    ## Get lat lon
    xdim = file.select('XDim')
    ydim = file.select('YDim')
    lon,lat = np.meshgrid(xdim[:].astype(np.double),ydim[:].astype(np.double))
    sds_obj = file.select('AOD_550_Dark_Target_Deep_Blue_Combined_Mean_Mean')
    data = sds_obj.get()
    for key, value in sds_obj.attributes().items():
        # print(key, value)
        if key == 'add_offset':
            add_offset = value  
        if key == 'scale_factor':
            scale_factor = value
        if key == '_FillValue':
            fill_value = value    
    data = data.astype('float')
    data[data == fill_value] = np.nan
    data_scaled = (data - add_offset) * scale_factor
    df = pd.DataFrame({'lat': lat.ravel(), 'lon': lon.ravel(), 'aod': data_scaled.ravel()})
    df['time'] =check_date
    df = dc.add_day_week_month_year_clim(df)
    df = dc.sort_values(df,['time','lat','lon'])
    df = df[['lat','lon','time','aod','year','month','week','dayofyear']]
    DB.toSQLbcp_wrapper(df, tbl, 'mariana')
    DB.toSQLbcp_wrapper(df, tbl, 'rossby')
    df.to_parquet(f"{rep_folder}{tbl}_{check_date.strftime('%Y-%m-%d').replace('-','_')}.parquet",index=False)

# stats_df = stats.build_stats_df_from_db_calls(tbl, server)
# stats.update_stats_large(tbl, stats_df, 'Opedia', server)