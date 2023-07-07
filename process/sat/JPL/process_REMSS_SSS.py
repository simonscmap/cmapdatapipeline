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
import DB 
import data_checks as dc
import stats

server = 'Rainier'
tbl = 'tblSSS_NRT'
base_folder = f'{vs.satellite}{tbl}/raw/'
meta_folder = f'{vs.satellite}{tbl}/metadata/'
nrt_folder = f'{vs.satellite}{tbl}/nrt/'

flist_all = np.sort(glob.glob(os.path.join(base_folder, f'*.nc')))

fil = flist_all[0]
d = {'filename':[],'null_check':[],'unique_check':[]}
df_error=pd.DataFrame(data=d)

DB.queryExecute(server, 'ALTER INDEX IX_tblSSS_NRT_year_lat_lon on dbo.tblSSS_NRT DISABLE')
DB.queryExecute(server, 'ALTER INDEX IX_tblSSS_NRT_month_lat_lon on dbo.tblSSS_NRT DISABLE')
DB.queryExecute(server, 'ALTER INDEX IX_tblSSS_NRT_week_lat_lon on dbo.tblSSS_NRT DISABLE')
DB.queryExecute(server, 'ALTER INDEX IX_tblSSS_NRT_dayofyear_lat_lon on dbo.tblSSS_NRT DISABLE')


for fil in tqdm(flist_all):
    x = xr.open_dataset(fil)
    x = x['sss_smap']
    df_raw = x.to_dataframe().reset_index()
    x.close()
    df = dc.add_day_week_month_year_clim(df_raw)
    df = df[['lat','lon','time','sss_smap','year','month','week','dayofyear']]
    df = df.sort_values(["time", "lat","lon"], ascending = (True, True,True))
    df = dc.mapTo180180(df)
    null_check = dc.check_df_nulls(df, tbl, server)
    unique_check = dc.check_df_constraint(df, 'tbl', server)
    if null_check == 0 and unique_check == 0:
        DB.toSQLbcp_wrapper(df, tbl, server) 
        # df.to_parquet(nrt_folder+fil.rsplit('/',1)[1].split('.',1)[0]+'.parquet')
        del df
    else:
        d2 = {'filename':[fil],'null_check':[null_check],'unique_check':[unique_check]}
        temp_df = pd.DataFrame(data=d2)
        df_error = df_error.append(temp_df, ignore_index=True)   
        del df
if len(df_error)>0:
    df_error.to_excel(meta_folder+f'error_checks_{server}.xlsx')
#RSS_smap_SSS_L3_8day_running_{yr}_{dayn_str}_FNL_v04.0.nc

DB.queryExecute(server, 'ALTER INDEX IX_tblSSS_NRT_year_lat_lon on dbo.tblSSS_NRT REBUILD')
DB.queryExecute(server, 'ALTER INDEX IX_tblSSS_NRT_month_lat_lon on dbo.tblSSS_NRT REBUILD')
DB.queryExecute(server, 'ALTER INDEX IX_tblSSS_NRT_week_lat_lon on dbo.tblSSS_NRT REBUILD')
DB.queryExecute(server, 'ALTER INDEX IX_tblSSS_NRT_dayofyear_lat_lon on dbo.tblSSS_NRT REBUILD')


stats_df = stats.build_stats_df_from_db_calls(tbl, server)
stats.update_stats_large(tbl, stats_df, 'Opedia', server)