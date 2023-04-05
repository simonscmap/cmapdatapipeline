import sys
import os
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'
import xarray as xr
import glob 
import numpy as np
from tqdm import tqdm 

sys.path.append("ingest")

import vault_structure as vs
import DB
import data_checks as dc
import stats

tbl = 'tblSST_AVHRR_OI_NRT'

sat_dir = f'{vs.satellite + tbl}/raw/'
## Specific year of import, if needed
date_import = '/2022'

flist_all = np.sort(glob.glob(os.path.join(sat_dir, '*.nc')))
## Filter out specific years for import
flist = [i for i in flist_all if date_import in i]


## Disable non-unique indices to speed up ingest
DB.queryExecute('Rainier', 'ALTER INDEX IX_tblSST_AVHRR_OI_NRT_dayofyear_lat_lon on dbo.tblSST_AVHRR_OI_NRT DISABLE')
DB.queryExecute('Rainier', 'ALTER INDEX IX_tblSST_AVHRR_OI_NRT_month_lat_lon on dbo.tblSST_AVHRR_OI_NRT DISABLE')
DB.queryExecute('Rainier', 'ALTER INDEX IX_tblSST_AVHRR_OI_NRT_week_lat_lon on dbo.tblSST_AVHRR_OI_NRT DISABLE')
DB.queryExecute('Rainier', 'ALTER INDEX IX_tblSST_AVHRR_OI_NRT_year_lat_lon on dbo.tblSST_AVHRR_OI_NRT DISABLE')

for fil in tqdm(flist):
    xdf = xr.open_dataset(fil)
    df = xdf.to_dataframe().reset_index()
    ## nv variable 1 and 0, both hold same SST data
    df = df.query('nv == 1')
    df_import = df[['lat', 'lon','time','analysed_sst']]
    df_import.rename(columns={'analysed_sst':'sst'}, inplace = True)
    ## Original data in Kelvin
    df_import['sst'] = df_import['sst']- 273.15 
    df_import = dc.add_day_week_month_year_clim(df_import)
    df_import.sort_values(['time', 'lat', 'lon'], ascending=[True, True, True], inplace=True)
    ## First round checks
    #dc.check_df_ingest(df_import,tbl,'Rossby')
    DB.toSQLbcp_wrapper(df_import, tbl, "Rainier") 

## Rebuild non-unique indices after ingest
DB.queryExecute('Rainier', 'ALTER INDEX IX_tblSST_AVHRR_OI_NRT_year_lat_lon on dbo.tblSST_AVHRR_OI_NRT REBUILD')
DB.queryExecute('Rainier', 'ALTER INDEX IX_tblSST_AVHRR_OI_NRT_month_lat_lon on dbo.tblSST_AVHRR_OI_NRT REBUILD')
DB.queryExecute('Rainier', 'ALTER INDEX IX_tblSST_AVHRR_OI_NRT_week_lat_lon on dbo.tblSST_AVHRR_OI_NRT REBUILD')
DB.queryExecute('Rainier', 'ALTER INDEX IX_tblSST_AVHRR_OI_NRT_dayofyear_lat_lon on dbo.tblSST_AVHRR_OI_NRT REBUILD')


df = stats.build_stats_df_from_db_calls(tbl, 'Rainier')
df.head
stats.update_stats_large(tbl, df, 'Opedia','Rainier')
