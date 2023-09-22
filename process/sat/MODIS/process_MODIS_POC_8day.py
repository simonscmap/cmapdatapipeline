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
from ingest import DB
from ingest import stats

tbl = 'tblModis_POC'
server = 'Rainier'
base_folder = f'{vs.satellite}{tbl}/raw/'
rep_folder = f'{vs.satellite}{tbl}/rep/'
code_folder= f'{vs.satellite}{tbl}/code/'
meta_folder= f'{vs.satellite}{tbl}/metadata/'

qry = f"Select distinct time from dbo.{tbl} where time >'2019-12-11'"
import_dates = DB.dbRead(qry,server)
import_dates['time'] =import_dates['time'].astype(str)

qry = f"Select max(time) mx from dbo.{tbl}"
max_date = DB.dbRead(qry,server)
max_date['mx'] =max_date['mx'].astype(str)

flist_all = np.sort(glob.glob(os.path.join(base_folder, f'*.nc')))
fil=flist_all[0]

modis_flist_base = [os.path.basename(filename) for filename in flist_all]
modis_file_date_list = np.sort(
    [i.strip(".L3m_8D_POC_poc_9km.nc") for i in modis_flist_base]
)

DB.queryExecute(server, 'ALTER INDEX IX_tblModis_POC_year_lat_lon on dbo.tblModis_POC DISABLE')
DB.queryExecute(server, 'ALTER INDEX IX_tblModis_POC_month_lat_lon on dbo.tblModis_POC DISABLE')
DB.queryExecute(server, 'ALTER INDEX IX_tblModis_POC_week_lat_lon on dbo.tblModis_POC DISABLE')
DB.queryExecute(server, 'ALTER INDEX IX_tblModis_POC_dayofyear_lat_lon on dbo.tblModis_POC DISABLE')

files_in_range = []
for i in modis_flist_base:
    fdate = pd.to_datetime(i.strip(".L3m_8D_POC_poc_9km.nc").strip("A")[:7], format="%Y%j")
    if ( fdate > pd.to_datetime(max_date['mx'][0], format="%Y-%m-%d")
    ):
        files_in_range.append(i)
len(files_in_range)
fil = files_in_range[0]
files_in_range.sort()

d = {'filename':[],'null_check':[],'unique_check':[]}
df_error=pd.DataFrame(data=d)

for fil in tqdm(files_in_range):
    print(fil)
    timecol = pd.to_datetime(fil.strip(".L3m_8D_POC_poc_9km.nc").strip("A")[:7], format="%Y%j").strftime("%Y-%m-%d")
    xdf = xr.open_dataset(base_folder + fil, autoclose=True)
    df = data.netcdf4_to_pandas(base_folder + fil, "poc")
    df["time"] = pd.to_datetime(fil.strip(".L3m_8D_POC_poc_9km.nc").strip("A")[:7], format="%Y%j").strftime("%Y-%m-%d")
    df["year"] = pd.to_datetime(df["time"]).dt.year
    df["month"] = pd.to_datetime(df["time"]).dt.month
    df["week"] = pd.to_datetime(df["time"]).dt.isocalendar().week
    df["dayofyear"] = pd.to_datetime(df["time"]).dt.dayofyear
    df = df[["time", "lat", "lon", "poc", "year", "month", "week", "dayofyear"]]
    df = data.sort_values(df, data.ST_columns(df))
    null_check = dc.check_df_nulls(df, tbl, server)
    unique_check = dc.check_df_constraint(df, tbl, server)
    if null_check == 0 and unique_check == 0:
        DB.toSQLbcp_wrapper(df, tbl, server) 
        # pq_file = rep_folder+fil.strip(".nc").replace(".","_")+".parquet"
        # df.to_parquet(pq_file, engine = 'auto', compression = None, index=False)
    else:
        d2 = {'filename':[fil],'null_check':[null_check],'unique_check':[unique_check]}
        temp_df = pd.DataFrame(data=d2)
        df_error = df_error.append(temp_df, ignore_index=True)
    xdf.close()
if len(df_error) > 0:
    df_error.to_excel(meta_folder+f'error_checks_{server}.xlsx')

DB.queryExecute(server, 'ALTER INDEX IX_tblModis_POC_year_lat_lon on dbo.tblModis_POC REBUILD')
DB.queryExecute(server, 'ALTER INDEX IX_tblModis_POC_month_lat_lon on dbo.tblModis_POC REBUILD')
DB.queryExecute(server, 'ALTER INDEX IX_tblModis_POC_week_lat_lon on dbo.tblModis_POC REBUILD')
DB.queryExecute(server, 'ALTER INDEX IX_tblModis_POC_dayofyear_lat_lon on dbo.tblModis_POC REBUILD')

script_path = os.getcwd()+ os.sep 
# copied_script_name = time.strftime("%Y-%m-%d_%H%M") + '_' + os.path.basename(__file__)
shutil.copy(script_path + 'process/sat/MODIS/process_MODIS_POC_8day.py', code_folder + 'process_MODIS_POC_8day.py')

stats_df=stats.build_stats_df_from_db_calls(tbl, server)
stats.update_stats_large(tbl, stats_df, 'Opedia', server)

