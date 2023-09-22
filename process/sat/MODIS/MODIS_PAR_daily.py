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
from ingest import DB
from ingest import data


# startdate = "2010001" #YYYYDDD
startdate = "2022007"  # YYYYDDD

enddate = "2022066"  # YYYYDDD
tbl = "tblModis_PAR"
base_folder = f'{vs.satellite}{tbl}/raw/'
rep_folder = f'{vs.satellite}{tbl}/rep/'
code_folder= f'{vs.satellite}{tbl}/code/'

qry = f"Select distinct time from dbo.{tbl} where year(time) >2020"
import_dates = DB.dbRead(qry,'Rainier')
import_dates['time'] =import_dates['time'].astype(str)

modis_flist = glob.glob(base_folder + "*.nc")
modis_flist_base = [os.path.basename(filename) for filename in modis_flist]
modis_file_date_list = np.sort(
    [i.strip(".L3m_DAY_PAR_par_9km.nc") for i in modis_flist_base]
)

# i=modis_flist_base[0]
files_in_range = []

for i in modis_flist_base:
    strpted_time = i.replace(".L3m_DAY_PAR_par_9km.nc", "").replace("A", "")
    zfill_time = strpted_time[:4] + strpted_time[4:].zfill(3)
    fdate = pd.to_datetime(zfill_time, format="%Y%j")
    fil_date = str(fdate.date().strftime('%Y-%m-%d'))
    # if import_dates['time'].str.contains(str(fil_date)).any():
        # print('already imported')
    if (
        pd.to_datetime(startdate, format="%Y%j")
        <= fdate
        <= pd.to_datetime(enddate, format="%Y%j")
        and not import_dates['time'].str.contains(str(fil_date)).any()
    ):
        print(fdate)
        files_in_range.append(i)

DB.queryExecute('Mariana', 'ALTER INDEX IX_tblModis_PAR_year_lat_lon on dbo.tblModis_PAR DISABLE')
DB.queryExecute('Mariana', 'ALTER INDEX IX_tblModis_PAR_month_lat_lon on dbo.tblModis_PAR DISABLE')
DB.queryExecute('Mariana', 'ALTER INDEX IX_tblModis_PAR_week_lat_lon on dbo.tblModis_PAR DISABLE')
DB.queryExecute('Mariana', 'ALTER INDEX IX_tblModis_PAR_dayofyear_lat_lon on dbo.tblModis_PAR DISABLE')

fil = files_in_range[0]

no_dup = df.drop_duplicates(['time','lat','lon'],keep= 'last')

len(files_in_range)
for fil in tqdm(files_in_range):
    timecol = pd.to_datetime(
        fil.replace(".L3m_DAY_PAR_par_9km.nc", "").replace("A", ""), format="%Y%j"
    ).strftime("%Y-%m-%d")
    xdf = xr.open_dataset(base_folder + fil)
    ## Option 1
    par = xdf.drop_dims(['rgb','eightbitcolor'])
    df = par.to_dataframe().reset_index()
    ## Option 2. Either option works
    # df = data.netcdf4_to_pandas(base_folder + fil, "par")
    df["time"] = timecol
    df["year"] = pd.to_datetime(df["time"]).dt.year
    df["month"] = pd.to_datetime(df["time"]).dt.month
    df["week"] = pd.to_datetime(df["time"]).dt.isocalendar().week
    df["dayofyear"] = pd.to_datetime(df["time"]).dt.dayofyear

    df = df[["time", "lat", "lon", "par", "year", "month", "week", "dayofyear"]]
    # stats.buildLarge_Stats(
    #     df, timecol, "tblModis_PAR", "satellite", transfer_flag="dropbox"
    # )
    df = data.clean_data_df(df)
    # pq_file = rep_folder+fil.strip(".nc").replace(".","_")+".parquet"
    # df.to_parquet(pq_file, engine = 'auto', compression = None, index=False)
    # dc.check_df_nulls(df, tbl, 'Rainier')
    # dc.check_df_dtypes(df, tbl, 'Rainier')
    # dc.check_df_constraint(df,  tbl, 'Rainier')
    DB.toSQLbcp_wrapper(df, tbl, "Rainier") 

# script_path = os.getcwd()+ os.sep 
# # copied_script_name = time.strftime("%Y-%m-%d_%H%M") + '_' + os.path.basename(__file__)
# shutil.copy(script_path + 'process/sat/MODIS/MODIS_PAR_daily.py', code_folder + 'MODIS_PAR_daily.py')

# yr = 2021
# mnt= 10
# day = 25
# qry = f'delete from dbo.{tbl} where year(time)={yr} and month(time)={mnt} and day(time)={day}'
# DB.queryExecute('Rainier', qry)


DB.queryExecute('Rossby', 'ALTER INDEX IX_tblModis_PAR_time_lat_lon on dbo.tblModis_PAR REBUILD')

DB.queryExecute('Mariana', 'ALTER INDEX IX_tblModis_PAR_year_lat_lon on dbo.tblModis_PAR REBUILD')
DB.queryExecute('Mariana', 'ALTER INDEX IX_tblModis_PAR_month_lat_lon on dbo.tblModis_PAR REBUILD')
DB.queryExecute('Rossby', 'ALTER INDEX IX_tblModis_PAR_week_lat_lon on dbo.tblModis_PAR REBUILD')
DB.queryExecute('Rainier', 'ALTER INDEX IX_tblModis_PAR_dayofyear_lat_lon on dbo.tblModis_PAR REBUILD')
