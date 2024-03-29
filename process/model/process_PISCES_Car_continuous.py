"""
Author: Diana Haring <dharing@uw.edu>
Date: 02-17-2023

Script to run processing for continuous ingestion
"""

import sys
import os
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'
import xarray as xr
import numpy as np
from tqdm import tqdm 

from multiprocessing import Pool

sys.path.append("ingest")
sys.path.append("../../ingest")

import vault_structure as vs
import DB
import data
import metadata
import credentials as cr


############# inputs #############
tbl = 'tblPisces_Forecast_Car'
##################################


mod_dir = f'{vs.model + tbl}/raw/'
nrt_dir = f'{vs.model + tbl}/nrt/'

os.chdir(nrt_dir)

## Compare data types with an old parquet in vault 
test_fil = nrt_dir+f'{tbl}_2023_11_15.parquet'
test_df = pd.read_parquet(test_fil)
test_dtype = test_df.dtypes.to_dict()



## Pull list of newly downloaded files
qry = f"SELECT Original_Name from tblProcess_Queue WHERE Table_Name = '{tbl}' AND Processed IS NULL AND Error_Str IS NULL"
flist_imp = DB.dbRead(qry,'Rainier')
flist = flist_imp['Original_Name'].str.strip().to_list()


def proc_pic(fil):
    """Process and ingest Pisces NetCDF"""
    xdf = xr.open_dataset(mod_dir+fil)
    df = xdf.to_dataframe().reset_index()
    xdf.close()
    df = data.add_day_week_month_year_clim(df)
    # df = df[['time','latitude','longitude','depth','talk', 'chl', 'dissic', 'fe', 'no3', 'nppv', 'o2', 'ph', 'phyc', 'po4', 'si', 'spco2','year','month','week','dayofyear']]

    columns = {
        "tblPisces_Forecast_Nutrients": ['time','latitude','longitude','depth', 'fe', 'no3', 'po4', 'si', 'year', 'month', 'week', 'dayofyear'],
        "tblPisces_Forecast_Bio": ['time','latitude','longitude','depth', 'nppv', 'o2', 'year', 'month', 'week', 'dayofyear'],
        "tblPisces_Forecast_Car": ['time','latitude','longitude','depth', 'ph', 'dissic', 'talk', 'year', 'month', 'week', 'dayofyear'],
        "tblPisces_Forecast_Co2": ['time','latitude','longitude', 'spco2', 'year', 'month', 'week', 'dayofyear'],  ## >>>>>>>>  has no depth
        "tblPisces_Forecast_Optics": ['time','latitude','longitude','depth', 'kd', 'year', 'month', 'week', 'dayofyear'],
        "tblPisces_Forecast_Pft": ['time','latitude','longitude','depth', 'phyc', 'chl', 'year', 'month', 'week', 'dayofyear']
    }

    df = df[columns[tbl]]

    df['time'] = df['time'].astype('<M8[us]')

    df.rename(columns={'latitude':'lat','longitude':'lon'},inplace=True)
    df.sort_values(['time','lat', 'lon','depth'], ascending=[True, True, True, True], inplace=True)
    fil_date = df['time'].max().strftime('%Y_%m_%d')
    path = f"{nrt_dir.split('vault/')[1]}{tbl}_{fil_date}.parquet"
    df.to_parquet(f'{nrt_dir}{tbl}_{fil_date}.parquet', index=False)
    metadata.tblProcess_Queue_Process_Update(fil, path, tbl, 'Opedia', 'Rainier')
    s3_str = f"aws s3 cp {tbl}_{fil_date}.parquet s3://cmap-vault/model/{tbl}/nrt/"
    os.system(s3_str)
    qry = f"SELECT * from tblIngestion_Queue where table_name = '{tbl}' and path = '{path}'"
    chk = DB.dbRead(qry,'Rainier')
    if len(chk) ==0:
        metadata.tblIngestion_Queue_Staged_Update(path, tbl, 'Opedia', 'Rainier')
    else:
        print('Overwrite')
        metadata.tblIngestion_Queue_Overwrite(path, tbl, 'Opedia', 'Rainier')

with Pool(processes=4) as pool:
    pool.map(proc_pic,  flist)
    pool.close() 
    pool.join()



