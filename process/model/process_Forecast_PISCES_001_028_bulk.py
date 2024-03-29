

import sys
import os
from glob import glob
from datetime import datetime
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'
import xarray as xr
import numpy as np
from tqdm import tqdm 

from multiprocessing import Pool

sys.path.append("ingest")
sys.path.append("../../ingest")

import vault_structure as vs
import data
import metadata



def proc_pic(fil):
    xdf = xr.open_dataset(mod_dir+fil)
    df = xdf.to_dataframe().reset_index()
    xdf.close()
    df = data.add_day_week_month_year_clim(df)

    # df = df[['time','latitude','longitude','depth','talk', 'chl', 'dissic', 'fe', 'no3', 'nppv', 'o2', 'ph', 'phyc', 'po4', 'si', 'spco2','year','month','week','dayofyear']]

    # print(df.columns)

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

    df.rename(columns={'latitude':'lat', 'longitude':'lon'}, inplace=True)
    df.sort_values(['time', 'lat', 'lon', 'depth'], ascending=[True, True, True, True], inplace=True)
    fil_date = df['time'].max().strftime('%Y_%m_%d')
    path = f"{nrt_dir.split('vault/')[1]}{tbl}_{fil_date}.parquet"
    df.to_parquet(f'{nrt_dir}{tbl}_{fil_date}.parquet', index=False)
    s3_str = f"aws s3 cp {tbl}_{fil_date}.parquet s3://cmap-vault/model/{tbl}/nrt/"
    os.system(s3_str)
    return






# tbl = 'tblPisces_Forecast_Nutrients'
# tbl = 'tblPisces_Forecast_Bio'
# tbl = 'tblPisces_Forecast_Car'
# tbl = 'tblPisces_Forecast_Co2'
# tbl = 'tblPisces_Forecast_Optics'
tbl = 'tblPisces_Forecast_Pft'

mod_dir = os.path.normpath(f'{vs.model + tbl}/raw') + '/'
nrt_dir = os.path.normpath(f'{vs.model + tbl}/nrt') + '/'
flist = [os.path.basename(x) for x in glob(f"{mod_dir}*.nc")]
os.chdir(nrt_dir)

# for i, f in enumerate(flist[:1]):
#     print(f"{i+1}/{len(flist)} -- {datetime.now()} --: Ingesting {f}")
#     proc_pic(f)

with Pool(processes=4) as pool:  ## large workers will suffer from mermory shortage
    pool.map(proc_pic,  flist)
    pool.close() 
    pool.join()



