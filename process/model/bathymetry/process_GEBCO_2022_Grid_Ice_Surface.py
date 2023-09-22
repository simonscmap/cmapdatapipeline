import sys
import os
import pandas as pd
import numpy as np
import glob
import xarray as xr
from datetime import datetime

sys.path.append("ingest")

import vault_structure as vs
import SQL
import DB
import metadata
import stats
import common as cmn

tbl = 'tblGEBCO_2022_Grid_Ice_Surface'


raw_folder = vs.model+tbl+'/raw'

flist = glob.glob(raw_folder+'/*.nc')

x =  xr.open_dataset(flist[0])
# x.dims 
# x.data_vars
# x.attrs
# x.crs

df=x.to_dataframe().reset_index()
df.drop('crs', axis=1, inplace=True)
dt = datetime.strptime('2022-06-22', '%Y-%m-%d').date()
df.insert(loc=0, column='time', value='2022-06-22')
df['time'] = pd.to_datetime(df['time'], format='%Y-%m-%d')


## Too large to pass full df
df_sql_build = df.head(100)
SQL.full_SQL_suggestion_build(df_sql_build, tbl, 'model', 'Rossby', 'Opedia')

DB.toSQLbcp_wrapper(df, tbl, 'Rossby')


metadata.tblDataset_Server_Insert(tbl, 'Opedia', 'Rossby')
## After metadata ingest
row_count = len(df)
stats.updateStats_Manual("2022-06-22T00:00:00.000Z", "2022-06-22T00:00:00.000Z", -89.99792, 89.99792, -179.9979, 179.9979, None, None, row_count, tbl, 'Opedia', 'Rainier')

