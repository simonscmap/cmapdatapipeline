import sys
import os
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'
import glob 
import numpy as np
from tqdm import tqdm 
import datetime
import shutil

sys.path.append("ingest")

import vault_structure as vs
import DB
import data_checks as dc
import common as cmn
import stats

tbl = 'tblHOT_CTD'

base_folder = f'{vs.cruise}{tbl}/raw/'
rep_folder = f'{vs.cruise}{tbl}/rep/'
code_folder= f'{vs.cruise}{tbl}/code/'
flist_all = glob.glob(os.path.join(base_folder, '*.xlsx'))

base_date = datetime.datetime.strptime('1988-10-01', '%Y-%m-%d').date()

df = pd.read_excel(flist_all[1], sheet_name='import')
df.columns
missingValue = -9
df = df.replace(missingValue, '')
df = cmn.strip_whitespace_headers(df)

df.insert(1,'time',df.apply(lambda df: base_date + pd.offsets.DateOffset(days=df['julian']), 1))
df.drop(['julian'], axis=1, inplace=True)

df['lat']=22.75
df['lon']=-158
df['depth']=df['pressure_ctd_hot']
df_clean = dc.clean_data_df(df)


dc.check_df_ingest(df_clean,tbl,'Rainier')

col_list = list(df.select_dtypes(include=['object']).columns)

for c in col_list:
    if 'time' not in c:
        df_clean[c] = df_clean[c].apply(pd.to_numeric, downcast='float')

dc.check_df_ingest(df_clean,tbl,'Rossby')
df_clean.columns

DB.toSQLbcp_wrapper(df_clean, tbl, "Rossby") 
DB.toSQLbcp_wrapper(df_clean, tbl, "Rainier") 

df_clean.to_parquet(rep_folder+tbl+"_1988_2020_depth.parquet", engine = 'auto', compression = None, index=False)

stats_df = stats.build_stats_df_from_db_calls(tbl,'Rainier')
stats.update_stats_large(tbl,stats_df,'Opedia','Rainier')
stats.update_stats_large(tbl,stats_df,'Opedia','Rossby')

script_path = os.getcwd()+ os.sep 

shutil.copy(script_path + 'process/insitu/cruise/HOT/process_HOT_CTD.py', code_folder + 'process_HOT_CTD.py')


