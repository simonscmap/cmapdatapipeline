import sys
import os
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'
import glob 
import numpy as np
import shutil
from tqdm import tqdm 

sys.path.append("ingest")

import vault_structure as vs
import DB
import data_checks as dc
import common as cmn
import stats
import data

tbl = 'tblHOT_EpiMicroscopy'

base_folder = f'{vs.cruise}{tbl}/raw/'
rep_folder = f'{vs.cruise}{tbl}/rep/'
code_folder= f'{vs.cruise}{tbl}/code/'

flist_all = glob.glob(os.path.join(base_folder, '*.xlsx'))

df = pd.read_excel(flist_all[0], sheet_name='import')
df.shape
df.columns
missingValue = -9
df = df.replace(missingValue, '')
df = cmn.strip_whitespace_headers(df)
df['date'] =df['date'].astype(str)
df['date'] =np.where(df.date.str.len()<6,df.date.str.zfill(6),df.date.str.zfill(6))

df.insert(1,'time',pd.to_datetime(df['date'], format='%m%d%y').dt.date)

df.drop(['date'], axis=1, inplace=True)

df['lat']=22.75
df['lon']=-158

df_clean = data.clean_data_df(df)

dc.check_df_ingest(df_clean,tbl,'Rainier')

col_list = list(df.select_dtypes(include=['object']).columns)

for c in col_list:
    if c not in ['botid_HOT', 'time']:
        df_clean[c] = df_clean[c].apply(pd.to_numeric, downcast='float')

dc.check_df_ingest(df_clean,tbl,'Rossby')
df_clean.dtypes
df_clean.columns

DB.toSQLbcp_wrapper(df_clean, tbl, "Rossby") 
DB.toSQLbcp_wrapper(df_clean, tbl, "Rainier") 

df_clean.to_parquet(rep_folder+tbl+"_2010_2014.parquet", engine = 'auto', compression = None, index=False)

stats_df = stats.build_stats_df_from_db_calls(tbl,'Rainier')
stats.update_stats_large(tbl,stats_df,'Opedia','Rainier')
stats.update_stats_large(tbl,stats_df,'Opedia','Rossby')

script_path = os.getcwd()+ os.sep 

shutil.copy(script_path + 'process/insitu/cruise/HOT/process_HOT_Epimicroscopy.py', code_folder + 'process_HOT_Epimicroscopy.py')





