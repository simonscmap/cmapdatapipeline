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

tbl = 'tblHOT_Macrozooplankton'

base_folder = f'{vs.cruise}{tbl}/raw/'
rep_folder = f'{vs.cruise}{tbl}/rep/'
code_folder= f'{vs.cruise}{tbl}/code/'
flist_all = glob.glob(os.path.join(base_folder, '*.xlsx'))

df = pd.read_excel(flist_all[0], sheet_name='import')
df.columns
missingValue = -9
df = df.replace(missingValue, '')
df = cmn.strip_whitespace_headers(df)
df['date'] =df['date'].astype(str)
df['date'] =np.where(df.date.str.len()<6,df.date.str.zfill(6),df.date.str.zfill(6))
df['time'] =df['time'].astype(str)
df['time'] =np.where(df.time.str.len()<4,df.time.str.zfill(4),df.time.str.zfill(4))
df['date_time_str']=df['date'] + df['time']
df.insert(1,'date_time',pd.to_datetime(df['date_time_str'], format='%m%d%y%H%M'))
df.drop(['date','date_time_str'], axis=1, inplace=True)
df.rename(columns={'time':'date_time', 'date_time':'time', 'crn':'cruise_number_HOT'}, inplace=True)
df['lat']=22.75
df['lon']=-158
df_clean = dc.clean_data_df(df)

dc.check_df_ingest(df_clean,tbl,'Rainier')

col_list = list(df.select_dtypes(include=['object']).columns)

for c in col_list:
    if 'time' not in c:
        df_clean[c] = df_clean[c].apply(pd.to_numeric, downcast='float')

dc.check_df_ingest(df_clean,tbl,'Rossby')


DB.toSQLbcp_wrapper(df_clean, tbl, "Rossby") 
DB.toSQLbcp_wrapper(df_clean, tbl, "Rainier") 

df_clean.to_parquet(rep_folder+tbl+"_2017_2020.parquet", engine = 'auto', compression = None, index=False)

stats_df = stats.build_stats_df_from_db_calls(tbl,'Rainier')
stats.update_stats_large(tbl,stats_df,'Opedia','Rainier')
stats.update_stats_large(tbl,stats_df,'Opedia','Rossby')

script_path = os.getcwd()+ os.sep 

shutil.copy(script_path + 'process/insitu/cruise/HOT/process_HOT_Macrozooplankton.py', code_folder + 'process_HOT_Macrozooplankton.py')

