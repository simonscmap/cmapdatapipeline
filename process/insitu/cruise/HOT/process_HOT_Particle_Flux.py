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

tbl = 'tblHOT_ParticleFlux'

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
df['date2'] =df['date2'].astype(str)
df['date2'] =np.where(df.date2.str.len()<6,df.date2.str.zfill(6),df.date2.str.zfill(6))
df['start_time'] =df['start_time'].astype(str)
df['start_time'] =np.where(df.start_time.str.len()<4,df.start_time.str.zfill(4),df.start_time.str.zfill(4))
df['end_time'] =df['end_time'].astype(str)
df['end_time'] =np.where(df.end_time.str.len()<4,df.end_time.str.zfill(4),df.end_time.str.zfill(4))
df.insert(1,'time',pd.to_datetime(df['date'], format='%y%m%d'))
df.insert(4,'time2',pd.to_datetime(df['date2'], format='%y%m%d'))
df.drop(['date','date2'], axis=1, inplace=True)

df['lat']=22.75
df['lon']=-158
df_clean = dc.clean_data_df(df)


dc.check_df_ingest(df_clean,tbl,'Rainier')

col_list = list(df.select_dtypes(include=['object']).columns)

for c in col_list:
    if c not in ['cruise_number_HOT', 'time', 'time2','start_time','end_time']:
        df_clean[c] = df_clean[c].apply(pd.to_numeric, downcast='float')

dc.check_df_ingest(df_clean,tbl,'Rossby')

DB.toSQLbcp_wrapper(df_clean, tbl, "Rossby") 
DB.toSQLbcp_wrapper(df_clean, tbl, "Rainier") 

df_clean.to_parquet(rep_folder+tbl+"_2016_2019.parquet", engine = 'auto', compression = None, index=False)

stats_df = stats.build_stats_df_from_db_calls(tbl,'Rainier')
stats.update_stats_large(tbl,stats_df,'Opedia','Rainier')
stats.update_stats_large(tbl,stats_df,'Opedia','Rossby')

script_path = os.getcwd()+ os.sep 

shutil.copy(script_path + 'process/insitu/cruise/HOT/process_HOT_Particle_Flux.py', code_folder + 'process_HOT_Particle_Flux.py')





