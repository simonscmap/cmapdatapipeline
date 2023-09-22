import sys
import os
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'
import glob 
import numpy as np
import shutil
from tqdm import tqdm 

sys.path.append("ingest")
sys.path.append("cmapdata/ingest")

import vault_structure as vs
import DB
import data_checks as dc
import common as cmn
import data

tbl = 'tblHOT_Bottle_WHOTS50'

vs.leafStruc(f'{vs.cruise}{tbl}')

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
df['date_time'] =df['date_time'].astype(str)
df['date_time'] =np.where(df.date_time.str.len()<6,df.date_time.str.zfill(6),df.date_time.str.zfill(6))
## SQL field is date only. Change table datatype to keep added time
df['date_time_str']=df['date'] + df['date_time']
df.drop(['date'], axis=1, inplace=True)
df.insert(1,'date',pd.to_datetime(df['date_time_str'], format='%m%d%y%H%M%S'))
df.drop(['date_time_str'], axis=1, inplace=True)
df.rename(columns={'date':'time'}, inplace=True)
	
df['lat']= 22.75
df['lon']= -157.9
df['depth']=df['pressure_ctd_bottle_whots50_hot']
df_clean = data.clean_data_df(df)

dc.check_df_ingest(df_clean,tbl,'Rainier')

col_list = list(df_clean.select_dtypes(include=['object']).columns)

for c in col_list:
    if 'time' not in c:
        df_clean[c] = df_clean[c].apply(pd.to_numeric, downcast='float')

#tbl = 'tblHOT_Bottle'
dc.check_df_ingest(df_clean,tbl,'Rossby')


DB.toSQLbcp_wrapper(df_clean, tbl, "Rossby") 
DB.toSQLbcp_wrapper(df_clean, tbl, "Rainier") 

df_clean.to_parquet(rep_folder+tbl+"_2016_2020.parquet", engine = 'auto', compression = None, index=False)

## Only aggregate table for all stations has stats
# stats_df = stats.build_stats_df_from_db_calls(tbl,'Rainier')
# stats.update_stats_large(tbl,stats_df,'Opedia','Rainier')
# stats.update_stats_large(tbl,stats_df,'Opedia','Rossby')

script_path = os.getcwd()+ os.sep 

shutil.copy(script_path + 'cmapdata/process/insitu/cruise/HOT/process_HOT_Bottle_WHOTS50.py', code_folder + 'process_HOT_Bottle_WHOTS50.py')
