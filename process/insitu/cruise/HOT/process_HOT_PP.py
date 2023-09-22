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
import stats
import data

tbl = 'tblHOT_PP'

base_folder = f'{vs.cruise}{tbl}/raw/'
rep_folder = f'{vs.cruise}{tbl}/rep/'
code_folder= f'{vs.cruise}{tbl}/code/'
flist_all = glob.glob(os.path.join(base_folder, '*.xlsx'))

df = pd.read_excel(flist_all[0], sheet_name='import')
df.columns
missingValue = -9
df = df.replace(missingValue, '')
df.insert(0,'time',pd.to_datetime(df['date'], format='%y%m%d'))
df.drop('date', axis=1, inplace=True)

df_clean = data.clean_data_df(df)

dc.check_df_ingest(df_clean,'tblHOT_PP','Rainier')
df_clean['light_12_hot']=df_clean['light_12_hot'].apply(pd.to_numeric, downcast='float')
df_clean['dark_12_hot']=df_clean['dark_12_hot'].apply(pd.to_numeric, downcast='float')
df_clean['prochlorococcus_hot']=df_clean['prochlorococcus_hot'].apply(pd.to_numeric, downcast='float')
df_clean['cruise_name']

dc.check_df_ingest(df_clean,'tblHOT_PP','Rossby')
df.loc[:,df.columns != 'time'].describe == df_clean.loc[:,df_clean.columns != 'time'].describe
df_clean.describe
df_clean.columns

DB.toSQLbcp_wrapper(df_clean, tbl, "Rossby") 
DB.toSQLbcp_wrapper(df_clean, tbl, "Rainier") 

df_clean.to_parquet(rep_folder+"tblHOT_PP_2016_2019.parquet", engine = 'auto', compression = None, index=False)

stats_df = stats.build_stats_df_from_db_calls(tbl,'Rainier')
stats.update_stats_large(tbl,stats_df,'Opedia','Rainier')
stats.update_stats_large(tbl,stats_df,'Opedia','Rossby')


script_path = os.getcwd()+ os.sep 
# copied_script_name = time.strftime("%Y-%m-%d_%H%M") + '_' + os.path.basename(__file__)
shutil.copy(script_path + 'process/insitu/cruise/HOT/process_HOT_PP.py', code_folder + 'process_HOT_PP.py')

