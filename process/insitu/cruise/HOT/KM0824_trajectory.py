import sys
import os
import pandas as pd
import numpy as np
import glob
import shutil

sys.path.append("ingest")

import vault_structure as vs
import common as cmn
import cruise
import DB

#https://www.marine-geo.org/tools/search/Files.php?data_set_uid=17293
cruise_name = 'KM0824'
vs.cruise_leaf_structure(vs.r2r_cruise + cruise_name)

raw_path = vs.r2r_cruise + cruise_name + '/raw/'
code_folder = vs.r2r_cruise + cruise_name + '/code/'
meta_folder = vs.r2r_cruise + cruise_name + '/metadata/'
traj_folder = vs.r2r_cruise + cruise_name + '/trajectory/'

gz_list = glob.glob(raw_path+'*.gz')

df = pd.read_csv(gz_list[0], sep = '\t', skiprows=2, header=None)

df.columns = ['time','lon','lat']
## Coerce errors for mid-file "segement 2" header
df['time'] = pd.to_datetime(df['time'], format='%Y-%m-%d %H:%M:%S', errors='coerce')

## Downsample for trajectory
df.index = pd.to_datetime(df.time)
rs_df = df.resample('1min').mean()
rs_df = rs_df.dropna()
rs_df.reset_index(inplace=True)

rs_df = rs_df[['time','lat','lon']]
rs_df.to_parquet(traj_folder +f'{cruise_name}_cruise_trajectory.parquet')

server_list = ['Rainier','Rossby','Mariana']
db_name = 'Opedia'

#cruise.delete_cruise_trajectory(cruise_name, db_name, server)
for server in server_list:
    cruise.insert_cruise_trajectory(rs_df, cruise_name, db_name, server)

for server in server_list:
    qry = f"select * from tblcruise where name = '{cruise_name}'"
    df_cruise = DB.dbRead(qry,server)
    if df_cruise['Start_Time'].isna().all() and len(df_cruise)==1:
        min_time = str(min(rs_df['time']))
        max_time = str(max(rs_df['time']))
        qry = f"UPDATE tblcruise set Start_Time = '{min_time}', End_Time = '{max_time}' where name = '{cruise_name}'"
        DB.DB_modify(qry, server)

df_cruise_meta = df_cruise[['Nickname', 'Name', 'Ship_Name','Chief_Name','Cruise_Series']]
df_cruise_meta.to_parquet(meta_folder + f'{cruise_name}_cruise_metadata.parquet')

script_path = os.getcwd()+ os.sep 

shutil.copy(script_path + f'process/insitu/cruise/HOT/{cruise_name}_trajectory.py', code_folder + f'{cruise_name}_trajectory.py')
