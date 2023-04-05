import sys
import os
import pandas as pd
import numpy as np
import glob
import shutil
import requests

import vault_structure as vs
import DB
import cruise

cruise_name = 'KM2003'
vs.cruise_leaf_structure(vs.r2r_cruise + cruise_name)

raw_path = vs.r2r_cruise + cruise_name + '/raw/'
code_folder = vs.r2r_cruise + cruise_name + '/code/'
meta_folder = vs.r2r_cruise + cruise_name + '/metadata/'
traj_folder = vs.r2r_cruise + cruise_name + '/trajectory/'

url = 'https://hahana.soest.hawaii.edu/FTP/hot/tsg/hot319ts.dat'
file_data = requests.get(url).content

r = requests.get(url, stream=True)
with open(raw_path+"hot319ts.dat", "wb") as f:
    for chunk in r.iter_content(chunk_size=1024 * 8):
        if chunk:
            f.write(chunk)
            f.flush()
            os.fsync(f.fileno())

flist = glob.glob(raw_path+'*.dat')

## Skips rows with NAN lon, breaking read_csv
df = pd.read_csv(flist[0], sep = ' ',error_bad_lines=False)
df.drop(columns={'Unnamed: 1','Unnamed: 4'}, inplace=True)

df.columns = ['year','decimal_year','lon','lat','temp','salinity','quality']

## Converts decimal year (28.74464) to datetime (2020-01-28 17:52:16.896)
df['time'] = pd.to_datetime(str(df['year'][0])) + df['decimal_year'].sub(1).apply(pd.Timedelta, unit='D')

#### Already at 1 min intervals
## Downsample for trajectory
# df.index = pd.to_datetime(df.time)
# rs_df = df.resample('1min').mean()
# rs_df = rs_df.dropna()
# rs_df.reset_index(inplace=True)

rs_df = df[['time','lat','lon']]
rs_df.to_parquet(traj_folder +f'{cruise_name}_cruise_trajectory.parquet')

server_list = ['Rainier','Rossby','Mariana']
db_name = 'Opedia'

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
