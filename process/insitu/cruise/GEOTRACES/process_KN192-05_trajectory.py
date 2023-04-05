import sys
import os
import pandas as pd
import numpy as np

sys.path.append("cmapdata/ingest")
from ingest import vault_structure as vs

tbl = 'tblGeotraces_Seawater'
raw_folder = f'{vs.cruise}{tbl}/raw/'
df_traj = pd.read_csv(raw_folder+'KN192-05.nav', sep='\t', skiprows=2, header=None)
df_traj.columns = ['time','lon','lat']
## Drop segment 2 header in the middle of the data
df_traj = df_traj[~df_traj['lat'].isnull()]

df_traj['time'] = pd.to_datetime(df_traj['time'].str.strip(), format='%Y-%m-%d %H:%M:%S')

## Downsample for trajectory
df_traj.index = pd.to_datetime(df_traj.time)
rs_df = df_traj.resample('1min').mean()
rs_df = rs_df.dropna()
rs_df.reset_index(inplace=True)

## Build cruise_metadata sheet for NBP1702
data = dict(Nickname='GAc01', Name='KN192-05', Ship_Name='R/V Knorr', Chief_Name='Saito, Mak',Cruise_Series=4)
df_cruise_meta = pd.DataFrame(data, index=[0])
## Export cruise metadata and trajectory template
with pd.ExcelWriter(f"{vs.combined}KN192-05_cruise_meta_nav_data.xlsx") as writer:  
    df_cruise_meta.to_excel(writer, sheet_name='cruise_metadata', index=False)
    rs_df.to_excel(writer, sheet_name='cruise_trajectory', index=False)