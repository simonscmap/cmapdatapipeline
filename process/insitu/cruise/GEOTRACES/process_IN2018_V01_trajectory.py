import sys
import os
import pandas as pd
import numpy as np

sys.path.append("cmapdata/ingest")
from ingest import vault_structure as vs

tbl = 'tblGeotraces_Seawater'
raw_folder = f'{vs.cruise}{tbl}/raw/'


df_traj = pd.read_csv(raw_folder+'in2018_v01uwy1min.csv')
df_traj['datetime'] = pd.to_datetime(df_traj['date']+df_traj['time'], format = "%d-%b-%y%H:%M:%S")
df_traj = df_traj[['datetime','latitude(degree_north)','longitude(degree_east)']]
df_traj.rename(columns={'datetime':'time','longitude(degree_east)':'lon','latitude(degree_north)':'lat'}, inplace=True)


## Build cruise_metadata sheet for IN2018_V01
data = dict(Nickname='GS01', Name='IN2018_V01', Ship_Name='Investigator', Chief_Name='Bowie, Andrew',Cruise_Series=4)
df_cruise_meta = pd.DataFrame(data, index=[0])
df_traj= df_traj[~df_traj['lat'].isnull()]

## Export cruise metadata and trajectory template
with pd.ExcelWriter(f"{vs.combined}IN2018_V01_cruise_meta_nav_data.xlsx") as writer:  
    df_cruise_meta.to_excel(writer, sheet_name='cruise_metadata', index=False)
    df_traj.to_excel(writer, sheet_name='cruise_trajectory', index=False)