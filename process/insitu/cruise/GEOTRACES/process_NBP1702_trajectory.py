import sys
import os
import pandas as pd
import numpy as np

sys.path.append("cmapdata/ingest")
from ingest import vault_structure as vs

tbl = 'tblGeotraces_Seawater'
raw_folder = f'{vs.cruise}{tbl}/raw/'
df_traj = pd.read_csv(raw_folder+'NBP1702.nav', sep='\t', skiprows=2, header=None)
df_traj.columns = ['time','lon','lat']
## Drop segment 2 header in the middle of the data
df_traj = df_traj[~df_traj['lat'].isnull()]
# 2017-04-18 03:56:00
df_traj['time'] = pd.to_datetime(df_traj['time'].str.strip(), format='%Y-%m-%d %H:%M:%S')

## Build cruise_metadata sheet for NBP1702
data = dict(Nickname='GSc02', Name='NBP1702', Ship_Name='Nathaniel B. Palmer', Chief_Name='Anderson, Robert',Cruise_Series=4)
df_cruise_meta = pd.DataFrame(data, index=[0])
## Export cruise metadata and trajectory template
with pd.ExcelWriter(f"{vs.combined}NBP1702_cruise_meta_nav_data.xlsx") as writer:  
    df_cruise_meta.to_excel(writer, sheet_name='cruise_metadata', index=False)
    df_traj.to_excel(writer, sheet_name='cruise_trajectory', index=False)