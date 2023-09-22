import pandas as pd

import vault_structure as vs
import credentials as cr
import common as cmn

cruise_name = 'KM0703'

cruise_raw = f"{vs.r2r_cruise}{cruise_name}/raw/"

df= pd.read_csv(cruise_raw+'KM0703_xml.csv')
df_start = df[['metadata_start__date', 'metadata_start__lon', 'metadata_start__lat']]
df_end = df[['metadata_stop__date', 'metadata_stop__lon','metadata_stop__lat']]

df_start.rename(columns={'metadata_start__date':'time', 'metadata_start__lon':'lon', 'metadata_start__lat':'lat'}, inplace=True)
df_end.rename(columns={'metadata_stop__date':'time', 'metadata_stop__lon':'lon', 'metadata_stop__lat':'lat'}, inplace=True)

df_comb = df_start.append(df_end)
cols = ['time','lat','lon']
df_comb=df_comb.sort_values(cols, ascending=[True] * len(cols))

df_comb['time']=pd.to_datetime(df_comb['time'], format="%Y-%m-%dT%H:%M:%S")

df_comb.shape

## Downsample for trajectory
df_comb.index = pd.to_datetime(df_comb.time)
rs_df = df_comb.resample('1min').mean()
rs_df = rs_df.dropna()
rs_df.reset_index(inplace=True)
rs_df['time'] = rs_df['time'].dt.strftime('%Y-%m-%dT%H:%M:%S')

mntime = min(rs_df['time'])
mxtime = max(rs_df['time'])
mnlat = min(rs_df['lat'])
mxlat = max(rs_df['lat'])
mnlon = min(rs_df['lon'])
mxlon = max(rs_df['lon'])

for server in cr.server_alias_list:
    cruise_id = cmn.getCruiseID_Cruise_Name(cruise_name, server)
    
