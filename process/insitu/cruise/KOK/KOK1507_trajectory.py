import sys
import os
import pandas as pd
import numpy as np
import glob

import vault_structure as vs

cruise_staging = vs.collected_data + 'insitu/cruise/misc_cruise/'

gz_list = glob.glob(cruise_staging+'*.gz')

df = pd.read_csv(gz_list[0], sep = ' ', header=None)

df = df[[0,1,2,3,4,5,8,9]]
df.columns = ['year','doy','hr','mn','s','ms','lat','lon']
df['date'] = pd.to_datetime(df['year']*1000 + df['doy'], format='%Y%j')
df['time'] = pd.to_datetime(df['date'].astype(str) + df['hr'].astype(str) +":"+ df['mn'].astype(str), format='%Y-%m-%d%H:%M')

## Downsample for trajectory
df.index = pd.to_datetime(df.time)
rs_df = df.resample('1min').mean()
rs_df = rs_df.dropna()
rs_df.reset_index(inplace=True)

rs_df = rs_df[['time','lat','lon']]

## Build cruise_metadata sheet for KOK1507
data = dict(Nickname='HOE Legacy 2b', Name='KOK1507', Ship_Name='Ka`imikai-O-Kanaloa', Chief_Name='Clemente, Tara',Cruise_Series='')
df_cruise_meta = pd.DataFrame(data, index=[0])
## Export cruise metadata and trajectory template
with pd.ExcelWriter(f"{vs.combined}KOK1507_cruise_meta_nav_data.xlsx") as writer:  
    df_cruise_meta.to_excel(writer, sheet_name='cruise_metadata', index=False)
    rs_df.to_excel(writer, sheet_name='cruise_trajectory', index=False)

