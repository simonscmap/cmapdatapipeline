import sys
import os
import pandas as pd
import numpy as np
import glob
from zipfile import ZipFile

sys.path.append("cmapdata/ingest")
from ingest import vault_structure as vs
from ingest import DB 
from ingest import common as cmn 


tbl = 'tblGeotraces_Seawater'
raw_folder = f'{vs.cruise}{tbl}/raw/'


def dm(x):
    degrees = int(x) // 100
    minutes = x - 100*degrees

    return degrees, minutes

def decimal_degrees(degrees, minutes, sec):
    return degrees + (minutes/60) + (sec/3600)


with ZipFile(f"{raw_folder}/ACE_Course.zip","r") as zip_ref:
    zip_ref.extractall(raw_folder)

flist = glob.glob(raw_folder+'COURSE*')

d = {'time':[], 'lat':[], 'lon':[]}
df_traj = pd.DataFrame(data=d)

for fil in flist:
    df = pd.read_csv(fil, sep='\t', engine='python',skiprows=1, header=None,encoding = "ISO-8859-1")
    df = df.iloc[:, 0:5]
    df.columns = ['date','lat_str','lat_dir','lon_str','lon_dir']

    pattern = r'(?P<d>[\d\.]+).*?(?P<m>[\d\.]+)'
    hemisphere_sig = {'N': 1, 'S': -1, 'E': 1, 'W': -1}

    dms = df['lat_str'].str.extract(pattern).astype(float)

    df['lat'] = dms['d'] + dms['m'].div(60)
    df['lat'] =df['lat'].mul(df['lat_dir'].map(hemisphere_sig)).fillna(df['lat'])

    dms = df['lon_str'].str.extract(pattern).astype(float)

    df['lon'] = dms['d'] + dms['m'].div(60)
    df['lon'] =df['lon'].mul(df['lon_dir'].map(hemisphere_sig)).fillna(df['lon'])

    df['time'] = pd.to_datetime(df['date'], format = "%Y%m%d%H%M%S")

    df_traj = pd.concat([df_traj, df[['time','lat','lon']]])

df_traj.drop_duplicates()
df_ace1 = df_traj[(df_traj['time'] < '2017-01-20')]
df_ace2 = df_traj[(df_traj['time'] > '2017-01-20') & (df_traj['time'] < '2017-02-23')]
df_ace3 = df_traj[(df_traj['time'] > '2017-02-24')]

## Build cruise_metadata sheet for ACE1
data = dict(Nickname='GSc01', Name='ACE1', Ship_Name='Akademik Tryoshnikov', Chief_Name='Janssen, David',Cruise_Series=4)
df_cruise_meta = pd.DataFrame(data, index=[0])
df_ace1[~df_ace1['lat'].isnull()]
## Export cruise metadata and trajectory template
with pd.ExcelWriter(f"{vs.combined}ACE1_cruise_meta_nav_data.xlsx") as writer:  
    df_cruise_meta.to_excel(writer, sheet_name='cruise_metadata', index=False)
    df_ace1.to_excel(writer, sheet_name='cruise_trajectory', index=False)

## Build cruise_metadata sheet for ACE2
data = dict(Nickname='GSc01', Name='ACE2', Ship_Name='Akademik Tryoshnikov', Chief_Name='Conway, Tim',Cruise_Series=4)
df_cruise_meta = pd.DataFrame(data, index=[0])
df_ace2[~df_ace2['lat'].isnull()]
## Export cruise metadata and trajectory template
with pd.ExcelWriter(f"{vs.combined}ACE2_cruise_meta_nav_data.xlsx") as writer:  
    df_cruise_meta.to_excel(writer, sheet_name='cruise_metadata', index=False)
    df_ace2.to_excel(writer, sheet_name='cruise_trajectory', index=False)    

## Build cruise_metadata sheet for ACE3
data = dict(Nickname='GSc01', Name='ACE3', Ship_Name='Akademik Tryoshnikov', Chief_Name='Conway, Tim',Cruise_Series=4)
df_cruise_meta = pd.DataFrame(data, index=[0])
df_ace3[~df_ace3['lat'].isnull()]
## Export cruise metadata and trajectory template
with pd.ExcelWriter(f"{vs.combined}ACE3_cruise_meta_nav_data.xlsx") as writer:  
    df_cruise_meta.to_excel(writer, sheet_name='cruise_metadata', index=False)
    df_ace3.to_excel(writer, sheet_name='cruise_trajectory', index=False)        