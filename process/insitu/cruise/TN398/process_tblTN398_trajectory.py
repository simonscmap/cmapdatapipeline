import os
import sys
import glob
import pandas as pd
import numpy as np
from zipfile import ZipFile

sys.path.append("cmapdata/ingest")

from ingest import vault_structure as vs
from ingest import credentials as cr
from ingest import cruise
from ingest import SQL
from ingest import DB



tbl = 'tblTN398_uw_tsg'
vs.leafStruc(vs.cruise+tbl)
raw_folder = f'{vs.cruise}{tbl}/raw/'
rep_folder = f'{vs.cruise}{tbl}/rep/'


with ZipFile(f"{raw_folder}/NAV.zip","r") as zip_ref:
    zip_ref.extractall(raw_folder)

with ZipFile(f"{raw_folder}/TSG-RAW.zip","r") as zip_ref:
    zip_ref.extractall(raw_folder)

nav_flist = glob.glob(raw_folder+'CNAV3050-GGA-RAW*')
tsg_flist = glob.glob(raw_folder+'TSG-RAW_*')


def dm(x):
    degrees = int(x) // 100
    minutes = x - 100*degrees

    return degrees, minutes

def decimal_degrees(degrees, minutes):
    return degrees + minutes/60 

df_nav =  pd.DataFrame(columns=['date','time_hms','time_ms','time','lat','lon'])

for nav in nav_flist:
    temp_nav = pd.read_csv(nav, names = ['date','time','GPGGA','time_utc','lat_dd','lat_dir','lon_ddd', 'lon_dir', 'qual', 'sat_count', 'hdop', 'antenna_alt', 'antenna_dir','geo', 'geo_dir', 'data_age','differential'])
    # convert lat units: DDMM.MMMM
    temp_nav['lat'] = temp_nav['lat_dd'].map(lambda x: decimal_degrees(*dm(x)))
    ## multiply by -1 if lat is S
    dict_lat = {'N':1, 'S':-1}
    temp_nav['lat'] =temp_nav['lat'].mul(temp_nav['lat_dir'].map(dict_lat)).fillna(temp_nav['lat'])
    # convert lon units: DDDMM.MMMM
    temp_nav['lon'] =  temp_nav['lon_ddd'].map(lambda x: decimal_degrees(*dm(x)))
    ## multiply by -1 if lon is W
    dict_lon = {'E':1, 'W':-1}
    temp_nav['lon'] =temp_nav['lon'].mul(temp_nav['lon_dir'].map(dict_lon)).fillna(temp_nav['lon'])
    ## split time field for join
    temp_nav[['time_hms','time_ms']] = temp_nav['time'].str.split('.', expand=True)
    df_nav = df_nav.append(temp_nav[['date','time_hms','time_ms','time','lat','lon']])

df_nav['datetime']=df_nav['date']+' '+df_nav['time_hms']+'.'+df_nav['time_ms']
df_nav['datetime']=pd.to_datetime(df_nav['datetime'], format='%m/%d/%Y %H:%M:%S.%f')    

df_tsg = pd.DataFrame(columns=['date','time','temp','cond','sal'])
for tsg in tsg_flist:
    temp_tsg = pd.read_csv(tsg, names=['date','time','temp','cond','sal'])
    if len(temp_tsg) == 0:
        continue 
    temp_tsg[['time_hms','time_ms']] = temp_tsg['time'].str.split('.', expand=True)
    df_tsg = df_tsg.append(temp_tsg)

df_nav.index = pd.to_datetime(df_nav.datetime)
rs_df = df_nav.resample('1s').mean()
rs_df = rs_df.dropna()
rs_df.reset_index(inplace=True)

df_tsg['datetime']=df_tsg['date']+' '+df_tsg['time_hms']+'.'+df_tsg['time_ms']
df_tsg['datetime']=pd.to_datetime(df_tsg['datetime'], format='%m/%d/%Y %H:%M:%S.%f')
df_tsg.index = pd.to_datetime(df_tsg.datetime)
df_tsg_1sec = df_tsg.resample('1s').mean()
df_tsg_1sec = df_tsg_1sec.dropna()
df_tsg_1sec.reset_index(inplace=True)

df = pd.merge(df_tsg_1sec, rs_df, how='left', on="datetime")
df_import = df[~df['lat'].isnull()]
df_import.rename(columns={'datetime':'time', 'temp':'SST', 'cond':'conductivity', 'sal':'salinity'}, inplace=True)
df_import[['time','lat','lon','SST','conductivity','salinity']].to_csv(rep_folder+'TN398_uw_tsg.csv', index=False)

df = pd.read_csv(rep_folder+'TN398_uw_tsg.csv')
df.to_csv('TN398_uw_tsg.csv', index=False)

server = 'Rossby'
SQL.full_SQL_suggestion_build(df_import, tbl, 'cruise', server, 'Opedia')
DB.toSQLbcp_wrapper(df_import, tbl, server)

## Build cruise_metadata sheet
rs_df.rename(columns={'datetime':'time'}, inplace=True)
df_tblCruise, cruise_name = cruise.build_cruise_metadata_from_user_input(df_import[['time','lat','lon']])
df_cruise_meta = df_tblCruise[['Nickname', 'Name', 'Ship_Name', 'Chief_Name']]
df_cruise_meta['Cruise_Series'] = None
rs_df[~rs_df['lat'].isnull()]
## Export cruise metadata and trajectory template
with pd.ExcelWriter(f"{vs.combined}TN398_cruise_meta_nav_data.xlsx") as writer:  
    df_cruise_meta.to_excel(writer, sheet_name='cruise_metadata', index=False)
    df_import[['time','lat','lon']].to_excel(writer, sheet_name='cruise_trajectory', index=False)

df_cruise_meta = pd.read_excel(f"{vs.combined}TN398_cruise_meta_nav_data.xlsx", sheet_name='cruise_metadata')
df.to_csv('TN398_uw_tsg.csv', index=False)