import os
import sys
import glob
import pandas as pd
import numpy as np
import plotly.express as px

sys.path.append("ingest")

from ingest import vault_structure as vs
from ingest import credentials as cr
from ingest import cruise



tbl = 'tblTN397_Gradients4_uw_tsg'
raw_folder = f'{vs.cruise}{tbl}/raw/'


base_folder = f'{vs.cruise}{tbl}/raw/TN397/scs/'

nav_flist = glob.glob(base_folder+'NAV/CNAV3050-GGA-RAW*')


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

## Joining with milliseconds gives >2million nulls
df_nav['datetime']=df_nav['date']+' '+df_nav['time_hms']+'.'+df_nav['time_ms']
df_nav['datetime']=pd.to_datetime(df_nav['datetime'], format='%m/%d/%Y %H:%M:%S.%f')

## Downsample for trajectory
df_nav.index = pd.to_datetime(df_nav.datetime)
rs_df = df_nav.resample('1min').mean()
rs_df = rs_df.dropna()
rs_df.reset_index(inplace=True)
rs_df['time'] = rs_df['datetime'].dt.strftime('%Y-%m-%dT%H:%M:%S')

## Build cruise_metadata sheet
df_tblCruise, cruise_name = cruise.build_cruise_metadata_from_user_input(rs_df)
df_cruise_meta = df_tblCruise[['Nickname', 'Name', 'Ship_Name', 'Chief_Name']]
df_cruise_meta['Cruise_Series'] = 1

## Export cruise metadata and trajectory template
with pd.ExcelWriter(f"{vs.combined}TN397_cruise_meta_nav_data.xlsx") as writer:  
    df_cruise_meta.to_excel(writer, sheet_name='cruise_metadata', index=False)
    rs_df[['time','lat','lon']].to_excel(writer, sheet_name='cruise_trajectory', index=False)


