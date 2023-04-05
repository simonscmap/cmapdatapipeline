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
from ingest import SQL
from ingest import DB

# os.path.expanduser('~')
# os.chdir('/home/exx/Documents/CMAP/cmapdata/ingest')

tbl = 'tblTN397_Gradients4_uw_tsg'
raw_folder = f'{vs.cruise}{tbl}/raw/'
rep_folder = f'{vs.cruise}{tbl}/rep/'
meta_folder = f'{vs.cruise}{tbl}/metadata/'

## New files from Chris
df_geotab = pd.read_csv(raw_folder+'TN397-geo.tab', names=['date','lat','lon','temp','cond','sal'],skiprows=7,delimiter='\t')
df_geotab['time']=pd.to_datetime(df_geotab['date'], format='%Y-%m-%dT%H:%M:%S.%fZ')
df_geotab.shape

df_geotab[['time','lat','lon','temp','cond','sal']].to_excel(raw_folder+'tsg_geotab_data.xlsx', index=False)


## Below is testing against raw TSG file. TSG is than Chris' (ship-provided) TSG pre-joined with location

base_folder = f'{vs.cruise}{tbl}/raw/TN397/scs/'

tsg_flist = glob.glob(base_folder+'SEAWATER/TSG-RAW*')
nav_flist = glob.glob(base_folder+'NAV/CNAV3050-GGA-RAW*')

tsg = tsg_flist[0]

df_tsg = pd.DataFrame(columns=['date','time_hms','time_ms','time','temp','cond','sal'])
for tsg in tsg_flist:
    temp_tsg = pd.read_csv(tsg, names=['date','time','temp','cond','sal'])
    temp_tsg[['time_hms','time_ms']] = temp_tsg['time'].str.split('.', expand=True)
    df_tsg = df_tsg.append(temp_tsg)

nav = nav_flist[0]

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
## Dropping milliseconds instead of rounding gives 9146 nulls
## Rounding to 1s gives 9126 nulls, 2,278,547 data points with location
## Rounding to 1.1s gives 72 nulls, 2,080,010 data points with location
## Rounding to 2s has no nulls, 1,144,064 data points with location
# df_nav_1sec = df_nav[['datetime','lat','lon']].drop_duplicates(keep='first')
df_nav.index = pd.to_datetime(df_nav.datetime)
rs_df = df_nav.resample('1s').mean()
rs_df = rs_df.dropna()
rs_df.reset_index(inplace=True)

df_tsg['datetime']=df_tsg['date']+' '+df_tsg['time_hms']+'.'+df_tsg['time_ms']
df_tsg['datetime']=pd.to_datetime(df_tsg['datetime'], format='%m/%d/%Y %H:%M:%S.%f')
#df_tsg_1sec = df_tsg[['datetime','temp','cond','sal']].drop_duplicates(keep='first')
df_tsg.index = pd.to_datetime(df_tsg.datetime)
df_tsg_1sec = df_tsg.resample('1s').mean()
df_tsg_1sec = df_tsg_1sec.dropna()
df_tsg_1sec.reset_index(inplace=True)

df = pd.merge(df_tsg_1sec, rs_df, how='left', on="datetime")
df_import = df[~df['lat'].isnull()]
df_import.rename(columns={'datetime':'time', 'temp':'SST', 'cond':'conductivity', 'sal':'salinity'}, inplace=True)
df_import[['time','lat','lon','SST','conductivity','salinity']].to_csv(rep_folder+'TN397_uw_tsg.csv', index=False)


## After review ingest data
df = pd.read_csv(rep_folder+'TN397_uw_tsg.csv')
server = 'Rossby'
branch='observation'
db_name='Opedia'
SQL.full_SQL_suggestion_build(df, tbl, branch, server, db_name)
DB.toSQLbcp_wrapper(df, tbl, server)
## Ingest metadata


## Null check
df_null = df[df['lat'].isna()]
df_null['datetime'].dt.date.unique()


## Downsample for trajectory
df_nav.index = pd.to_datetime(df_nav.datetime)
rs_df = df_nav.resample('1min').mean()
rs_df = rs_df.dropna()
rs_df.reset_index(inplace=True)
rs_df['time'] = rs_df['datetime'].dt.strftime('%Y-%m-%dT%H:%M:%S')


## Drop duplicate lat/lon for join 
df_nav.drop_duplicates(['date','time_hms'],keep= 'first', inplace=True)

## Sort to prepare for nearest join 
## Join without nearest left ~9000 without lat/lon
df_nav.sort_values('datetime', inplace=True)
df_tsg.sort_values('datetime', inplace=True)
df = pd.merge_asof(df_tsg[['datetime','temp','cond','sal']], df_nav[['datetime','lat','lon']], on="datetime", direction='nearest')

# df = pd.merge(df_tsg, df_nav, how='left', on=['date','time_hms'], validate = 'm:1')
df.head
df.shape

## Null check
df_null = df[df['lat'].isna()]

## Downsample for trajectory
df.index = pd.to_datetime(df.datetime)
rs_df = df.resample('1min').mean()
rs_df = rs_df.dropna()
rs_df.reset_index(inplace=True)
rs_df['time'] = rs_df['datetime'].dt.strftime('%Y-%m-%dT%H:%M:%S')

## Downsample chris' version
df_geotab.index = pd.to_datetime(df_geotab.time)
rs_df = df_geotab.resample('1min').mean()
rs_df = rs_df.dropna()
rs_df.reset_index(inplace=True)
rs_df['datetime'] = rs_df['time'].dt.strftime('%Y-%m-%dT%H:%M:%S')
rs_df.to_excel(f'{vs.cruise}{tbl}/metadata/chris_tsg_traj.xlsx', index=False)



## Map check
# fig = px.scatter_geo(rs_df,lat='lat',lon='lon', hover_name="datetime")
# fig.show()

## Check against Chris' file
df_tsg[df_tsg.date=='12/07/2021']
df_tsg[(df_tsg.date=='12/07/2021') & (df_tsg.time_hms == '00:00:05')]
df_geotab.query('date.str.startswith("2021-11-21T16:3")', engine='python')
df_nav[(df_nav.date=='11/23/2021')]
## Round to nearest millisecond ('100L') 148,609 nulls
## Round to nearest second ('1s') 14,227 nulls
df_tsg['datetime_round'] = df_tsg['datetime'].dt.round('100L')
df_geotab['datetime_round'] = df_geotab['time'].dt.round('100L')

## Join on nearest millisecond doesn't have matching data (off by ~.001)
df_check = pd.merge(df_geotab, df_tsg, how='left', on='datetime_round')
df_check[~df_check['datetime'].isnull()]
df_check[df_check['datetime'].isna()]

df_nav.dtypes
df_geotab.dtypes
df_tsg.dtypes
df_tsg.shape
df_geotab.shape
df_nav.shape