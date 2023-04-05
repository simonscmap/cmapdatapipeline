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


tbl = 'tblTN397_Gradients4_uw_par'
vs.leafStruc(vs.cruise + tbl)
raw_folder = f'{vs.cruise}{tbl}/raw/'

## PAR file from Chris, data every minute
df_par = pd.read_csv(raw_folder+'par_TN397.tsdata', names=['date','par'],skiprows=7,delimiter='\t')

df_par['datetime']=pd.to_datetime(df_par['date']).dt.tz_localize(None)
df_par.dtypes


## Raw nav data
navtbl = 'tblTN397_Gradients4_uw_tsg'
nav_folder = f'{vs.cruise}{navtbl}/raw/TN397/scs/'
nav_flist = glob.glob(nav_folder+'NAV/CNAV3050-GGA-RAW*')

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
df_nav['datetime']=df_nav['date']+' '+df_nav['time_hms']
df_nav['datetime']=pd.to_datetime(df_nav['datetime'], format='%m/%d/%Y %H:%M:%S')

df_nav.dtypes

## Round to nearest minute to match PAR
df_nav['datetime_round'] = df_nav['datetime'].dt.round('1min')

## Drop duplicate lat/lon for join 
df_nav.drop_duplicates(['datetime_round'],keep= 'first', inplace=True)
# df_geotab.drop_duplicates(['datetime_round'],keep= 'first', inplace=True)

## Resample
df_nav.index = pd.to_datetime(df_nav.datetime)
rs_df = df_nav.resample('1min').mean()
rs_df = rs_df.dropna()
rs_df.reset_index(inplace=True)
rs_df.columns

# 586 nulls
df_par_rs_nav = pd.merge(df_par, rs_df, how='left', left_on='datetime', right_on='datetime')
df_par_rs_nav[df_par_rs_nav['lat'].isna()]

# 585 nulls
df_par_nav = pd.merge(df_par, df_nav, how='left', left_on='datetime', right_on='datetime_round')
df_par.shape
df_par_nav.shape
df_par_nav.columns
df_par_ingest = df_par_nav[~df_par_nav['lat'].isnull()]
df_par_ingest['time'] = df_par_ingest['datetime_x'].dt.strftime('%Y-%m-%dT%H:%M:%S')
df_par_ingest[['time', 'lat', 'lon', 'par']].to_excel(raw_folder+'par_data.xlsx', index=False)

df_null = df_par_nav[df_par_nav['lat'].isna()]
df_null.datetime_x.dt.date.unique()


# 1019 nulls
df_par_geo= pd.merge(df_par, df_geotab, how='left', left_on='datetime', right_on='datetime_round')
df_null2 = df_par_geo[df_par_geo['lat'].isna()]
df_null2.datetime.dt.date.unique()

