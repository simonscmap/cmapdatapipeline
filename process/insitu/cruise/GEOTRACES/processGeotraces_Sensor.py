import sys
import os
import tqdm

import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
import pandas as pd
import numpy as np
import xarray as xr

sys.path.append("cmapdata/ingest")
sys.path.append("ingest")
from ingest import vault_structure as vs
from ingest import DB 
from ingest import stats
from ingest import data

server = 'Beast'
tbl = 'tblGeotraces_Sensor'
n = f'{vs.cruise}{tbl}/raw/GEOTRACES_IDP2021_Seawater_Sensor_Data_v1.nc' 
raw_folder = f'{vs.cruise}{tbl}/raw/'

xl = f'{vs.cruise}{tbl}/metadata/Sensor_Datetime_Join.xlsx' 


x = xr.open_dataset(n)
x = xr.open_dataset(n, chunks={'N_STATIONS': 100})
# df_all = x.to_dataframe().reset_index()

var_names = []
col_names = []
for varname, da in x.data_vars.items():
    try:
        v = da.attrs['long_name']
        fl_val = da.attrs['flag_values'].tolist()
        fl_def = da.attrs['flag_meanings']
        var_names.append(v)
        col_names.append(da.attrs['standard_name'])
    except:
        print(varname)

d1 = {'var_name':[], 'std_name':[], 'long_name':[], 'dtype':[], 'units':[], 'comment':[], 'flag_val':[], 'flag_def':[]}
df_varnames = pd.DataFrame(data=d1)

for varname, da in x.data_vars.items():
    dtype = da.data.dtype
    if 'flag_values' in da.attrs.keys():
        fl_val = da.attrs['flag_values'].tolist()
        fl_def = da.attrs['flag_meanings']
    else:
        fl_val = None
        fl_def = None
    if 'long_name' in da.attrs.keys():
        long_name = da.attrs['long_name']
    else:
        long_name = None
    if 'standard_name' in da.attrs.keys():
        std_name = da.attrs['standard_name']
    else:
        std_name = None      
    if 'comment' in da.attrs.keys():
        comment = da.attrs['comment']
    else:
        comment = None   
    if 'units' in da.attrs.keys():
        units = da.attrs['units']
    else:
        units = None          

    d1 = {'var_name':[varname], 'std_name':[std_name], 'long_name':[long_name], 'dtype':[dtype], 'units':[units], 'comment':[comment], 'flag_val':[fl_val], 'flag_def':[fl_def]}
    temp_df = pd.DataFrame(data=d1)
    df_varnames = df_varnames.append(temp_df, ignore_index=True)

df_varnames.to_excel(vs.download_transfer +'Geotraces_Sensor_Vars.xlsx', index=False)

x.dims 
x.data_vars
for dv in x.data_vars:
    print(dv)

sub = x.isel(N_SAMPLES=0)
df_sub = sub.to_dataframe().reset_index()
df_sub.to_excel(raw_folder+'sub_test.xlsx', index=False)
x.date_time.attrs
x.metavar1[700:800]
x.date_time[700:800]
x.date_time.max()
x.metavar5.values.tolist()
np.unique(x['metavar5'])
nulls = x.where(x.metavar5==b'2012-13')
nulls = x.where(x.date_time.isnull())

var_list = ['metavar1', 'metavar2', 'metavar3',
        'metavar4', 'metavar5',  'var1', 'var1_qc', 'var2',
        'var2_qc', 'var3', 'var3_qc', 'var4', 'var4_qc', 'var5', 'var5_qc',
        'var6', 'var6_qc', 'var7', 'var7_qc', 'var8', 'var8_qc', 'var9',
        'var9_qc', 'var10', 'var10_qc']
cruise_list = [b'2013-18',b'2014-19',b'ArcticNet1502',b'ArcticNet1503',b'AU0703',b'AU0806',b'AU1603',b'D357',b'GEOVIDE',b'HLY1502',b'IN2016_V01',b'IN2016_V02',b'ISSS-08',b'JC057',b'JC068',b'JC150',b'JC156',b'KH09-05',b'KH11-07',b'KH12-04',b'KH14-06',b'KH15-03',b'KH17-03',b'KN199',b'KN204',b'M81_1',b'MD166',b'MD188',b'MedSeA2013',b'NP1409',b'PANDORA',b'PE319',b'PE321',b'PE370',b'PE373',b'PE374',b'PEACETIME',b'PS100',b'PS70',b'PS71',b'PS94',b'RR1814',b'RR1815',b'SK304',b'SK311',b'SK312',b'SK324',b'SO245',b'SS01_10',b'SS2011',b'TAN1109',b'TN303',b'RR1814 | RR1815']
for c in cruise_list:
    print(c)
    nulls = x.where(x.metavar5==c)
    for v in var_list:
        if np.count_nonzero(~pd.isna(nulls[v].values)) > 0:
            print(v)
    print(nulls[v].size)
    print(np.count_nonzero(~pd.isna(nulls[v].values)))
nulls.var1.size
nulls.metavar1.values.tolist()
len(nulls.metavar1.values.tolist())
np.count_nonzero(~np.isnan(nulls.var6.values))
nulls.metavar5.values.tolist()
nulls.data_vars
df_null = nulls.to_dataframe().reset_index() #243047000
## MemoryError: Unable to allocate 81.7 GiB for an array with shape (23, 952987287)
# df_x = x.to_dataframe().reset_index()

# D357 = x.where(x.metavar5 == 'D357', drop=True)
####### Group by, min max lat long group by station sample
# df_D357 = D357.to_dataframe().reset_index()

cruise_groups = x.groupby("metavar5").groups
for cruise, stations in cruise_groups.items():
    print(cruise)
    print(stations)

## 3921 stations
x_1 = x.sel(N_STATIONS=slice(0,100)) 
test = x.where(x.metavar5=='TN303')
test_drop = x.load().where(x.metavar5=='TN303', drop = True)

test = x.loc[stations,:]
test = x.sel(N_STATIONS=(list(stations), [0]))
test = x.where(x.N_STATIONS in stations, drop = True).all()
test = x.sel(N_STATIONS= stations)

test.metavar5.values.tolist()
test.N_STATIONS.values.tolist()


time_count_stations = x.groupby("date_time").count(dim="N_STATIONS")
cruise_count_stations = x.groupby("metavar5").count(dim="N_STATIONS") 

no_samples = x.drop_dims('N_SAMPLES')
no_samples.dims
no_samples.data_vars

df_ns = no_samples.to_dataframe().reset_index()
df_ns.shape
df_ns.columns



# ## Create SQL table syntax
# for c in df_x.columns:
#     if 'N_S' in c:
#         print('['+ c + '] [int] NULL, ' )
#     elif df_x[c].dtype == 'object':
#         print('['+ c + '] [nvarchar](' + str(df_x[c].str.len().max()) + ') NULL, ' )
#     else:
#         print('['+ c + '] [float] NULL, ' )



### Import by column due to memory error (56.8 GB)
x_1 = x['metavar1']
df_1 = x_1.to_dataframe().reset_index()
x_1 = x['metavar2']
df_2 =x_1.to_dataframe().reset_index()
df = pd.merge(df_1, df_2, how='left', on='N_STATIONS')
var_list = ['metavar3','date_time','latitude','longitude','metavar4','metavar5']
for var in var_list:
    x_1 = x[var]
    df_v= x_1.to_dataframe().reset_index()
    df = pd.merge(df, df_v, how='left', on='N_STATIONS')
# df.to_excel(raw_folder+'Sensor_Stations.xlsx', index = False)
qry_dt = "SELECT Operators_Cruise_Name, N_Stations, time as time_fix FROM tblGeotraces_Sensor_datetime"
df_dt = DB.dbRead(qry_dt, 'Beast')

df['date_time'] = df['date_time'].astype('datetime64[s]')

for col, dtype in df.dtypes.items():
    if dtype == 'object':
        print(col)
        df[col] = df[col].str.decode("utf-8").fillna(df[col])

df.rename(columns={'latitude':'lat','longitude':'lon', 'date_time':'time'}, inplace=True)
df_time = pd.merge(df, df_dt, how='left', left_on=['N_STATIONS','metavar5'], right_on=['N_Stations','Operators_Cruise_Name'])
df_time["time_fill"] = df_time["time"].fillna(df_time["time_fix"])
len(df_time[df_time['time_fill'].isna()]) == 0
df_time = df_time[['time_fill', 'lat', 'lon', 'N_STATIONS', 'metavar1', 'metavar2', 'metavar3','metavar4', 'metavar5']]


x_1 = x['var1']
df_v1= x_1.to_dataframe().reset_index()
x_1 = x['var1_qc']
df_v1q= x_1.to_dataframe().reset_index()
df_samples = pd.merge(df_v1,df_v1q,how='left', on=['N_STATIONS','N_SAMPLES'])
df_samples_n = df_samples.dropna(subset=['var1', 'var1_qc'], thresh=2)
len(df_samples[~df_samples['var1'].isna()]) 

s_list = ['var2','var3', 'var4','var5','var6','var7','var8']

## held through var9, not var9qc
var = ''
for var in s_list:
    x_1 = x[var]
    df_v1= x_1.to_dataframe().reset_index()
    df_samples = pd.merge(df_samples, df_v1,how='left', on=['N_STATIONS','N_SAMPLES'])
    print(var)
    qc=var+'_qc'
    x_1 = x[qc]
    df_v1q= x_1.to_dataframe().reset_index()
    df_samples = pd.merge(df_samples, df_v1q,how='left', on=['N_STATIONS','N_SAMPLES'])
    print(qc)
DB.toSQLbcp_wrapper(df_samples, 'tblGeotraces_Sensor1_8', 'Beast')

s_list = ['var9','var10']
for var in s_list:
    x_1 = x[var]
    df_v1= x_1.to_dataframe().reset_index()
    df_samples = pd.merge(df_samples, df_v1,how='left', on=['N_STATIONS','N_SAMPLES'])
    print(var)
    qc=var+'_qc'
    x_1 = x[qc]
    df_v1q= x_1.to_dataframe().reset_index()
    df_samples = pd.merge(df_samples, df_v1q,how='left', on=['N_STATIONS','N_SAMPLES'])

df_import = pd.merge(df_time, df_samples, how='left', on='N_STATIONS')

# null_check = dc.check_df_nulls(df_import, 'tbl15673890', "Rainier")
df_import_1 = data.mapTo180180(df_import)
df_import_1 = df_import_1[['time_fill', 'lat', 'lon', 'N_STATIONS', 'N_SAMPLES', 'metavar1', 'metavar2',
       'metavar3', 'metavar4', 'metavar5', 'var9_qc', 'var10',
       'var10_qc']]
if len(df_import_1) != len(df_import_1.groupby(['time_fill', 'lat', 'lon', 'N_SAMPLES', 'N_STATIONS']).count()):
    print(f'{crs} not unique')

DB.toSQLbcp_wrapper(df_import_1, 'tblGeotraces_Sensor9_10', 'Beast')
df_import_1.dtypes


## Fill in missing datetimes
df_xl = pd.read_excel(xl, sheet_name = 'SQL_Import')
DB.toSQLpandas(df_xl, 'tblGeotraces_Sensor_datetime', 'Beast')
cruise_list = df_xl['Operators_Cruise_Name'].unique().tolist()
crs = cruise_list[0]
qry = f"SELECT * from tblGeotraces_Sensor where operators_cruise_name='{crs}'"
df_fix = DB.dbRead(qry, 'Beast')
df_fix.drop(columns='time', inplace=True)
df_fix.dtypes


df_xl['time'] = df_xl['time'].dt.date
df_join = pd.merge(df_fix, df_xl, how='left', on=['Operators_Cruise_Name','N_Stations'])
df_join.dtypes



## zero index used as test
i = 1
while i < len(df_dt):
    qry_s = f"UPDATE tblGeotraces_Sensor SET time = '{df_dt['time'].loc[i]}' where operators_cruise_name = '{df_dt['Operators_Cruise_Name'].loc[i]}' and N_stations={df_dt['N_Stations'][i]}"
    DB.queryExecute(server,qry_s)
    print(df_dt['Operators_Cruise_Name'].loc[i]+': '+str(df_dt['N_Stations'][i]))
    i += 1

# DB.toSQLbcp_wrapper(df_clean_1, 'tblGeotraces_Seawater_1', "Beast") 
# DB.toSQLbcp_wrapper(df_clean_2, 'tblGeotraces_Seawater_2', "Beast") 


stats_df = stats.build_stats_df_from_db_calls(tbl, 'Mariana')
# fix var6, add depth
stats_df['depth'] = stats_df['CTDPRS_T_VALUE_SENSOR']
df_s_stats = df_sample_stats.reindex(['count','max','mean','min','std','25%','50%','75%'])
df_s_stats.drop(df_s_stats.tail(3).index,inplace=True)
stats_df['CTDFLUOR_T_VALUE_SENSOR'] = df_s_stats['var6']

stats.update_stats_large(tbl, stats_df, 'Opedia', 'Rainier')


## Transfer from Rainier
qry= f'SELECT * FROM {tbl}'
df = DB.dbRead(qry, 'Mariana')
DB.toSQLbcp_wrapper(df, tbl, 'Mariana')

## Change flag values from ASCII to int
flag_defs = {48:'0', 49:'1', 50:'2', 51:'3', 52:'4', 53:'5', 54:'6', 55:'7', 56:'8', 57:'9', 65:'A', 66:'B', 81:'Q'}
qc_vars = [q for q in df.columns.tolist() if '_qc' in q]

server = 'Mariana'
for qc_v in qc_vars: 
    qry = f"alter table dbo.{tbl} alter column {qc_v} varchar(2) null"
    print(qry)
    DB.queryExecute(server, qry)

for qc_v in qc_vars: 
    for i, v in flag_defs.items():
        qry = f"update {tbl} set {qc_v} = '{v}' where {qc_v} = '{i}'"
        print(qry)
        DB.queryExecute(server, qry)


