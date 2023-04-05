import sys
import os
from tqdm import tqdm 
import pandas as pd
import numpy as np
import xarray as xr
import sqlalchemy
import datetime
sys.path.append("../../../ingest")
sys.path.append("ingest")
from ingest import vault_structure as vs
from ingest import credentials as cr
from ingest import DB 



n =r"/data/CMAP Data Submission Dropbox/Simons CMAP/collected_data/insitu/cruise/GEOTRACES/GEOTRACES_IDP2021_Seawater_Discrete_Sample_Data_v1.nc"
## define wide table, check max columns
## additional meta tables for cruise look up, maybe _qc, _err vars
x = xr.open_dataset(n)
x.dims
x.N_STATIONS.attrs
x.attrs
x.data_vars.values
x.N_STATIONS[1]
x.N_STATIONS.values
x.N_SAMPLES.values
x.metavar5.values.tolist() #Operator's Cruise Name (KN204)

df_x1 = x.to_dataframe().reset_index()
df_x1.metavar3.unique()
df_x1['var4'].str.decode("utf-8").fillna(df_x1['var4'])
df_x.var5.unique()
df_x.var10.unique()

df_x.metavar3.unique()
df_x = x.to_dataframe().reset_index()
df_x.columns.tolist()
## find max length of string in column
str_cols = ['metavar1', 'metavar2', 'metavar3', 'metavar5', 'metavar6', 'metavar7', 'metavar8', 'metavar9', 'metavar10', 'metavar11', 'var5','var6','var7','var10','var11','var12','var13','var14']

for c in str_cols:
    print(c)
    df_x[c].str.len().max()

df_x.metavar1.str.len().max() ##6
df_x.metavar2.str.len().max() ##26
df_x.metavar3.str.len().max() ##1
df_x.metavar5.str.len().max() ##13
df_x.metavar6.str.len().max() ##20
df_x.metavar7.str.len().max() ##23
df_x.metavar8.str.len().max() ##31
df_x.metavar9.str.len().max() #77
df_x.metavar10.str.len().max() ##25
df_x.metavar11.str.len().max() ##75
df_x.var5.str.len().max() ##36



stations_g3 = x.sel(N_STATIONS=slice(298,403))
stations_g3.metavar3.values
stations_g3.var1.values
stations_g3.var1.shape
stations_g3.data_vars
stations_g3.data_vars.items()

var_names = []
col_names = []
drop_vars = []
for varname, da in stations_g3.data_vars.items():
    if da.data.dtype == 'float32' and np.isnan(da.data).all():
        drop_vars.append(varname)
        continue
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

df_varnames.to_excel(vs.download_transfer +'Geotraces_Seawater_Vars.xlsx', index=False)

for c in df_sm.columns:
    if 'N_S' in c or 'metavar' in c:
        print('['+ c + '] [nvarchar](200) NULL, ' )
    else:
        print('['+ c + '] [float] NULL, ' )

for varname, da in stations_g3.data_vars.items():
    if da.data.dtype == 'float32' and np.isnan(da.data).all():
        drop_vars.append(varname)
        continue
    try:
        v = da.attrs['long_name']
        var_names.append(v)
        col_names.append(da.attrs['standard_name'])
    except:
        print(varname)


df = stations_g3.to_dataframe().reset_index()
df.head
df.shape #73290, 1590
df_sm = df.dropna(axis=1, how='all')
df_sm.shape
df_x.columns.tolist()

## Print column names for SQL table creation script
for c in df_sm.columns:
    if 'N_S' in c or 'metavar' in c:
        print('['+ c + '] [nvarchar](200) NULL, ' )
    else:
        print('['+ c + '] [float] NULL, ' )


df_x['date_time'] = df_x['date_time'].astype('datetime64[s]')

for col, dtype in df_x.dtypes.items():
    if dtype == 'object':
        print(col)
        df_x[col] = df_x[col].str.decode("utf-8").fillna(df_x[col])
        
df_x.date_time.dtypes

df_prep = df_x.drop(columns={'N_SAMPLES','N_STATIONS','metavar1',
'metavar2',
'metavar3',
'longitude',
'latitude',
'metavar4',
'metavar5',
'metavar6',
'metavar7',
'metavar8',
'metavar9',
'metavar10',
'metavar11',
'metavar12',
'date_time'})
df_prep.columns

df_prep.N_SAMPLES.unique()
df_xml = df_prep.to_xml()

xml_data = ['<root>']
for column in df_prep.columns:
    xml_data.append('<{}>'.format(column))  # Opening element tag
    for field in df_prep.index:
        # writing sub-elements
        xml_data.append('<{0}>{1}</{0}>'.format(field, df_prep[column][field]))
    xml_data.append('</{}>'.format(column))  # Closing element tag
xml_data.append('</root>')

df_x.columns.tolist()

DB.toSQLbcp_wrapper(df_x, 'tblGeotraces_Seawater', "Beast") # 9:58am


len(var_names)/ 3
da.attrs.keys()
da.attrs['long_name']
## Check if array is only nan
np.isnan(stations_g3.var100.data).all()

np.isnan(stations_g3.var1.data).all()
type(stations_g3)
v = stations_g3.data_vars.items()
for x1 in v:
    print(x1)

