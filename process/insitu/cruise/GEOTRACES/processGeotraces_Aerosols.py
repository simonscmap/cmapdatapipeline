import sys
import os

import pandas as pd
import numpy as np
import xarray as xr

sys.path.append("../../../ingest")
sys.path.append("ingest")
import vault_structure as vs
import credentials as cr
import DB 
import data_checks as dc


tbl = 'tblGeotraces_Aerosols'
n = f'{vs.cruise}{tbl}/raw/GEOTRACES_IDP2021_Aerosol_Data_v1.nc' 

x = xr.open_dataset(n)

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

df_varnames.to_excel(vs.download_transfer +'Geotraces_Aerosol_Variables.xlsx', index=False)

x.dims 
x.date_time.attrs
x.metavar1[700:800]
x.date_time[700:800]
nulls = x.where(x.date_time.isnull())
df_null = nulls.to_dataframe().reset_index() 

df_x = x.to_dataframe().reset_index()

df_x.N_STATIONS.unique()
df_x.columns
df_x.dtypes
df_x[df_x['date_time'].isna()]['date_time']
df_x['date_time'] = df_x['date_time'].astype('datetime64[s]')

for col, dtype in df_x.dtypes.items():
    if dtype == 'object':
        print(col)
        df_x[col] = df_x[col].str.decode("utf-8").fillna(df_x[col])
        

df_x['metavar5'].str.len().max()
df_x.rename(columns={'latitude':'lat','longitude':'lon', 'date_time':'time'}, inplace=True)
        
df_x.shape #(810, 506)

for c in df_x.columns:
    if 'N_S' in c:
        print('['+ c + '] [int] NULL, ' )
    elif df_x[c].dtype == 'object':
        print('['+ c + '] [nvarchar](' + str(df_x[c].str.len().max()) + ') NULL, ' )
    else:
        print('['+ c + '] [float] NULL, ' )

## Column order of df has to match SQL table for check to work
df_x = df_x[['time', 'lat', 'lon', 'N_SAMPLES', 'N_STATIONS', 'metavar1', 'metavar2', 'metavar3',
       'metavar4', 'metavar5',  'var1', 'var1_qc', 'var2',
       'var2_qc', 'var3', 'var3_qc', 'var4', 'var4_qc', 'var5', 'var5_qc',
       'var6', 'var6_qc', 'var7', 'var7_qc', 'var8', 'var8_qc', 'var9',
       'var9_qc', 'var10', 'var10_qc']]
dc.check_df_ingest(df_x, 'tblGeotraces_Aerosol', "Beast")
df_x[df_x['time'].isna()] #32,082,204

df_import_1 = dc.mapTo180180(df_x)

dc.check_df_ingest(df_import_1, 'tblGeotraces_Aerosol', "Beast")


df_clean_1 = dc.clean_data_df(df_import_1)
dc.check_df_ingest(df_clean_1, 'tblGeotraces_Aerosol', "Beast")

df_clean_1.columns

# DB.toSQLbcp_wrapper(df_clean_1, 'tblGeotraces_Aerosol', "Beast") 


# query = '''
# INSERT INTO dbo.tblGeotraces_Seawater (
#       [time]
#       ,[lat]
#       ,[lon]
#       ,[N_Samples]
#       ,[N_Stations]
      

# '''

# DB.queryExecute("Beast", query)





