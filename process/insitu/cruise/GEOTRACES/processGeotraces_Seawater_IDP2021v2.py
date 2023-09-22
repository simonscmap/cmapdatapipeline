"""
Author: Diana Haring <dharing@uw.edu>
Date: 08-16-2021

Script to process raw Geotraces Seawater IDP2021v2 data
"""
import sys


import pandas as pd
import numpy as np
import xarray as xr

sys.path.append("ingest")
sys.path.append("cmapdata/ingest")
import vault_structure as vs
import credentials as cr
import DB 
import data
import common as cmn
import SQL
import stats

tbl = 'tblGeotraces_Seawater_IDP2021v2'
meta_folder = f'{vs.cruise}{tbl}/metadata/'
n = f'{vs.cruise}{tbl}/raw/GEOTRACES_IDP2021_Seawater_Discrete_Sample_Data_v2.nc' 

x = xr.open_dataset(n)
# x.attrs
# x.N_STATIONS
# ## Example of cut off metadata. Full description found in metadata PDF
# x.EMBL_EBI_Metagenome_MGNIFY_Analysis_Accession.attrs


### Variable names changed from v1 to v2
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

df_varnames.to_excel(meta_folder +'Geotraces_Seawater_Vars.xlsx', index=False)

df_x = x.to_dataframe().reset_index()

df_x['date_time'] = df_x['date_time'].astype('datetime64[s]')

for col, dtype in df_x.dtypes.items():
    if dtype == 'object':
        print(col)
        df_x[col] = df_x[col].str.decode("utf-8").fillna(df_x[col])

df_x['Chief_Scientist'] = df_x['Chief_Scientist'].str.replace(',',';')
df_x['GEOTRACES_Scientist'] = df_x['GEOTRACES_Scientist'].str.replace(',',';')
df_x['Cruise_Information_Link'] = df_x['Cruise_Information_Link'].str.replace(',',';')

df_x.rename(columns={'latitude':'lat','longitude':'lon', 'date_time':'time'}, inplace=True)
        
df_x.shape # v1(2,198,002, 1590), v2 (2,346,676, 1189)

df_subset = df_x[['cruise_id','Operator_s_Cruise_Name','Cruise_Aliases','time','lat','lon']].drop_duplicates()
df_subset.to_excel(meta_folder +'Geotraces_Seawater_UniqueCruiseTime.xlsx', index=False)

## Change flag values from ASCII to int
flag_defs = {'48.0':'0', '49.0':'1', '50.0':'2', '51.0':'3', '52.0':'4', '53.0':'5', '54.0':'6', '55.0':'7', '56.0':'8', '57.0':'9', '65.0':'A', '66.0':'B', '81.0':'Q'}
qc_vars = [q for q in df_x.columns.tolist() if '_qc' in q]
for q in qc_vars:
      df_x[q] = df_x[q].astype(str)
      df_x[q] = df_x[q].map(flag_defs).fillna(df_x[q])

for q in qc_vars:
      df_x[q] = df_x[q].replace('nan', '')
for q in qc_vars:
      df_x[q] = df_x[q].replace(np.nan, '')
## Rename depth column because there are nulls
df_x.rename(columns={'DEPTH':'DEPTH_SENSOR','DEPTH_qc':'DEPTH_SENSOR_qc'},inplace=True)

## Find missing time, lat, lon
df_unique = df_x[['time','lat','lon','Operator_s_Cruise_Name']].drop_duplicates()
df_unique.isnull().values.any()

meta_cols = ['time', 'lat', 'lon', 'N_SAMPLES', 'N_STATIONS', 'cruise_id', 'station_id', 'station_type', 'Bot__Depth', 'Operator_s_Cruise_Name', 'Ship_Name', 'Period', 'Chief_Scientist', 'GEOTRACES_Scientist', 'Cruise_Aliases', 'Cruise_Information_Link', 'BODC_Cruise_Number']

sug_df = pd.DataFrame(columns=["column_name", "dtype","null_status"])
for cn in meta_cols:
    if cn == "time":
        col_dtype = "[datetime]"
        null_status = "NOT NULL,"
    elif df_x[cn].dtype=='O':
        max_len = df_x[cn].map(len).max()
        col_dtype = "[nvarchar]("+str(max_len)+")"
        null_status = "NULL,"
    elif cn in ['N_SAMPLES', 'N_STATIONS']:
        col_dtype = "[int]"
        null_status = "NULL,"
    elif cn in ['lat', 'lon']:
        col_dtype = "[float]"
        null_status = "NOT NULL,"
    else:
        col_dtype = "[float]"
        null_status = "NULL,"
    c_str = "["+cn+"]"
    sug_list = [c_str, col_dtype, null_status]
    sug_df.loc[len(sug_df)] = sug_list
sug_list = ['CSet xml column_set for all_sparse_columns,', '','']
sug_df.loc[len(sug_df)] = sug_list
for cn in [c for c in list(df_x) if c not in meta_cols]:
    if cn == 'Zn_CELL_CONC_BOTTLE_qc':
        max_len = df_x[cn].astype(str).map(len).max()
        col_dtype = "[nvarchar]("+str(max_len)+") SPARSE"        
    elif df_x[cn].dtype=='O' in cn or '_qc' in cn:
        max_len = df_x[cn].astype(str).map(len).max()
        col_dtype = "[nvarchar]("+str(max_len)+") SPARSE,"
    else:
        col_dtype = "[float] SPARSE,"
    c_str = "["+cn+"]"
    sug_list = [c_str, col_dtype,'']
    sug_df.loc[len(sug_df)] = sug_list
var_string = sug_df.to_string(header=False, index=False)  
""" Print table as SQL format """
SQL_tbl = f"""
USE [Opedia]

SET ANSI_NULLS ON


SET QUOTED_IDENTIFIER ON


CREATE TABLE [dbo].[{tbl}](

{var_string}


) ON [FG3]"""
server = 'Mariana'
server = 'Rainier'
sql_dict = {"sql_tbl": SQL_tbl}  
SQL_index_dir = SQL.SQL_index_suggestion_formatter(df_x, tbl, server, "Opedia", "FG3")
sql_combined_str = sql_dict["sql_tbl"] + SQL_index_dir["sql_index"]
DB.DB_modify(sql_dict["sql_tbl"], server)

# SQL.write_SQL_file(sql_combined_str, tbl, 'cruise')


df_import = data.mapTo180180(df_x)

cols= ['time','lat','lon']
col_order = cols+ [c for c in list(df_import) if c not in cols]
df_import = df_import[col_order]
df_import=data.sort_values(df_import, cols)

## Change back for import
qc_vars = [q for q in df_x.columns.tolist() if '_qc' in q]
for q in qc_vars:
      df_x[q] = df_x[q].replace('', np.nan)

## Check for commas before import
cols=[]
for a in list(df_import):
    if df_import[a].dtype=='O':
        if (df_import[a].str.contains(',') == True).any():
            cols.append(a)

## Have to split the table in two for initial ingest, then insert into table with column set
df_x1 = df_import.iloc[:,0:800]
df_x2 = df_import[list(df_import.columns[0:17]) + list(df_import.columns[800:])]

cdt = SQL.build_SQL_suggestion_df(df_x1)
sql_tbl = SQL.SQL_tbl_suggestion_formatter(cdt, tbl+'_1', server, 'Opedia', 'FG3')
DB.DB_modify(sql_tbl["sql_tbl"], server)
DB.toSQLbcp_wrapper(df_x1, tbl+'_1', server) 

cdt = SQL.build_SQL_suggestion_df(df_x2)
sql_tbl = SQL.SQL_tbl_suggestion_formatter(cdt, tbl+'_2', server, 'Opedia', 'FG3')
DB.DB_modify(sql_tbl["sql_tbl"], server)
DB.toSQLbcp_wrapper(df_x2, tbl+'_2', server) 


col_str = ', '.join(col_order)
non_meta_cols = ', '.join(col_order[17:])
query = f'''
INSERT INTO dbo.{tbl} (
      {col_str})

	  SELECT s1.[time]
      ,s1.[lat]
      ,s1.[lon]
      ,s1.[N_SAMPLES]
      ,s1.[N_STATIONS]
      ,s1.[cruise_id]
      ,s1.[station_id]
      ,s1.[station_type]
      ,s1.[Bot__Depth]
      ,s1.[Operator_s_Cruise_Name]
      ,s1.[Ship_Name]
      ,s1.[Period]
      ,s1.[Chief_Scientist]
      ,s1.[GEOTRACES_Scientist]
      ,s1.[Cruise_Aliases]
      ,s1.[Cruise_Information_Link]
      ,s1.[BODC_Cruise_Number]
    ,{non_meta_cols}
	   FROM dbo.{tbl+'_1'} s1
	  INNER JOIN dbo.{tbl+'_2'} s2
	  on s1.N_Samples = s2.N_Samples and s1.N_Stations = s2.N_Stations and s1.station_id = s2.station_id and s1.lat = s2.lat and s1.lon = s2.lon and s1.time = s2.time 
	  ORDER BY s1.time, s1.lat, s1.lon

'''

### Initial create table script missed these columns as nvarchar
alter_list = ["alter table tblGeotraces_Seawater_IDP2021v2 alter column Single_Cell_ID nvarchar(4) sparse", "alter table tblGeotraces_Seawater_IDP2021v2 alter column NCBI_Metagenome_BioSample_Accession nvarchar(12) sparse", "alter table tblGeotraces_Seawater_IDP2021v2 alter column NCBI_Single_Cell_Genome_BioProject_Accession nvarchar(22) sparse", "alter table tblGeotraces_Seawater_IDP2021v2 alter column NCBI_16S_18S_rRNA_gene_BioSample_Accession nvarchar(25) sparse", "alter table tblGeotraces_Seawater_IDP2021v2 alter column EMBL_EBI_Metagenome_MGNIFY_Analysis_Accession nvarchar(56) sparse", "alter table tblGeotraces_Seawater_IDP2021v2 alter column Bottle_Flag nvarchar(36) sparse", "alter table tblGeotraces_Seawater_IDP2021v2 alter column Cast_Identifier nvarchar(12) sparse", "alter table tblGeotraces_Seawater_IDP2021v2 alter column Sampling_Device nvarchar(7) sparse"]

for alt in alter_list:
    DB.DB_modify(alt, server)


DB.queryExecute(server, query)

DB.DB_modify(SQL_index_dir["sql_index"], server)
## Remove interim tables after insert query
qry = f"DROP TABLE {tbl+'_1'}"
DB.queryExecute(server,qry)
qry = f"DROP TABLE {tbl+'_2'}"
DB.queryExecute(server,qry)

#### Modify stats after metadata ingest to show min/max depth and not "surface only"
## Nulls in depth don't allow for naming convention of depth
## Reverted as this breaks download subsetting
# server = 'Mariana'
# qry = "SELECT  max(depth_sensor) mx_depth FROM [Opedia].[dbo].[tblGeotraces_Seawater_IDP2021v2]"
# df_depth = DB.dbRead(qry, server)


# pref = '{"depth":{"count":"","max":7300,"mean":"","min":0,"std":""},"time":'
# ds_id = cmn.getDatasetID_Tbl_Name(tbl, "Opedia",server)
# sts_df = DB.dbRead(f"select * from tblDataset_Stats where Dataset_ID ={ds_id}", server)
# sts = sts_df['JSON_stats'][0]
# # sts_new = sts.replace('{"time":',pref)
# sts_new = sts.replace(pref,'{"time":')
# sts_new[:100]

# stats.updateStatsTable(ds_id, sts_new, server)

