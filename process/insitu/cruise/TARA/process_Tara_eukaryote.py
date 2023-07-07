import os
import pandas as pd
import glob

import vault_structure as vs
import SQL
import DB
import metadata

tbl = 'tblTara_eukaryote_otu'
base_folder = os.path.join(vs.cruise,tbl,'raw')

## Cleaned up columns in CMAP tab
## Same data as sheet name Table W1 in OM.CompanionTables.xlsx 
xl_st = os.path.join(vs.cruise,tbl,'raw','sunagawa_tables1.xlsx')

xl_w1 = os.path.join(vs.cruise,tbl,'raw','Database_W1_Sample_parameters.xls')

flist = glob.glob(base_folder+'/*.tsv')
df = pd.read_csv(flist[0], sep = '\t')
## Drop total row
df = df.loc[df['md5sum']!='All']

cols = df.columns.to_list()
st_cols = [c for c in cols if 'TARA' in c]
join_cols = ['md5sum', 'cid','OTU_purity', 'pid', 'taxogroup', 'lineage']

####### Reshape 
df_long = pd.melt(df, id_vars='md5sum', value_vars=st_cols)
df_long.rename(columns={'value':'otu_count','variable':'station'}, inplace=True)
df_join = pd.merge(df_long, df[join_cols], how='left', on='md5sum')

df_cast = pd.read_excel(xl_w1)
df_cast.columns.to_list()
df_cast.rename(columns={'TARA STATION IDENTIFIER':'station','LATITUDE (Decimal Degrees)':'lat','LONGITUDE (Decimal Degrees)':'lon','TIME AND DATE':'time'},inplace=True)



# df_cast = pd.read_excel(xl_st, sheet_name='CMAP')
df_cast['time'] =pd.to_datetime(df_cast['time'],format="%Y-%m-%dT%H:%M:%S")
df_cast_min = df_cast[df_cast.groupby('station').time.transform('min') == df_cast['time']]
## cast data is by sample with duplicates in space/time
df_cast_min = df_cast_min.drop_duplicates(subset=['station'],keep='first')
df_cast_max = df_cast[df_cast.groupby('station').time.transform('max') == df_cast['time']]
df_cast_max = df_cast_max.drop_duplicates(subset=['station'],keep='first')
df_station_cast = pd.merge(df_join, df_cast_min[['time','lat','lon','station']], how='left', on = 'station')
len(df_join) == len(df_station_cast)

## Set date from min time as time column
df_station_cast['start_time'] = df_station_cast['time']
df_station_cast['time'] = df_station_cast['time'].dt.date
df_station_cast = pd.merge(df_station_cast, df_cast_max[['time','station']], how='left', on = 'station')
len(df_join) == len(df_station_cast)
df_station_cast.rename(columns={'time_x':'time','time_y':'end_time'},inplace=True)

df_station_cast = df_station_cast.sort_values(['time', 'lat', 'lon' ], ascending=[True] * 3)
df_station_cast.loc[df_station_cast['time'].isna()]
df_station_cast.columns.to_list()
df_station_cast = df_station_cast[['time', 'lat', 'lon', 'start_time', 'end_time','station', 'taxogroup', 'lineage',  'otu_count', 'OTU_purity', 'pid', 'cid', 'md5sum']]
df_station_cast.dtypes
df_station_cast.to_excel(base_folder+"/tblTara_eukaryote_otu_data.xlsx",index=False)

# SQL.full_SQL_suggestion_build(df_station_cast, tbl, 'cruise', 'Rainier', 'Opedia')
# DB.toSQLbcp_wrapper(df_join_cast, tbl, 'Rainier')

metadata.export_script_to_vault(tbl,'cruise','process/insitu/cruise/TARA/process_Tara_eukaryote.py','process.txt')