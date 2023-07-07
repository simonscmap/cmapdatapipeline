import os
import pandas as pd
import glob

import vault_structure as vs
import SQL
import DB
import metadata

tbl = 'tblTara_prokaryote_otu'
base_folder = os.path.join(vs.cruise,tbl,'raw')

## Cleaned up columns in CMAP tab
## Same data as sheet name Table W1 in OM.CompanionTables.xlsx 
xl_st = os.path.join(vs.cruise,tbl,'raw','sunagawa_tables1.xlsx')

flist = glob.glob(base_folder+'/*.gz')
df = pd.read_csv(flist[0], sep = '\t')

cols = df.columns.to_list()

####### Reshape 
df_long = pd.melt(df, id_vars='OTU.rep', value_vars=cols[7:])
df_long.rename(columns={'value':'otu_count','variable':'sample_label'}, inplace=True)
df_join = pd.merge(df_long, df[['Domain', 'Phylum', 'Class', 'Order', 'Family', 'Genus', 'OTU.rep']], how='left', on='OTU.rep')
df_cast = pd.read_excel(xl_st, sheet_name='CMAP')
df_join_cast = pd.merge(df_join, df_cast[['time','lat','lon','station','depth','sample_label']], how='left', on='sample_label')
df_join_cast['time'] =pd.to_datetime(df_join_cast['time'],format="%Y-%m-%dT%H:%M:%S")
df_join_cast = df_join_cast[['time', 'lat', 'lon', 'depth', 'station', 'sample_label', 'Domain', 'Phylum', 'Class', 'Order', 'Family', 'Genus', 'OTU.rep', 'otu_count']]
df_join_cast.rename(columns={'OTU.rep':'otu_rep'}, inplace=True)
df_join_cast = df_join_cast.sort_values(['time', 'lat', 'lon', 'depth'], ascending=[True] * 4)
df_join_cast.columns.to_list()
## Subset 4,955,489 rows for SQL table build script to run on 
df_sql_build = df_join_cast.head(10000)

SQL.full_SQL_suggestion_build(df_sql_build, tbl, 'cruise', 'Rainier', 'Opedia')
DB.toSQLbcp_wrapper(df_join_cast, tbl, 'Rainier')

metadata.export_script_to_vault(tbl,'cruise','process/insitu/cruise/TARA/process_Tara_prokaryote.py','process.txt')