import os
import sys
import pandas as pd
import re

sys.path.append("ingest")

import vault_structure as vs
import data
import SQL
import DB

tbl = 'tblSOCATv2022'

data_file_path = f'{vs.mixed}{tbl}/raw/SOCATv2022.tsv'
#head -n 7000 SOCATv2022.tsv >> header.tsv



## 33705437 rows long
with open(data_file_path, encoding='utf-8') as readfile:
    ls_readfile = readfile.readlines()
    ##Find all occurances of Expocode in header metadata
    skip_list = list(filter(lambda x: x[1].startswith('Expocode'), enumerate(ls_readfile)))
    skip_entry = skip_list[len(skip_list)-1]
    skip = skip_entry[0] #6978


df = pd.read_csv(data_file_path, skiprows=skip, sep='\t',engine='python')
rename_list = {'Expocode':'Expocode',
'version':'version',
'Source_DOI':'Source_DOI',
'QC_Flag':'QC_Flag',
'sal':'sal',
'SST [deg.C]':'SST',
'Tequ [deg.C]':'Tequ',
'PPPP [hPa]':'PPPP',
'Pequ [hPa]':'Pequ',
'WOA_SSS':'WOA_SSS',
'NCEP_SLP [hPa]':'NCEP_SLP',
'ETOPO2_depth [m]':'ETOPO2_depth',
'dist_to_land [km]':'dist_to_land',
'GVCO2 [umol/mol]':'GVCO2',
'xCO2water_equ_dry [umol/mol]':'xCO2water_equ_dry',
'xCO2water_SST_dry [umol/mol]':'xCO2water_SST_dry',
'pCO2water_equ_wet [uatm]':'pCO2water_equ_wet',
'pCO2water_SST_wet [uatm]':'pCO2water_SST_wet',
'fCO2water_equ_wet [uatm]':'fCO2water_equ_wet',
'fCO2water_SST_wet [uatm]':'fCO2water_SST_wet',
'fCO2rec [uatm]':'fCO2rec',
'fCO2rec_src':'fCO2rec_src',
'fCO2rec_flag':'fCO2rec_flag',
'sample_depth [m]':'sample_depth',
'longitude [dec.deg.E]':'lon',
'latitude [dec.deg.N]':'lat'
}
df.rename(columns=rename_list, inplace = True)
## Seconds are float but all are X.0
## Convert to int for time conversion
df['ss']=df['ss'].astype(int)
df['ss_str'] = df['ss'].astype(str).str.zfill(2)
df['mm_str'] = df['mm'].astype(str).str.zfill(2)
df['hh_str'] = df['hh'].astype(str).str.zfill(2)
df['day_str'] = df['day'].astype(str).str.zfill(2)
df['mon_str'] = df['mon'].astype(str).str.zfill(2)

## Testing on coerce to ensure no times are dropped
# df['time_coerce']=pd.to_datetime(df['yr'].astype(str)+df['mon_str']+df['day_str']+df['hh_str']+df['mm_str']+df['ss_str'], format="%Y%m%d%H%M%S", errors='coerce')
# df_error = df.loc[df['time_coerce'].isna()] ## 264069

df['time']=pd.to_datetime(df['yr'].astype(str)+df['mon_str']+df['day_str']+df['hh_str']+df['mm_str']+df['ss_str'], format="%Y%m%d%H%M%S")
df.columns.to_list()

df = df[['time','lat', 'lon', 'sample_depth','Expocode', 'version', 'Source_DOI', 'QC_Flag', 'sal', 'SST', 'Tequ', 'PPPP', 'Pequ', 'WOA_SSS', 'NCEP_SLP', 'ETOPO2_depth', 'dist_to_land', 'GVCO2', 'xCO2water_equ_dry', 'xCO2water_SST_dry', 'pCO2water_equ_wet', 'pCO2water_SST_wet', 'fCO2water_equ_wet', 'fCO2water_SST_wet', 'fCO2rec', 'fCO2rec_src', 'fCO2rec_flag' ]]

df = data.mapTo180180(df)
df = data.sort_values(df, ['time','lat', 'lon','Expocode'])

SQL.full_SQL_suggestion_build(df, tbl, 'mixed', 'Rainier', 'Opedia')

DB.toSQLbcp_wrapper(df, tbl, 'Rainier')

df.to_parquet(f'{vs.mixed}{tbl}/rep/{tbl}_data.parquet')

## Post ingest tests
df.shape

df.loc[df['sample_depth']<0]
df.loc[df['lat'].isna()]
df.loc[df['lon'].isna()]
df.loc[df['time'].isna()]
df.loc[df['sample_depth'].isna()] #27273804




