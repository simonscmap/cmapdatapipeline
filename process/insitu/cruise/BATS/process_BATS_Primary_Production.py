"""
Author: Diana Haring <dharing@uw.edu>
Date: 08-01-2021

Script to process raw BATS Primary Production data
"""
import glob
import pandas as pd
import numpy as np
import shutil

import vault_structure as vs
import metadata

## Flag to export final processed data and script path to vault
export_data = False

tbl = 'tblBATS_Primary_Production'
base_folder = f"{vs.cruise}{tbl}/raw"
### Excel file in download_transfer copied from Google sheets BATS
### https://docs.google.com/spreadsheets/d/10FwNUZF_l8VJENiH2MUPEkmo7SWOjGYPGy_FSGBp6zs/edit#gid=1871267742
# shutil.copy(vs.download_transfer+"BATS_traj.xlsx",base_folder)
flist = glob.glob(base_folder+"/*txt")

df = pd.read_csv(flist[0], skiprows=38)
cols = df.columns.to_list()
cols = [x.replace('\t','').replace(' ','') for x in cols]
## drop last empty col
cols = cols[:-1]

df = pd.read_csv(flist[0], sep='\t', skiprows=39, names=cols)
df = df.replace(-999, '')

### Added errors = 'coerce' to force times that were -999 into dates
df['hhmm_in'] = df['hhmm_in'].astype(str).str.zfill(4)
df['time'] = pd.to_datetime(df['yymmdd_in'].astype(str) + ':' + df['hhmm_in'].astype(str), format = '%Y%m%d:%H%M')

df.rename(columns={'Id':'Cruise_ID'},inplace=True)
df['Cruise_ID'] = df['Cruise_ID'].astype(str)
df['hhmm_out'] = df['hhmm_out'].astype(str).str.zfill(4)
df['time_out'] = pd.to_datetime(df['yymmdd_out'].astype(str) + ':' + df['hhmm_out'].astype(str), format = '%Y%m%d:%H%M')

df['lat'] = df['Lat_in']
df['lon'] = df['Long_in']
## Missing lat lon in 16 rows
df = df.loc[df['lon']!='']
df['lon'] = df['lon'].astype(float) * -1.0 # converts degrees west to -180,180.
df['Long_out'] = pd.to_numeric(df['Long_out'], errors='coerce').astype(float)
df['Long_out'] = df['Long_out'] * -1.0 # converts degrees west to -180,180.

#### Formatting for full validator ingestion instead of SQL update
df['time'] = df.time.dt.strftime('%Y-%m-%dT%H:%M')
df['time_out'] = df.time_out.dt.strftime('%Y-%m-%dT%H:%M')

df.rename(columns={'dep1':'depth','QF':'nisken_flag'},inplace=True)
### Assign UNOLS name based on cruise_id
bats_lookup = pd.read_excel(f"{base_folder}/BATS_traj.xlsx")
bats_lookup['Nickname'] = bats_lookup['Nickname'].astype(str)
bats_dict = dict(zip(bats_lookup.Nickname, bats_lookup.Name))
df['UNOLS'] = df['Cruise_ID'].str[:5].map(bats_dict)
### Not all BATS cruises have UNOLS names assigned
# df.loc[df['UNOLS'].isna()]
### Fix assignments for duplicate bats_lookup Nicknames
df.loc[(df['UNOLS']== 'AE2021') & (df['time']> '2020-12-16'),'UNOLS']='AE2022'
df.loc[(df['UNOLS']== 'AE2022') & (df['time']<= '2020-12-16'),'UNOLS']='AE2021'

df.loc[(df['UNOLS']== 'AE2121') & (df['time']< '2021-10-11'),'UNOLS']='AE2120'
df.loc[(df['UNOLS']== 'AE2120') & (df['time']>= '2021-10-11'),'UNOLS']='AE2121'

df.loc[(df['UNOLS']== 'AE1108') & (df['time']> '2011-04-28'),'UNOLS']='AE1109'
df.loc[(df['UNOLS']== 'AE1109') & (df['time']< '2011-04-29'),'UNOLS']='AE1108'

df.loc[(df['UNOLS']== 'AE1301') & (df['time']> '2013-01-24'),'UNOLS']='AE1302'
df.loc[(df['UNOLS']== 'AE1302') & (df['time'] < '2013-01-25'),'UNOLS']='AE1301'

reorder =['time',  'lat', 'lon',  'depth', 'time_out', 'Lat_out', 'Long_out','pres', 'temp', 'salt', 'lt1', 'lt2', 'lt3', 'dark', 't0', 'pp','Cruise_ID', 'nisken_flag','UNOLS']

df =df[reorder]
df = df.sort_values(['time', 'lat', 'lon', 'depth'], ascending=[True] * 4)

if export_data:
    df.to_excel(base_folder+f'/{tbl}_data.xlsx', index=False)
    metadata.export_script_to_vault(tbl,'cruise','process/insitu/cruise/BATS/process_BATS_Primary_Production.py','process.txt')


