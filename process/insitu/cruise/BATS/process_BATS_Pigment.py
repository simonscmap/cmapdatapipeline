"""
Author: Diana Haring <dharing@uw.edu>
Date: 08-01-2021

Script to process raw BATS Pigment data
"""
import glob
import pandas as pd
import numpy as np
import shutil

import vault_structure as vs
import metadata

## Flag to export final processed data and script path to vault
export_data = False

tbl = 'tblBATS_Pigment'
base_folder = f"{vs.cruise}{tbl}/raw"
### Excel file in download_transfer copied from Google sheets BATS
### https://docs.google.com/spreadsheets/d/10FwNUZF_l8VJENiH2MUPEkmo7SWOjGYPGy_FSGBp6zs/edit#gid=1871267742
# shutil.copy(vs.download_transfer+"BATS_traj.xlsx",base_folder)

flist = glob.glob(base_folder+"/*txt")

df = pd.read_csv(flist[0], skiprows=50)
cols = df.columns.to_list()
cols = [x.replace('\t','').replace(' ','') for x in cols]

renamed = ['cruise_ID', 'date','decimal_year','time_hhmm','lat','lon','nisken_flag', 'depth','pigment1', 'pigment2', 'pigment3', 'pigment4', 'pigment5', 'pigment6', 'pigment7', 'pigment8', 'pigment9', 'pigment10', 'pigment11', 'pigment12', 'pigment13', 'pigment14', 'pigment15', 'pigment16', 'pigment17', 'pigment18', 'pigment19', 'pigment20', 'pigment21']
df = pd.read_csv(flist[0], sep='\t', skiprows=51, names=renamed)

### Nisken flag data not included in original import of BATS Bottle data
## (-3 = suspect, 1=unverified, 2= verified/acceptable)
# df['nisken_flag'].unique()
df['time_hhmm'] = df['time_hhmm'].astype(float).astype(int).astype(str).str.zfill(4)
df = df.replace(-999, np.nan)
df['lon'] = df['lon'] * -1.0 # converts degrees west to -180,180.
df['cruise_ID'] = df['cruise_ID'].astype(str)


df['date'] = df['date'].astype(str)
### No times as -999, no need for coerce
df['time'] = pd.to_datetime(df['date'].astype(str) + ':' + df['time_hhmm'].astype(str), format = '%Y%m%d:%H%M')
#### Formatting for full validator ingestion instead of SQL update
df['time'] = df.time.dt.strftime('%Y-%m-%dT%H:%M:%S')
### Assign UNOLS name based on cruise_id
bats_lookup = pd.read_excel(f"{base_folder}/BATS_traj.xlsx")
bats_lookup['Nickname'] = bats_lookup['Nickname'].astype(str)
bats_dict = dict(zip(bats_lookup.Nickname, bats_lookup.Name))
df['UNOLS'] = df['cruise_ID'].str[:5].map(bats_dict)
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

reorder = ['time', 'lat', 'lon', 'depth', 'pigment1', 'pigment2', 'pigment3', 'pigment4', 'pigment5', 'pigment6', 'pigment7', 'pigment8', 'pigment9', 'pigment10', 'pigment11', 'pigment12', 'pigment13', 'pigment14', 'pigment15', 'pigment16', 'pigment17', 'pigment18', 'pigment19', 'pigment20', 'pigment21', 'cruise_ID', 'nisken_flag','UNOLS']

df =df[reorder]
df = df.sort_values(['time', 'lat', 'lon', 'depth'], ascending=[True] * 4)

if export_data:
    df.to_excel(base_folder+f'/{tbl}_data.xlsx', index=False)
    metadata.export_script_to_vault(tbl,'cruise','process/insitu/cruise/BATS/process_BATS_Pigment.py','process.txt')


