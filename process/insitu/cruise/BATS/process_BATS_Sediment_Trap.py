"""
Author: Diana Haring <dharing@uw.edu>
Date: 08-01-2021

Script to process raw BATS Sediment Trap data
"""
import glob
import pandas as pd
import numpy as np
import shutil

import vault_structure as vs
import metadata

## Flag to export final processed data and script path to vault
export_data = False

tbl = 'tblBATS_Sediment_Trap'
base_folder = f"{vs.cruise}{tbl}/raw"
### Excel file in download_transfer copied from Google sheets BATS
### https://docs.google.com/spreadsheets/d/10FwNUZF_l8VJENiH2MUPEkmo7SWOjGYPGy_FSGBp6zs/edit#gid=1871267742
# shutil.copy(vs.download_transfer+"BATS_traj.xlsx",base_folder)
flist = glob.glob(base_folder+"/*.txt")

df = pd.read_csv(flist[0], skiprows=50)
cols = df.columns.to_list()
cols = [x.replace('\t','').replace(' ','') for x in cols]
## drop last empty col
cols = cols[:-1]

df = pd.read_csv(flist[0], sep='\t', skiprows=51, names=cols)
df = df.replace(-999.00, np.nan)
### "Inf" text in some C3 and C_avg values
df = df.replace('    Inf', np.nan)
df['C3'] = df['C3'].astype(float)
df['Cavg'] = df['Cavg'].astype(float)
df['N3'] = df['N3'].astype(float)
df['Navg'] = df['Navg'].astype(float)

df['time'] = pd.to_datetime(df['yymmdd1'].astype(str) , format = '%Y%m%d')
df['time_recovery'] = pd.to_datetime(df['yymmdd2'].astype(str), format = '%Y%m%d')

df['Cruise_ID'] = df['cr'].astype(str)

df['lat'] = df['Lat1']
df['lon'] = df['long1']
df['lon'] = df['lon'].astype(float) * -1.0 # converts degrees west to -180,180.
df['long2'] = df['long2'].astype(float) * -1.0 # converts degrees west to -180,180.
#### Formatting for full validator ingestion instead of SQL update
df['time'] = df.time.dt.strftime('%Y-%m-%d')
df['time_recovery'] = df.time_recovery.dt.strftime('%Y-%m-%d')

df.rename(columns={'dep':'depth','long2':'lon_recovery','Lat2':'lat_recovery','Cavg':'C_avg','Navg':'N_avg','Pavg':'P_avg','FBCavg':'FBC_avg','FBNavg':'FBN_avg'},inplace=True)
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

reorder =['time',  'lat', 'lon',  'depth', 'time_recovery', 'lat_recovery', 'lon_recovery','M1', 'M2', 'M3', 'M_avg', 'C1', 'C2', 'C3', 'C_avg', 'N1', 'N2', 'N3', 'N_avg', 'P1', 'P2', 'P3', 'P_avg', 'FBC1', 'FBC2', 'FBC3', 'FBC_avg', 'FBN1', 'FBN2', 'FBN3', 'FBN_avg','Cruise_ID','UNOLS']

df =df[reorder]
df = df.replace('-999.00', '')
df = df.replace(-999, '')
df = df.sort_values(['time', 'lat', 'lon', 'depth'], ascending=[True] * 4)

if export_data:
    df.to_excel(base_folder+f'/{tbl}_data.xlsx', index=False)
    metadata.export_script_to_vault(tbl,'cruise','process/insitu/cruise/BATS/process_BATS_Sediment_Trap.py','process.txt')


