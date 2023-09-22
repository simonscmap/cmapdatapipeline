"""
Author: Diana Haring <dharing@uw.edu>
Date: 08-01-2021

Script to process raw BATS Zooplankton data
"""
import glob
import pandas as pd
import numpy as np
import shutil
import vault_structure as vs
import metadata

## Flag to export final processed data and script path to vault
export_data = False

tbl = 'tblBATS_Zooplankton_Biomass'
base_folder = f"{vs.cruise}{tbl}/raw"
### Excel file in download_transfer copied from Google sheets BATS
### https://docs.google.com/spreadsheets/d/10FwNUZF_l8VJENiH2MUPEkmo7SWOjGYPGy_FSGBp6zs/edit#gid=1871267742
# shutil.copy(vs.download_transfer+"BATS_traj.xlsx",base_folder)
flist = glob.glob(base_folder+"/*.txt")


def dms2dd(degree_col, min_col, sec_col):
    """Converts degrees minutes seconds to decimal

    Args:
        degree_col {str}: Pandas column of lat or lon degrees
        min_col {str}: Pandas column of lat or lon minutes
        sec_col {str}: Pandas column of lat or lon seconds

    Returns:
        dd_col (Pandas column): Pandas column of lat or lon decimal
    """
    dd_col = degree_col.astype(float) + (min_col.astype(float))/60.0 + (sec_col.astype(float))/(60.0*60.0)
    return dd_col

renamed = ['cruise_ID', 'date','tow_number','lat_degrees','lat_minutes','lon_degrees','lon_minutes','time_in','time_out','duration_minutes','depth','volume_water','sieve_size','wet_weight','dry_weight','wet_weight_vol_water_ratio','dry_weight_vol_water_ratio','total_wet_weight_volume_all_size_fractions_ratio',
    'total_dry_weight_volume_all_size_fractions_ratio','wet_weight_vol_water_ratio_200m_depth','dry_weight_vol_water_ratio_200m_depth','total_wet_weight_volume_all_size_fractions_ratio_200m_depth','total_dry_weight_volume_all_size_fractions_ratio_200m_depth']
df = pd.read_csv(flist[0], sep='\t', skiprows=36, names=renamed)

df['lat_minutes'], df['lat_seconds'] = df['lat_minutes'].astype(str).str.split('.').str
df['lon_minutes'], df['lon_seconds'] = df['lon_minutes'].astype(str).str.split('.').str
df['lat'] = dms2dd(df['lat_degrees'],df['lat_minutes'],df['lat_seconds'])
df['lon'] = dms2dd(df['lon_degrees'],df['lon_minutes'],df['lon_seconds']) * -1.0

df['cruise_ID'] = df['cruise_ID'].astype(str)
df['time_in'] = df['time_in'].astype(str).str.zfill(4)
df['time_out'] = df['time_out'].astype(str).str.zfill(4)
df['time'] = pd.to_datetime(df['date'].astype(str), format = '%Y%m%d')
df = df.replace(-999, np.nan)

#### Formatting for full validator ingestion instead of SQL update
df['time'] = df.time.dt.strftime('%Y-%m-%d')
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

reorder = ['time','lat','lon','depth','time_in','time_out','duration_minutes','volume_water','sieve_size','wet_weight','dry_weight','wet_weight_vol_water_ratio','dry_weight_vol_water_ratio','total_wet_weight_volume_all_size_fractions_ratio',
      'total_dry_weight_volume_all_size_fractions_ratio','wet_weight_vol_water_ratio_200m_depth','dry_weight_vol_water_ratio_200m_depth','total_wet_weight_volume_all_size_fractions_ratio_200m_depth','total_dry_weight_volume_all_size_fractions_ratio_200m_depth','cruise_ID','UNOLS']

df =df[reorder]

## 571 rows had -999 for depth (~8% of total rows)
df = df.loc[~df['depth'].isna()]
df = df.sort_values(['time', 'lat', 'lon', 'depth'], ascending=[True] * 4)

### rename to get through validator total_wet_weight_volume_all_size_fractions_ratio_200m_depth
df.rename(columns={"total_wet_weight_volume_all_size_fractions_ratio_200m_depth":"total_wet_weight_volume_all_size_fractions_200m","total_dry_weight_volume_all_size_fractions_ratio_200m_depth":"total_dry_weight_volume_all_size_fractions_200m"}, inplace=True)


if export_data:
    df.to_excel(base_folder+f'/{tbl}_data.xlsx', index=False)
    metadata.export_script_to_vault(tbl,'cruise','process/insitu/cruise/BATS/process_BATS_Zooplankton.py','process.txt')


