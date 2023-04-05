from re import sub
import sys
import os

import pandas as pd
import numpy as np
import xarray as xr
import glob
from tqdm import tqdm


sys.path.append("cmapdata/ingest")
import vault_structure as vs
import credentials as cr
import DB 
import data_checks as dc


tbl = 'tblWind_NRT_hourly'
base_folder = f'{vs.satellite}{tbl}/raw/'
base_folder = f'/data/vault/observation/remote/satellite/{tbl}/raw/'

meta_folder = f'{vs.satellite}{tbl}/metadata/'
nrt_folder = f'{vs.satellite}{tbl}/nrt/'


flist_all = np.sort(glob.glob(os.path.join(base_folder, f'*.nc')))


# flist = [i for i in flist_all if date_import[0] in i or date_import[1] in i]
# flist = [i for i in flist_all if date_import[0] in i]

len(flist_all)
flist_all[18023]
flist_all[0]

# ## Create SQL table syntax
# for c in df.columns:
#     if c == 'lat' or c == 'lon':
#         print('['+ c + '] [float] NOT NULL, ' )
#     elif c == 'time':
#         print('['+ c + '] [datetime] NOT NULL, ' )
#     elif df[c].dtype == 'object':
#         print('['+ c + '] [nvarchar](' + str(df[c].str.len().max()) + ') NULL, ' )
#     else:
#         print('['+ c + '] [float] NULL, ' )



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

df_varnames.to_csv('Hourly_wind_Vars.csv', index=False)


d = {'filename':[],'null_check':[],'unique_check':[]}
df_error=pd.DataFrame(data=d)

day_list = range(10, 13, 1) #through 2000


yr=1993
fil=flist[0]

len(flist_all)
yr = sub_list[0]
## 433/744, 58% of f'_PT1H_20210{str(yr)}'
sub_list = range(10,13, 1) 
hour_list = ['00','06','12','18']

flist=[]
for fil in tqdm(flist_all):
    for hr in hour_list:
        if f'{str(hr)}_R' in fil:
            flist.append(fil)
len(flist)

mnts = ['08','09']
flist_m =[]
for fil in flist:
    for mn in mnts:
        if f'PT1H_2022{mn}' in fil:
            flist_m.append(fil)   
len(flist_m)             

## stopped at /data/CMAP Data Submission Dropbox/Simons CMAP/vault/observation/remote/satellite/tblWind_NRT_hourly/raw/cmems_obs-wind_glo_phy_nrt_l4_0.125deg_PT1H_2021102706_R20211027T00_06.nc
## 1933/3016
flist = flist[1933:]

for fil in tqdm(flist_m):
    ## Equal dataframes with and without Mask and scale 
    x = xr.open_dataset(fil, mask_and_scale=True )
    df = x.to_dataframe().reset_index()
    x.close()
    df['hour'] = df['time'].dt.hour
    df['time'] = df['time'].dt.date
    cols = df.columns.tolist()
    df = df[['time', 'lat', 'lon','hour'] + cols[3:30]]
    df = df.sort_values(["time", "lat","lon","hour"], ascending = (True, True,True,True))
    # 40203 before changing data type from float to int with nulls. 39836 after
    df['number_of_observations'] = df['number_of_observations'].astype('Int64')
    df['number_of_observations_divcurl'] = df['number_of_observations_divcurl'].astype('Int64')
    df = dc.add_day_week_month_year_clim(df)
    DB.toSQLbcp_wrapper(df, tbl, "Rossby") 
    print(fil)

## Export all as parquet
for fil in tqdm(flist_all):
    x = xr.open_dataset(fil, mask_and_scale=True )
    df = x.to_dataframe().reset_index()
    x.close()
    df['hour'] = df['time'].dt.hour
    df['time'] = df['time'].dt.date
    cols = df.columns.tolist()
    df = df[['time', 'lat', 'lon','hour'] + cols[3:30]]
    df = df.sort_values(["time", "lat","lon","hour"], ascending = (True, True,True,True))
    # 40203 before changing data type from float to int with nulls. 39836 after
    df['number_of_observations'] = df['number_of_observations'].astype('Int64')
    df['number_of_observations_divcurl'] = df['number_of_observations_divcurl'].astype('Int64')
    df = dc.add_day_week_month_year_clim(df)
    file_name = fil.rsplit('/',1)[1].rsplit('.',1)[0]
    df.to_parquet(nrt_folder+file_name+'.parquet')



include = "[wind_curl]"
indices = [

f"""CREATE NONCLUSTERED INDEX [IX_{tbl}_dayofyear_lat_lon] ON [dbo].[{tbl}]
(
	[dayofyear] ASC,
	[lat] ASC,
	[lon] ASC
)
INCLUDE({include}) 
WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) 
ON [FG5]"""
,

f"""CREATE NONCLUSTERED INDEX [IX_{tbl}_week_lat_lon] ON [dbo].[{tbl}]
(
	[week] ASC,
	[lat] ASC,
	[lon] ASC
)
INCLUDE({include}) 
WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) 
ON [FG4]"""
,

f"""CREATE NONCLUSTERED INDEX [IX_{tbl}_month_lat_lon] ON [dbo].[{tbl}]
(
	[month] ASC,
	[lat] ASC,
	[lon] ASC
)
INCLUDE({include}) 
WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) 
ON [FG3]""",

f"""CREATE NONCLUSTERED INDEX [IX_{tbl}_year_lat_lon] ON [dbo].[{tbl}]
(
	[year] ASC,
	[lat] ASC,
	[lon] ASC
)
INCLUDE({include}) 
WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) 
ON [FG2]"""
]

DB.queryExecute('Rossby', indices[0])

for i in tqdm(indices):
    DB.queryExecute('Rossby', i)