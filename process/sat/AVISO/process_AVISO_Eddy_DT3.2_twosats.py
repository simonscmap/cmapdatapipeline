import sys
import os

import pandas as pd
import numpy as np
import xarray as xr
import glob
from tqdm import tqdm

# os.chdir('/home/exx/Documents/CMAP/cmapdata')
sys.path.append("ingest")
from ingest import vault_structure as vs
from ingest import SQL
from ingest import common as cm
from ingest import data_checks as dc
from ingest import DB
from ingest import metadata

tbl = 'tblMesoscale_Eddy_twosats'

vs.leafStruc(vs.satellite+tbl+'_short')
vs.leafStruc(vs.satellite+tbl+'_long')
vs.leafStruc(vs.satellite+tbl+'_untracked')

base_folder = f'{vs.satellite}{tbl}/raw/'

flist_all = np.sort(glob.glob(os.path.join(base_folder, f'*.nc')))



d1 = {'var_name':[], 'std_name':[], 'long_name':[], 'dtype':[], 'units':[], 'comment':[], 'flag_val':[], 'flag_def':[]}
df_varnames = pd.DataFrame(data=d1)

for varname, da in x.data_vars.items():
    print(varname)
    print(da.attrs)
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

df_varnames.to_excel(vs.download_transfer +'AVISO_Eddy32_allsats_Vars.xlsx', index=False)
df_varnames.to_csv('AVISO_Eddy32_allsats_Vars.csv', index=False)

core_cols = ["time",
    "lat",
    "lon",
    "year",
    "month",
    "week",
    "dayofyear"]

fil = flist_all[3]

x = xr.open_dataset(fil)

SQL.full_SQL_suggestion_build(df, 'tblMesoscale_Eddy_twosats_long', 'satellite', 'Rainier', 'Opedia')
SQL.full_SQL_suggestion_build(df, 'tblMesoscale_Eddy_twosats_short', 'satellite', 'Rainier', 'Opedia')
SQL.full_SQL_suggestion_build(df, 'tblMesoscale_Eddy_twosats_untracked', 'satellite', 'Rainier', 'Opedia')

for fil in tqdm(flist_all):
    x = xr.open_dataset(fil).sel(NbSample=0)
    df = x.to_dataframe().reset_index()
    df = df.drop(["obs"], axis=1)
    df =df.rename(
        columns={
            "latitude": "lat",
            "longitude": "lon"
        })
    if 'Anticyclonic' in fil:
        df['eddy_polarity'] = 1
    else:
        df['eddy_polarity'] = -1
    df_cols = df.columns.tolist()
    data_cols = [e for e in df_cols if e not in core_cols]
    df = dc.add_day_week_month_year_clim(df)
    df = df[core_cols + data_cols]
    df = dc.mapTo180180(df)
    df = dc.sort_values(df, ['time','lat','lon'])
    if '_long' in fil:
        DB.toSQLbcp_wrapper(df, 'tblMesoscale_Eddy_twosats_long', 'Rossby')
    elif '_short' in fil: 
        DB.toSQLbcp_wrapper(df, 'tblMesoscale_Eddy_twosats_short', 'Rossby')
    elif '_untracked' in fil:
        DB.toSQLbcp_wrapper(df, 'tblMesoscale_Eddy_twosats_untracked', 'Rossby')


tbl_list = ['tblMesoscale_Eddy_twosats_long', 'tblMesoscale_Eddy_twosats_short', 'tblMesoscale_Eddy_twosats_untracked']
for tbl in tbl_list:    
    qry = f"select distinct year from dbo.{tbl} order by year"
    df_yr = DB.dbRead(qry, 'Rossby')

    yr_list = df_yr['year'].to_list()
    for yr in tqdm(yr_list):
        qry = f"select * from dbo.{tbl} where year = {yr}"
        df_ing = DB.dbRead(qry, 'Rossby')
        DB.toSQLbcp_wrapper(df_ing, tbl, "Mariana")
        del df_ing

s_list = ['rainier','rossby','mariana']
for server in s_list:
    for tbl in tbl_list:
        metadata.tblDataset_Server_Insert(tbl, 'Opedia', server)