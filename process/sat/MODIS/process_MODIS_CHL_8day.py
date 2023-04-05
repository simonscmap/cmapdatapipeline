import sys
import os

import pandas as pd
import numpy as np
import xarray as xr
import glob
from tqdm import tqdm
import shutil


sys.path.append("ingest")
import vault_structure as vs
import credentials as cr
import DB 
import data_checks as dc
import stats


tbl = 'tblModis_CHL'
base_folder = f'{vs.satellite}{tbl}/raw/'
rep_folder = f'{vs.satellite}{tbl}/rep/'
code_folder= f'{vs.satellite}{tbl}/code/'

server = 'Rossby'

qry = "Select distinct time from dbo.tblModis_CHL"
import_dates = DB.dbRead(qry,server)
import_dates['time'] =import_dates['time'].astype(str)

date_import = ['AQUA_MODIS.2022']
date_import = ['A2022']

flist_all = np.sort(glob.glob(os.path.join(base_folder, f'*.nc')))
flist = [i for i in flist_all if date_import[0] in i or date_import[1] in i or date_import[2] in i or date_import[3] in i or date_import[4] in i ]
flist = [i for i in flist_all if date_import[0] in i]
len(flist)

fil = base_folder+'A20221612022168.L3m_8D_CHL_chlor_a_9km.nc'
fil = base_folder+'AQUA_MODIS.20220610_20220617.L3m.8D.CHL.chlor_a.9km.NRT.nc'
for fil in tqdm(flist):

    ## Old format
    # fil_date = pd.to_datetime(
    #         fil.rsplit("/",1)[1].split(".",1)[0][1:8], format="%Y%j"
    #     ).strftime("%Y-%m-%d")
    ## New format
    fil_date = pd.to_datetime(
            fil.rsplit("/",1)[1].split(".",2)[1][0:8], format="%Y%m%d"
        ).strftime("%Y-%m-%d")    

    if import_dates['time'].str.contains(str(fil_date)).any():
        print("already imported: " + fil_date)
    else:
        x = xr.open_dataset(fil)
        chl = x.drop_dims(['rgb','eightbitcolor'])
        df = chl.to_dataframe().reset_index()
        df.insert(0,"time", pd.to_datetime(
                fil.rsplit("/",1)[1].split(".",1)[0][1:8], format="%Y%j"
            ).strftime("%Y-%m-%d")
        )
        # dc.check_df_nulls(df, tbl, "Beast")
        df_clean = dc.clean_data_df(df)
        # pq_file = rep_folder+os.path.basename(fil).strip(".nc").replace(".","_")+".parquet"
        # df_clean.to_parquet(pq_file, engine = 'auto', compression = None, index=False)
        DB.toSQLbcp_wrapper(df_clean, tbl, server) 

for fil in tqdm(flist):
    fil_date = pd.to_datetime(
            fil.rsplit("/",1)[1].split(".",1)[0][1:8], format="%Y%j"
        ).strftime("%Y-%m-%d")
    x = xr.open_dataset(fil)
    chl = x.drop_dims(['rgb','eightbitcolor'])
    df = chl.to_dataframe().reset_index()
    df.insert(0,"time", pd.to_datetime(
            fil.rsplit("/",1)[1].split(".",1)[0][1:8], format="%Y%j"
        ).strftime("%Y-%m-%d")
    )
    # dc.check_df_nulls(df, tbl, "Beast")
    df_clean = dc.clean_data_df(df)
    pq_file = rep_folder+os.path.basename(fil).strip(".nc").replace(".","_")+".parquet"
    df_clean.to_parquet(pq_file, engine = 'auto', compression = None, index=False)
    x.close()

script_path = os.getcwd()+ os.sep 
# copied_script_name = time.strftime("%Y-%m-%d_%H%M") + '_' + os.path.basename(__file__)
shutil.copy(script_path + 'process/sat/MODIS/process_MODIS_CHL_8day.py', code_folder + 'process_MODIS_CHL_8day.py')

stats_df = stats.build_stats_df_from_db_calls(tbl,server)
stats.update_stats_large(tbl, stats_df, 'Opedia',server)
