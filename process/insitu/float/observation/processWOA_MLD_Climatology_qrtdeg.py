import sys
import os
from tqdm import tqdm 
import pandas as pd
import numpy as np
import xarray as xr
import glob

sys.path.append("/ingest")
import vault_structure as vs
import DB
import data

month_list = ['01','02','03','04','05','06','07','08','09','10','11','12']


tbl_names = ['tblWOA_2018_1deg_Climatology', 'tblWOA_2018_qrtdeg_Climatology']
i_list =  ['lat','lon','month']

WOA_dir = vs.float_dir + tbl_names[1] + "/raw"

m = '01' 
flist_all = np.sort(glob.glob(os.path.join(WOA_dir, f'*_M02{m}_04.nc'))) ## qrtdeg
# flist_all = np.sort(glob.glob(os.path.join(WOA_dir, f'*_M02{m}_01.nc'))) # 1deg

cols = []
var_name = []
for fil in flist_all:
    xdf = xr.open_dataset(fil, decode_times=False)
    for varname, da in xdf.data_vars.items():
        cols = cols + list(da.attrs.keys())
        var_name.append(varname)

## Drop _bounds and _bnds from unique list of varnames
cols_all_df = [ x for x in set(var_name) if "_b" not in x ]
cols_all_df.remove('crs')
cols_mlt = [ x for x in cols_all_df if "M_" in x ]
cols_mlt.sort()
# cols_df.remove('time')
for i in reversed(i_list):
    cols_mlt.insert(0,i)


#fil = flist_all[0]
for m in tqdm(month_list):
    flist_all = np.sort(glob.glob(os.path.join(WOA_dir, f'*_M02{m}_04.nc'))) 
    print(m)
    for fil in tqdm(flist_all):
        xdf = xr.open_dataset(fil, decode_times=False)  
        # xdf.dims xdf.lat
        # xdf.attrs
        # for v in xdf.data_vars.items():
        #     print(v)
        # xdf.nbounds.attrs
        #df_n0.head df_mlt.columns
        df = xdf.to_dataframe().reset_index() 
        df.drop(columns={'crs', 'time','lat_bnds', 'lon_bnds','depth_bnds', 'climatology_bounds'}, inplace=True) #'nbounds' doubles data
        df_n1 = df.query("nbounds == 1")
        df_n1 = df_n1.drop(columns='nbounds')
        df_n1.reset_index(inplace=True, drop=True)
        df_n0 = df.query("nbounds == 0")
        df_n0 = df_n0.drop(columns='nbounds')
        df_n0.reset_index(inplace=True, drop=True)
        if df_n0.equals(df_n1):
            df_mlt = df_n0
            df_mlt.drop(columns='depth', inplace=True)
            df_mlt['month'] = int(f'{m}')
            df_mlt = df_mlt[cols_mlt] 
            df_mlt = data.clean_data_df(df_mlt, True)
            print('data cleaned')
            DB.toSQLbcp_wrapper(df_mlt, 'tblWOA_2018_MLD_qrtdeg_Climatology', "Rossby")
            del df_mlt
            
        else:
            print(fil)
            break



