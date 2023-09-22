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
i_list =  ['depth','lat','lon','month']

WOA_dir = vs.float_dir + tbl_names[1] + "/raw"

m = 1 
# flist_all = np.sort(glob.glob(os.path.join(WOA_dir, f'*{m}_01.nc'))) # 1deg
flist_all = np.sort(glob.glob(os.path.join(WOA_dir, f'*{m}_04.nc'))) ## qrtdeg

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
cols_df = [ x for x in cols_all_df if "M_" not in x ]
cols_mlt = [ x for x in cols_all_df if "M_" in x ]
cols_df.sort()
cols_mlt.sort()
# cols_df.remove('time')
for i in reversed(i_list):
    cols_df.insert(0,i)
    cols_mlt.insert(0,i)
cols_mlt.remove('depth')


df_woa = pd.DataFrame(columns=cols_df)
## Print column names for SQL table creation script
for c in df_woa.columns:
    if 'depth' in c or 'lat' in c or 'lon' in c:
        print('['+ c + '] [float] NOT NULL, ' )
    else:
        print('['+ c + '_clim] [float] NULL, ' )

for m in tqdm(month_list):
    df_data = []
    flist_all = np.sort(glob.glob(os.path.join(WOA_dir, f'*{m}_04.nc'))) 
    print(m)
    for fil in tqdm(flist_all):
        xdf = xr.open_dataset(fil, decode_times=False)  
        df = xdf.to_dataframe().reset_index() 
        df.drop(columns={'crs', 'time','lat_bnds', 'lon_bnds','depth_bnds', 'climatology_bounds'}, inplace=True) #'nbounds' doubles data
        df_n1 = df.query("nbounds == 1")
        df_n1 = df_n1.drop(columns='nbounds')
        df_n1.reset_index(inplace=True, drop=True)
        df_n0 = df.query("nbounds == 0")
        df_n0 = df_n0.drop(columns='nbounds')
        df_n0.reset_index(inplace=True, drop=True)
        if df_n0.equals(df_n1):
            df_data.append(df_n0)
        else:
            print(fil)
            break   

    if len(df_data[0])==len(df_data[1])==len(df_data[3])==len(df_data[4]):
        df_1 = df_data[0].copy()
        long_order = [1,3,4]
        for l in long_order:
            print(l)
            df_1[str(df_data[l].columns[3])], df_1[str(df_data[l].columns[4])], df_1[str(df_data[l].columns[5])], df_1[str(df_data[l].columns[6])], df_1[str(df_data[l].columns[7])], df_1[str(df_data[l].columns[8])], df_1[str(df_data[l].columns[9])], df_1[str(df_data[l].columns[10])] = df_data[l].iloc[:,3], df_data[l].iloc[:,4], df_data[l].iloc[:,5], df_data[l].iloc[:,6], df_data[l].iloc[:,7], df_data[l].iloc[:,8], df_data[l].iloc[:,9], df_data[l].iloc[:,10]
        
    else:
        print('appending different sizes')
        break


    ## df_1.shape df_1.describe() df_1.columns  3693600 rows x 51 columns
    # df_3 = pd.merge(df_1, df_2, how='left', on=['lat','lon','depth']) ## left & outer: 3693600 rows x 67 columns
    df_1['month'] = int(f'{m}')
    df_1 = df_1[cols_df] 
    df_1 = data.clean_data_df(df_1, True)
    print('data cleaned')
    DB.toSQLbcp_wrapper(df_1, 'tblWOA_2018_qrtdeg_Climatology', "Rossby")
    # DB.toSQLbcp_wrapper(df_1, 'tblWOA_2018_1deg_Climatology', "Mariana") 
    del df_1
    del df_data



# for d in df_data:
#     print(d.shape)
# df_woa = pd.concat(df_data, axis=0)        

# ## Check MLD
# df_data[2].describe()

# ## Check nutrient grid
# df_nutr = df_data[8]
# df_nutr_0 = df_nutr.query("depth == 0")

# ## Check salinity grid
# df_sal = df_data[0]
# df_sal_0 = df_sal.query("depth == 0")

# ## Check join grid
# df_1_0 = df_1.query("depth == 0")
# df_2_0 = df_2.query("depth == 0")
# df_3_0 = df_3.query("depth == 0")

# flist_all = np.sort(glob.glob(os.path.join(WOA_dir, '*_01.nc')))
# flist_all = np.sort(glob.glob(os.path.join(WOA_dir, '*_04.nc')))
# fil = flist_all[1]
# xdf = xr.open_dataset(fil, decode_times=False)

# xdf.dims
# xdf.attrs
# xdf.data_vars.items()
# xdf.time.attrs
# xdf.p_sd.attrs
# xdf.p_dd.attrs

# for x in df_woa.dtypes:
#     print(x)

# col_list = list(set(cols + ['var_name']))
# df_meta = pd.DataFrame(columns=col_list)

# for fil in flist_all:
#     xdf = xr.open_dataset(fil, decode_times=False)
#     for varname, da in xdf.data_vars.items():
#         print(da)
#         s = pd.DataFrame(da.attrs, index=[0])
#         s['var_name'] = varname     
#         df_meta = df_meta.append(s)

# df_meta.to_csv(
#         vs.float_dir + tbl_names[1] + '/metadata/' + tbl_names[1] + '_metadata.csv',
#         sep=",",
#         index=False)

