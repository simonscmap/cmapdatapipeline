import sys
import os

import pandas as pd
import numpy as np
import xarray as xr
import glob
from tqdm import tqdm

sys.path.append("cmapdata/ingest")

sys.path.append("ingest")
import vault_structure as vs
import DB
import metadata
import data_checks as dc



tbl = 'tblModis_CHL_cl1'
base_folder = f'{vs.satellite}{tbl}/raw/'
rep_folder = f'{vs.satellite}{tbl}/rep/'
code_folder= f'{vs.satellite}{tbl}/code/'

## New naming convention: https://oceandata.sci.gsfc.nasa.gov/cgi/getfile/AQUA_MODIS.20220720_20220727.L3m.8D.CHL.chlor_a.9km.NRT.nc

flist = np.sort(glob.glob(os.path.join(base_folder, f'*.nc')))
flist = glob.glob(base_folder+'AQUA_MODIS.20100218_*')
len(flist)

# ## testing
# qry = "select * from tblmodis_chl where time = '2002-07-04'"
# df_old = DB.dbRead(qry,'Rossby')

# df_old.describe()
# df.describe()

# df_p = pd.read_parquet(rep_folder+'tblModis_CHL_cl1_2002_07_04.parquet')
# df_p.describe()

# fil_o = f'{vs.satellite}tblModis_CHL/raw/A20021852002192.L3m_8D_CHL_chlor_a_9km.nc'
# xo = xr.open_dataset(fil_o)


# d1 = {'var_name':[], 'std_name':[], 'long_name':[], 'dtype':[], 'units':[], 'comment':[], 'flag_val':[], 'flag_def':[]}
# df_varnames = pd.DataFrame(data=d1)

# for varname, da in x.data_vars.items():
#     print(da.attrs)

# for varname, da in xo.data_vars.items():
#     print(da.attrs)

fil = flist[0]
for fil in tqdm(flist):
    fil_date = pd.to_datetime(
            fil.rsplit("/",1)[1].split(".",2)[1][0:8], format="%Y%m%d"
        ).strftime("%Y_%m_%d")    
    x = xr.open_dataset(fil)
    chl = x.drop_dims(['rgb','eightbitcolor'])
    df = chl.to_dataframe().reset_index()
    fdate = pd.to_datetime(
            fil.rsplit("/",1)[1].split(".",2)[1][0:8], format="%Y%m%d"
        )
    df.insert(0,"time", fdate)
    df["time"] = pd.to_datetime(df["time"])
    df["year"] = pd.to_datetime(df["time"]).dt.year
    df["month"] = pd.to_datetime(df["time"]).dt.month
    df["week"] = pd.to_datetime(df["time"]).dt.isocalendar().week
    df["dayofyear"] = pd.to_datetime(df["time"]).dt.dayofyear
    fil_name = os.path.basename(fil)
    path = f"{rep_folder.split('vault/')[1]}{tbl}_{fil_date.replace('-','_')}.parquet"
    df.to_parquet(f"{rep_folder}{tbl}_{fil_date.replace('-','_')}.parquet", index=False)      
    metadata.tblProcess_Queue_Process_Update(fil_name, path, tbl, 'Opedia', 'Rainier')
    x.close()


metadata.export_script_to_vault(tbl,'satellite','process/sat/MODIS/process_MODIS_CHL_8day_cl1.py','process.txt')

## Backfill to on prem
flist = glob.glob(rep_folder+'*.parquet')
server = 'rossby'
for fil in tqdm(flist):
    df = pd.read_parquet(fil)
    DB.toSQLbcp_wrapper(df,tbl,server)

## Fix time dtype
from multiprocessing import Pool
flist = glob.glob(rep_folder+'*.parquet')
def to_pq(fil):
    try:
        df = pd.read_parquet(fil)
        df['time'] = pd.to_datetime(df['time'])
        df.to_parquet(fil, index=False)
    except Exception as e:        
        print(str(e))   
        print(fil)
with Pool(processes=6) as pool:
    pool.map(to_pq,  tqdm(flist))
    pool.close() 
    pool.join()


## Check errors
fil = '/data/CMAP Data Submission Dropbox/Simons CMAP/vault/observation/remote/satellite/tblModis_CHL_cl1/rep/tblModis_CHL_cl1_2010_02_18.parquet'
flist = glob.glob(rep_folder+'*.parquet')
error_list = []
for fil in flist:
    try:
        df = pd.read_parquet(fil)
    except:
        print(fil)

## Check errors
flist = ['2019_06_02','2021_10_08','2018_11_17','2018_06_02','2018_01_09','2017_07_04','2016_09_29','2016_03_05','2015_08_21','2014_10_08','2014_06_10','2011_09_22','2007_10_24','2007_07_12','2006_08_29','2005_10_08','2004_06_17','2002_11_17']
fl = '2010_02_18'
for fl in flist:
    fil = rep_folder+f'tblModis_CHL_cl1_{fl}.parquet'
    to_pq(fil)
    df = pd.read_parquet(fil)

## Backfill holes
missing = ['2002-07-28', '2005-02-18', '2006-06-02', '2008-05-24', '2009-03-06', '2010-01-25', '2010-09-22', '2010-11-25', '2010-12-03', '2011-02-18', '2011-11-17', '2012-03-29', '2013-05-25', '2013-11-01', '2014-05-01', '2016-01-01', '2016-02-26', '2017-05-09', '2019-05-01', '2021-08-21']
flist = glob.glob(rep_folder+'*.parquet')
fil = missing[0]
for fil in tqdm(missing):
    miss_date = fil.replace('-','_')
    df = pd.read_parquet('/data/CMAP Data Submission Dropbox/Simons CMAP/vault/observation/remote/satellite/tblModis_CHL_cl1/rep/tblModis_CHL_cl1_'+miss_date+'.parquet')
    DB.toSQLbcp_wrapper(df,tbl,'mariana') 