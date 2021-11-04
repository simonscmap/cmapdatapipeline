import sys
import os

sys.path.append("../..")

from ingest import vault_structure as vs
from ingest import DB

import pandas as pd
import xarray as xr
import glob 
import numpy as np
import shutil
from tqdm import tqdm 

MLD_dir = vs.collected_data + "model/ARGO_MLD_Climatology/"

flist_all = np.sort(glob.glob(os.path.join(MLD_dir, '*.nc')))
fsort = sorted(flist_all, reverse=True)[:1]


# vs.leafStruc(vs.satellite + "tblArgo_MLD_Climatology")

for fil in tqdm(fsort):
    xdf = xr.open_dataset(fil)
    df = xdf.to_dataframe().reset_index()
    df = df[['month','lat','lon','mld_da_mean', 'mld_dt_mean', 'mld_da_median', 'mld_dt_median', 'mld_da_std', 'mld_dt_std', 'mld_da_max', 'mld_dt_max', 'mlpd_da', 'mlpd_dt', 'mlt_da', 'mlt_dt', 'mls_da', 'mls_dt', 'num']]
    # df = df[['month','lat','lon','mld_da_mean', 'mld_dt_mean', 'mld_da_median', 'mld_dt_median', 'mld_da_std', 'mld_dt_std', 'mld_da_max', 'mld_dt_max', 'mlpd_da', 'mlpd_dt', 'mlt_da', 'mlt_dt', 'mls_da', 'mls_dt', 'num_profiles']]
    DB.toSQLbcp_wrapper(df, 'tblArgo_MLD_Climatology', "Beast")
    print(fil)
    
    ## Export copy of data ingested to DB as parquet file into vault
    df.to_parquet(
        vs.model
        + "tblArgo_MLD_Climatology/rep/"
        + "tblArgo_MLD_Climatology.parquet"
    )
    print('Parquet saved')
    ## Copy (or move?) original downloaded file into vault
    shutil.copy(
        fil, vs.model + "tblArgo_MLD_Climatology/raw"
    )
    print('NetCDF copied')