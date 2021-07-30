from cmapingest import vault_structure as vs
from cmapingest import common as cmn
from cmapingest import DB
from cmapingest import data 

import pandas as pd
import xarray as xr
import glob 
import numpy as np
from tqdm import tqdm 

NRT_pisces_dir = vs.collected_data + "model/CMEMS_Forecast_PISCES_001_028/"

flist = np.sort(glob.glob(NRT_pisces_dir + '*.nc'))
for fil in tqdm(flist):
    xdf = xr.open_dataset(fil)
    df = xdf.to_dataframe().reset_index()
    df = data.add_day_week_month_year_clim(df)
    df = df[['time','latitude','longitude','depth','chl','fe','no3','nppv','o2','ph','phyc','po4','si','spco2','year','month','week','dayofyear']]
    df.rename({'latitude':'lat','longitude':'lon'},inplace=True)
    DB.toSQLbcp_wrapper(df, 'tblPisces_Forecast', "Rossby")

    df.to_csv(
        vs.model + "tblPisces_Forecast/nrt/" + fil.strip(".nc") + ".csv",
        sep=",",
        index=False)
