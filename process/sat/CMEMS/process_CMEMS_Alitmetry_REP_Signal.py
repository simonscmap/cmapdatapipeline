import sys
import os

import pandas as pd
import numpy as np
import xarray as xr
import glob
from tqdm import tqdm


sys.path.append("cmapdata/ingest")
import vault_structure as vs
import data
import DB 
import data_checks as dc


tbl = 'tblAltimetry_REP_Signal'
base_folder = f'{vs.satellite}{tbl}/raw/'
meta_folder = f'{vs.satellite}{tbl}/metadata/'
rep_folder = f'{vs.satellite}{tbl}/rep/'


date_import = ['dt_global_allsat_phy_l4_199907']
date_import = ['A2016']

flist_all = np.sort(glob.glob(os.path.join(base_folder, f'*.nc')))

# flist = [i for i in flist_all if date_import[0] in i or date_import[1] in i]
# flist = [i for i in flist_all if date_import[0] in i]
len(flist_all)
len(flist)
# fil = flist_all[2126]
# d = {'year':[]}
# df=pd.DataFrame(data=d)
# for fil in flist_all:
#     yr = fil.split('raw/',1)[1].split('dt_global_allsat_phy_l4_')[1][:4]
#     d2 = {'year':[yr]}
#     temp_df = pd.DataFrame(data=d2)
#     df = df.append(temp_df, ignore_index=True)

# df.year.value_counts()
# df1 = df1.sort_values(["time", "latitude","longitude"], ascending = (True, True,True))
# df2 = df2.sort_values(["time", "latitude","longitude"], ascending = (True, True,True))
# df1=df1.reset_index(drop=True)
# df2=df2.reset_index(drop=True)

d = {'filename':[],'null_check':[],'unique_check':[]}
df_error=pd.DataFrame(data=d)

day_list = range(10, 13, 1) #through 2000
for yr in tqdm(day_list):
    flist = [i for i in flist_all if 'dt_global_allsat_phy_l4_1999'+str(yr) in i]
    for fil in tqdm(flist):
        x = xr.open_dataset(fil)
        df = x.to_dataframe().reset_index()
        df = df.query('nv == 1')
        df = df[['time','latitude', 'longitude', 'sla', 'err_sla', 'ugosa', 'err_ugosa', 'vgosa', 'err_vgosa', 'adt', 'ugos', 'vgos', 'flag_ice', 'tpa_correction']]
        df = df.sort_values(["time", "latitude","longitude"], ascending = (True, True,True))
        df.rename(columns={'latitude':'lat', 'longitude':'lon'}, inplace = True)
        df = data.add_day_week_month_year_clim(df)
        null_check = dc.check_df_nulls(df, tbl, "Beast")
        unique_check = dc.check_df_constraint(df, 'tblKOK1606_Gradients1_Surface_O2Ar_NCP', 'Rossby')
        if null_check == 0 and unique_check == 0:
            DB.toSQLbcp_wrapper(df, tbl, "Beast") 
        else:
            d2 = {'filename':[fil],'null_check':[null_check],'unique_check':[unique_check]}
            temp_df = pd.DataFrame(data=d2)
            df_error = df_error.append(temp_df, ignore_index=True)     


yr=1993
fil=flist[0]
yr_list = range(1993, 2021, 1) #through 2000
for yr in tqdm(yr_list):
    flist = [i for i in flist_all if 'dt_global_allsat_phy_l4_'+str(yr) in i]
    for fil in tqdm(flist):
        x = xr.open_dataset(fil)
        df = x.to_dataframe().reset_index()
        x.close()
        df = df.query('nv == 1')
        df = df[['time','latitude', 'longitude', 'sla', 'err_sla', 'ugosa', 'err_ugosa', 'vgosa', 'err_vgosa', 'adt', 'ugos', 'vgos', 'flag_ice', 'tpa_correction']]
        df = df.sort_values(["time", "latitude","longitude"], ascending = (True, True,True))
        df.rename(columns={'latitude':'lat', 'longitude':'lon'}, inplace = True)
        df = data.add_day_week_month_year_clim(df)
        null_check = 0 #dc.check_df_nulls(df, tbl, "Rossby")
        unique_check = 0 #dc.check_df_constraint(df, 'tblKOK1606_Gradients1_Surface_O2Ar_NCP', 'Rossby')
        if null_check == 0 and unique_check == 0:
            DB.toSQLbcp_wrapper(df, tbl, "Mariana") 
            # df.to_parquet(rep_folder+fil.rsplit('/',1)[1].split('.',1)[0]+'.parquet')
            del df
        else:
            d2 = {'filename':[fil],'null_check':[null_check],'unique_check':[unique_check]}
            temp_df = pd.DataFrame(data=d2)
            df_error = df_error.append(temp_df, ignore_index=True)   
            del df  
df_error.to_excel(meta_folder+'error_checks_Rossby.xlsx')



indices = [
"""CREATE UNIQUE CLUSTERED INDEX [IX_tblAltimetry_REP_Signal_time_lat_lon] ON [dbo].[tblAltimetry_REP_Signal]
(
	[time] ASC,
	[lat] ASC,
	[lon] ASC
) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [FG4]"""
,

"""CREATE NONCLUSTERED INDEX [IX_tblAltimetry_REP_Signal_dayofyear_lat_lon] ON [dbo].[tblAltimetry_REP_Signal]
(
	[dayofyear] ASC,
	[lat] ASC,
	[lon] ASC
)
INCLUDE([sla],[ugosa],[vgosa],[adt],[ugos],[vgos]) 
WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) 
ON [FG4]"""
,

"""CREATE NONCLUSTERED INDEX [IX_tblAltimetry_REP_Signal_week_lat_lon] ON [dbo].[tblAltimetry_REP_Signal]
(
	[week] ASC,
	[lat] ASC,
	[lon] ASC
)
INCLUDE([sla],[ugosa],[vgosa],[adt],[ugos],[vgos]) 
WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) 
ON [FG4]"""
,

"""CREATE NONCLUSTERED INDEX [IX_tblAltimetry_REP_Signal_month_lat_lon] ON [dbo].[tblAltimetry_REP_Signal]
(
	[month] ASC,
	[lat] ASC,
	[lon] ASC
)
INCLUDE([sla],[ugosa],[vgosa],[adt],[ugos],[vgos]) 
WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) 
ON [FG4]""",

"""CREATE NONCLUSTERED INDEX [IX_tblAltimetry_REP_Signal_year_lat_lon] ON [dbo].[tblAltimetry_REP_Signal]
(
	[year] ASC,
	[lat] ASC,
	[lon] ASC
)
INCLUDE([sla],[ugosa],[vgosa],[adt],[ugos],[vgos]) 
WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) 
ON [FG4]"""
]


for i in  tqdm(indices):
    DB.queryExecute('Mariana', i)