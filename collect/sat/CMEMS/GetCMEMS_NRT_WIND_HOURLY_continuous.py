import sys
import os
import numpy as np
from tqdm import tqdm
import datetime
import glob

# sys.path.append("cmapdata")
# sys.path.append("cmapdata/ingest")
sys.path.append("ingest")
# sys.path.append("../../../ingest")
sys.path.append("../../../")
sys.path.append("../../../ingest")

import vault_structure as vs
import DB
import metadata
import credentials as cr
import copernicusmarine



tbl = 'tblWind_NRT_hourly'

base_folder = f'{vs.satellite}{tbl}/raw/'
# output_dir = base_folder.replace(" ", "\\ ")
output_dir = os.path.normpath(base_folder)

usr = cr.usr_cmem
psw = cr.psw_cmem

def getMaxDate(tbl):
    ## Check tblIngestion_Queue for downloaded but not ingested
    qry = f"SELECT Path from dbo.tblIngestion_Queue WHERE Table_Name = '{tbl}' AND Ingested IS NULL"
    df_ing = DB.dbRead(qry, 'Rainier')
    ### Add in check for Process_Queue
    qry = f"SELECT Original_Name from dbo.tblProcess_Queue WHERE Table_Name = '{tbl}' AND Processed IS NULL"
    df_pro = DB.dbRead(qry, 'Rainier')
    if len(df_ing) == 0 and len(df_pro) == 0:
        ## Change to pull from cluster
        # qry = f"SELECT MAX(Time) as Time_max FROM {tbl}"
        # df_mx = DB.dbRead(qry, 'Rossby')
        # max_date = df_mx['Time_max'][0]
        qry = f"SELECT max(path) mx from dbo.tblIngestion_Queue WHERE Table_Name = '{tbl}' AND Ingested IS NOT NULL"
        mx_path = DB.dbRead(qry,'Rainier')
        path_date = mx_path['mx'][0].split('.parquet')[0].split('PT1H_',1)[1].split('_',1)[0]
        yr = path_date[:4]
        mo = path_date[4:6]
        day = path_date[6:8]
        max_date = datetime.date(int(yr),int(mo),int(day))
    elif len(df_ing)>0:
        ## Add checks that something didn't go wrong
        last_path = df_ing['Path'].max()
        path_date = last_path.split('.parquet')[0].split('PT1H_',1)[1].split('_',1)[0]
        yr = path_date[:4]
        mo = path_date[4:6]
        day = path_date[6:8]
        ## Check there are 24 files for last date downloaded
        qry = f"SELECT count(*) cnt from dbo.tblIngestion_Queue WHERE Table_Name = '{tbl}' AND Ingested IS NULL AND Path like '%PT1H_{yr}{mo}{day}%'"
        max_cnt = DB.dbRead(qry,'Rainier')
        max_date = datetime.date(int(yr),int(mo),int(day))
        ## If missing some hours, download that day again
        ## Try / except in download code skips over files already existing
        if max_cnt['cnt'][0] != 24:
            max_date = max_date - datetime.timedelta(days=1)
    else:
        last_path = df_pro['Original_Name'].max()
        path_date = last_path.split('.parquet')[0].split('PT1H_',1)[1].split('_',1)[0]
        yr = path_date[:4]
        mo = path_date[4:6]
        day = path_date[6:8]
        max_date = datetime.date(int(yr),int(mo),int(day))
    return max_date
 
# ## Run time for one day: 9:45:43 - 9:50:23. 4 min 40sec
# def wget_file(output_dir, yr, mnth, day, usr, psw):
#     hr_list = np.arange(0, 24, 1, int)
#     for hr_int in hr_list:
#         hr = str(hr_int).zfill(2)
#         fpath = f"ftp://nrt.cmems-du.eu/Core/WIND_GLO_PHY_L4_NRT_012_004/cmems_obs-wind_glo_phy_nrt_l4_0.125deg_PT1H/{yr}/{mnth}/cmems_obs-wind_glo_phy_nrt_l4_0.125deg_PT1H_{yr}{mnth}{day}{hr}_*"
#         fname = f"cmems_obs-wind_glo_phy_nrt_l4_0.125deg_PT1H_{yr}{mnth}{day}{hr}.nc"
#         os.system(
#             f"""wget --no-parent -nd -r -m --ftp-user={usr} --ftp-password={psw} {fpath} -O  {output_dir}{fname}"""
#         )
#         metadata.tblProcess_Queue_Download_Insert(fname, tbl, 'Opedia', 'Rainier')

#/Core/WIND_GLO_PHY_L4_NRT_012_004/cmems_obs-wind_glo_phy_nrt_l4_0.125deg_PT1H/2020/07/cmems_obs-wind_glo_phy_nrt_l4_0.125deg_PT1H_2020070123_R20200701T12_11.nc
## Run time for one day: 10:09:48 - 10:13:28  3 min 40sec

def wget_file(yr, mnth, day, retry=False):
    # fpath = f"ftp://nrt.cmems-du.eu/Core/WIND_GLO_PHY_L4_NRT_012_004/cmems_obs-wind_glo_phy_nrt_l4_0.125deg_PT1H/{yr}/{mnth}/cmems_obs-wind_glo_phy_nrt_l4_0.125deg_PT1H_{yr}{mnth}{day}*"    
    # os.system(
    #     f"""wget --no-parent -nd -r -m --ftp-user={usr} --ftp-password={psw} {fpath} -P {output_dir}"""
    # )

    copernicusmarine.get( 
                        dataset_id="cmems_obs-wind_glo_phy_nrt_l4_0.125deg_PT1H",
                        output_directory=output_dir,
                        username=cr.usr_cmem,
                        password=cr.psw_cmem,
                        no_directories=True,
                        show_outputnames=True,
                        overwrite_output_data=True,
                        force_download=True,
                        filter=f"*{datetime.date(int(yr), int(mnth), int(day)).strftime("%Y%m%d")}*_R*.nc"
                        )

    hr_list = np.arange(0, 24, 1, int)
    for hr_int in hr_list:
        hr = str(hr_int).zfill(2)
        pname = glob.glob(f"{base_folder}cmems_obs-wind_glo_phy_nrt_l4_0.125deg_PT1H_{yr}{mnth}{day}{hr}*")
        if len(pname) == 0:
            print(f'### No file found for {yr}{mnth}{day}{hr} ')
            if not retry:
                pass
                metadata.tblProcess_Queue_Download_Insert(f"{yr}_{mnth}_{day}_{hr}", tbl, 'Opedia', 'Rainier','Download Error')
            continue
        elif len(pname) > 1:
             print(f'### Multiple download options: {pname}')
             sys.exit()
        else:
            fname = pname[0].split(f"{base_folder}")[1]
            if retry:
                Error_Date = f"{yr}_{mnth}_{day}_{hr}"
                metadata.tblProcess_Queue_Download_Error_Update(Error_Date, fname, tbl, 'Opedia', 'Rainier')
                print(f"Successful retry for {Error_Date}")                
            try:
                pass
                metadata.tblProcess_Queue_Download_Insert(fname, tbl, 'Opedia', 'Rainier')
            except:
                print(f"File already downloaded: {fname}")
                continue

def retryError(tbl):
    qry = f"SELECT DISTINCT left(Original_Name,10) Original_Date from dbo.tblProcess_Queue WHERE Table_Name = '{tbl}' AND Error_Str IS NOT NULL"
    df_err = DB.dbRead(qry, 'Rainier')
    dt_list = df_err['Original_Date'].to_list()
    if len(dt_list)>0:
        dt_list = [datetime.datetime.strptime(x.strip(), '%Y_%m_%d').date() for x in dt_list]
        for dt in dt_list:
            yr = dt.year
            mnth = f"{dt:%m}"
            day = f"{dt:%d}"
            wget_file(yr, mnth, day, True)



# ### Redownload old data to check if different
# yr = 2020
# mnth = '07'
# day = '01'
# hr = '00'
# fpath = f"ftp://nrt.cmems-du.eu/Core/WIND_GLO_PHY_L4_NRT_012_004/cmems_obs-wind_glo_phy_nrt_l4_0.125deg_PT1H/{yr}/{mnth}/cmems_obs-wind_glo_phy_nrt_l4_0.125deg_PT1H_{yr}{mnth}{day}{hr}*"    
# os.system(
#         f"""wget --no-parent -nd -r -m --ftp-user={user} --ftp-password={pw} {fpath} -O {output_dir}datacheck_{yr}{mnth}{day}{hr}.nc"""
#     )



retryError(tbl)

max_date = getMaxDate(tbl)
print(f"Last {tbl} data downloaded: {max_date}")
end_date = datetime.date.today()
# end_date = datetime.date(2022, 9, 11)
delta = datetime.timedelta(days=1)
max_date += delta
## New data on one day delay
end_date -= delta
## ran thru 2/13
## Oct 2022 start 2023-02-14 12:07:31, FINISHED --2023-02-14 14:10:33. 2hr download time



while max_date <= end_date:
    yr = max_date.year
    mnth = f"{max_date:%m}"
    day = f"{max_date:%d}"
    wget_file(yr, mnth, day)
    max_date += delta


        


