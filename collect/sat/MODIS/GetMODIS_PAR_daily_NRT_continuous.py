import sys
import os
import time
import datetime as dt
import pandas as pd
from multiprocessing import Pool

sys.path.append("ingest")
sys.path.append("../../../")

import vault_structure as vs
import credentials as cr
import DB
import metadata
import api_checks as api

#https://oceandata.sci.gsfc.nasa.gov/cgi/getfile/AQUA_MODIS.20230110.L3m.DAY.PAR.par.9km.NRT.nc

tbl = 'tblModis_PAR_NRT'

# vs.leafStruc(vs.satellite+tbl)
base_folder = f'{vs.satellite}{tbl}/raw/'

#AQUA_MODIS.20230228.L3m.DAY.PAR.par.9km.nc


def getMaxDate(tbl):
    ## Check tblIngestion_Queue for downloaded but not ingested
    qry = f"SELECT Path from dbo.tblIngestion_Queue WHERE Table_Name = '{tbl}' AND Ingested IS NULL"
    df_ing = DB.dbRead(qry, 'Rainier')

    if len(df_ing) == 0:
        qry = f"SELECT max(path) mx from dbo.tblIngestion_Queue WHERE Table_Name = '{tbl}' AND Ingested IS NOT NULL"
        mx_path = DB.dbRead(qry,'Rainier')
        path_date = mx_path['mx'][0].split('.parquet')[0].rsplit(tbl+'_',1)[1]
        yr, mo, day = path_date.split('_')
        max_path_date = dt.date(int(yr),int(mo),int(day)) 
        
        qry = f"SELECT max(original_name) mx from dbo.tblProcess_Queue WHERE Table_Name = '{tbl}' AND Error_str IS NOT NULL"
        mx_name = DB.dbRead(qry,'Rainier')
        if mx_name['mx'][0] == None:
            max_name_date = dt.date(1900,1,1)
        else: 
            yr, mo, day = mx_name['mx'][0].strip().split('_')
            max_name_date = dt.date(int(yr),int(mo),int(day)) 

        max_data_date = api.maxDateCluster(tbl)   


        max_date = max([max_path_date,max_name_date,max_data_date])
    else:
        last_path = df_ing['Path'].max()
        path_date = last_path.split('.parquet')[0].rsplit(tbl+'_',1)[1]
        yr, mo, day = path_date.split('_')
        max_date = dt.date(int(yr),int(mo),int(day))
    return max_date

def get_PAR(start_date,retry=False):
    start_index = start_date.strftime('%Y%m%d')
    file_url = f"https://oceandata.sci.gsfc.nasa.gov/cgi/getfile/AQUA_MODIS.{start_index}.L3m.DAY.PAR.par.9km.NRT.nc"
    Original_Name =f"AQUA_MODIS.{start_index}.L3m.DAY.PAR.par.9km.NRT.nc"
    save_path = base_folder+Original_Name
    wget_str = f'wget --user={cr.usr_earthdata} --password={cr.psw_earthdata} --auth-no-challenge=on "{file_url}" -O "{save_path}"'
    downloaded = os.path.isfile(base_folder + Original_Name) 
    Error_Date = start_date.strftime('%Y_%m_%d') 
    if not downloaded:
        try:
            os.system(wget_str)
                ## Remove empty downloads
            if os.path.getsize(save_path) == 0:
                print(f'empty download for {Error_Date}')
                os.remove(save_path)
                if not retry:
                    metadata.tblProcess_Queue_Download_Insert(Error_Date, tbl, 'Opedia', 'Rainier','Download Error')
            else:
                if retry:
                    metadata.tblProcess_Queue_Download_Error_Update(Error_Date, Original_Name,  tbl, 'Opedia', 'Rainier')
                    print(f"Successful retry for {Error_Date}")
                else:
                    metadata.tblProcess_Queue_Download_Insert(Original_Name, tbl, 'Opedia', 'Rainier')
            time.sleep(2)
        except:
            print("No file found for date: " + Error_Date )
            metadata.tblProcess_Queue_Download_Insert(Error_Date, tbl, 'Opedia', 'Rainier','No data')
    else:
        print("Already downloaded: " +  Error_Date )

def retryError(tbl):
    qry = f"SELECT Original_Name from dbo.tblProcess_Queue WHERE Table_Name = '{tbl}' AND Error_Str IS NOT NULL"
    df_err = DB.dbRead(qry, 'Rainier')
    dt_list = df_err['Original_Name'].to_list()
    if len(dt_list)>0:
        dt_list = [dt.datetime.strptime(x.strip(), '%Y_%m_%d').date() for x in dt_list]
        for par_dt in dt_list:
            get_PAR(par_dt, True)


retryError(tbl)
start_date = getMaxDate(tbl)
end_date = dt.date.today()
delta = dt.timedelta(days=1)
start_date += delta
end_date -= delta

while start_date <= end_date:
    get_PAR(start_date)
    start_date = start_date + delta

# dlist = []
# while start_date <= end_date:
#     dlist.append(start_date)
#     start_date += delta


# with Pool() as pool:
#     pool.map(get_PAR,  dlist)
#     pool.close() 
#     pool.join()
        