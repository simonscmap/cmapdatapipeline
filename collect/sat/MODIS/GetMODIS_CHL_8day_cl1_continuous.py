import sys
import os
import time
import datetime as dt
from multiprocessing import Pool

sys.path.append("ingest")
sys.path.append("../../../")

import vault_structure as vs
import credentials as cr
import DB
import metadata
import api_checks as api

#https://oceandata.sci.gsfc.nasa.gov/directaccess/MODIS-Aqua/Mapped/8-Day/9km/chlor_a/
#https://oceandata.sci.gsfc.nasa.gov/cgi/getfile/AQUA_MODIS.20020704_20020711.L3m.8D.CHL.chlor_a.9km.nc

tbl = 'tblModis_CHL_cl1'

base_folder = f'{vs.satellite}{tbl}/raw/'

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


def get_CHL(start_date,retry=False):
    ## Handle beginning and end of year exceptions
    if start_date.month == 1 and start_date.day <=6:
        start_date = dt.date(start_date.year, 1, 1)
    if start_date.month == 12 and start_date.day >24:
        date2 = dt.date(start_date.year, 12, 31)
    else:
        date2 = start_date + dt.timedelta(days=7)
    start_index = start_date.strftime('%Y%m%d')
    end_index = date2.strftime('%Y%m%d')
    file_url = f"https://oceandata.sci.gsfc.nasa.gov/cgi/getfile/AQUA_MODIS.{start_index}_{end_index}.L3m.8D.CHL.chlor_a.9km.nc"
    Original_Name = f"AQUA_MODIS.{start_index}_{end_index}.L3m.8D.CHL.chlor_a.9km.nc"
    save_path = base_folder+Original_Name
    wget_str = f'wget --user={cr.usr_earthdata} --password={cr.psw_earthdata} --auth-no-challenge=on "{file_url}" -O "{save_path}"'  
    downloaded = os.path.isfile(base_folder + Original_Name) 
    if not downloaded:
        try:
            os.system(wget_str)
                ## Remove empty downloads
            Error_Date = start_date.strftime('%Y_%m_%d')    
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
            print("No file found for date: " + start_index + "_" + end_index)
            metadata.tblProcess_Queue_Download_Insert(Error_Date, tbl, 'Opedia', 'Rainier','No data')
    else:
        print("Already downloaded: " +  start_index + "_" + end_index)


def retryError(tbl):
    qry = f"SELECT Original_Name from dbo.tblProcess_Queue WHERE Table_Name = '{tbl}' AND Error_Str IS NOT NULL"
    df_err = DB.dbRead(qry, 'Rainier')
    dt_list = df_err['Original_Name'].to_list()
    if len(dt_list)>0:
        dt_list = [dt.datetime.strptime(x.strip(), '%Y_%m_%d').date() for x in dt_list]
        for dat in dt_list:
            get_CHL(dat, True)


retryError(tbl)

start_date = getMaxDate(tbl)
end_date = api.minDateCluster('tblModis_CHL_NRT').to_pydatetime().date()

delta = dt.timedelta(days=8)

start_date += delta
# end_date -= delta
# end_date += delta

while start_date <= end_date:
    get_CHL(start_date)
    start_date = start_date + delta
    if start_date.month == 1 and start_date.day <=6:
        start_date = dt.date(start_date.year, 1, 1)


# df = api.query("select time, count(*) cnt from tblModis_CHL_nrt group by time")
# yr = '2023', mnth = '02', day = '18'
# api.clusterModify(f"delete from tblModis_CHL_NRT where time='{yr}-{mnth}-{day}'")
# dlist = []
# while start_date <= end_date:
#     dlist.append(start_date)
#     start_date += delta
#     if start_date.month == 1 and start_date.day <=6:
#         start_date = dt.date(start_date.year, 1, 1)

# with Pool() as pool:
#     pool.map(get_CHL,  dlist)
#     pool.close() 
#     pool.join()

# metadata.export_script_to_vault(tbl,'satellite','collect/sat/MODIS/GetMODIS_CHL_8day_cl1.py','collect.txt')       