import sys
import os
import datetime
import requests
import pandas as pd 

sys.path.append("ingest")
sys.path.append("../../../")
import vault_structure as vs
import DB
import metadata
import api_checks as api

tbl = 'tblSST_AVHRR_OI_NRT'

## https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/AVHRR_OI-NCEI-L4-GLOB-v2.1/20190428120000-NCEI-L4_GHRSST-SSTblend-AVHRR_OI-GLOB-v02.0-fv02.1.nc
## https://podaac.jpl.nasa.gov/dataset/AVHRR_OI-NCEI-L4-GLOB-v2.1


def getMaxDate(tbl):
    ## Check tblIngestion_Queue for downloaded but not ingested
    qry = f"SELECT Path from dbo.tblIngestion_Queue WHERE Table_Name = '{tbl}' AND Ingested IS NULL"
    df_ing = DB.dbRead(qry, 'Rainier')

    if len(df_ing) == 0:
        qry = f"SELECT max(path) mx from dbo.tblIngestion_Queue WHERE Table_Name = '{tbl}' AND Ingested IS NOT NULL"
        mx_path = DB.dbRead(qry,'Rainier')
        path_date = mx_path['mx'][0].split('.parquet')[0].rsplit(tbl+'_',1)[1]
        yr, mo, day = path_date.split('_')
        max_path_date = datetime.date(int(yr),int(mo),int(day)) 
        
        qry = f"SELECT max(original_name) mx from dbo.tblProcess_Queue WHERE Table_Name = '{tbl}' AND Error_str IS NOT NULL"
        mx_name = DB.dbRead(qry,'Rainier')
        if mx_name['mx'][0] == None:
            max_name_date = datetime.date(1900,1,1)
        else: 
            yr, mo, day = mx_name['mx'][0].strip().split('_')
            max_name_date = datetime.date(int(yr),int(mo),int(day))              
            
        max_data_date = api.maxDateCluster(tbl)     
        max_date = max([max_path_date,max_name_date,max_data_date])
    else:
        last_path = df_ing['Path'].max()
        path_date = last_path.split('.parquet')[0].rsplit(tbl+'_',1)[1]
        yr, mo, day = path_date.split('_')
        max_date = datetime.date(int(yr),int(mo),int(day))
    return max_date

def getSST(yr, month, day, retry=False):
    Original_Name = f'{yr}{month}{day}120000-NCEI-L4_GHRSST-SSTblend-AVHRR_OI-GLOB-v02.0-fv02.1.nc'
    qry = f"select * from dbo.tblProcess_Queue WHERE Table_Name = '{tbl}' AND Original_Name = '{Original_Name}'"
    rerun_check = DB.dbRead(qry, 'Rainier')
    if len(rerun_check) == 0:
        file_url = f'https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/AVHRR_OI-NCEI-L4-GLOB-v2.1/{yr}{month}{day}120000-NCEI-L4_GHRSST-SSTblend-AVHRR_OI-GLOB-v02.0-fv02.1.nc'
        save_path = f'{vs.satellite + tbl}/raw/{yr}{month}{day}120000-NCEI-L4_GHRSST-SSTblend-AVHRR_OI-GLOB-v02.0-fv02.1.nc'
        wget_str = f'wget --no-check-certificate "{file_url}" -O "{save_path}"'
        os.system(wget_str)
        ## Remove empty downloads
        if os.path.getsize(save_path) == 0:
            print(f'empty download for {yr}{month}{day}')
            if not retry:
                metadata.tblProcess_Queue_Download_Insert(f"{yr}_{month}_{day}", tbl, 'Opedia', 'Rainier','Download Error')
                os.remove(save_path)
        else:
            if retry:
                Error_Date = f"{yr}_{month}_{day}"
                metadata.tblProcess_Queue_Download_Error_Update(Error_Date, Original_Name,  tbl, 'Opedia', 'Rainier')
                print(f"Successful retry for {Error_Date}")
            else:
                metadata.tblProcess_Queue_Download_Insert(Original_Name, tbl, 'Opedia', 'Rainier')
    else:
        print(f"### File {Original_Name} already in db")

def retryError(tbl):
    qry = f"SELECT Original_Name from dbo.tblProcess_Queue WHERE Table_Name = '{tbl}' AND Error_Str IS NOT NULL"
    df_err = DB.dbRead(qry, 'Rainier')
    dt_list = df_err['Original_Name'].to_list()
    if len(dt_list)>0:
        dt_list = [datetime.datetime.strptime(x.strip(), '%Y_%m_%d').date() for x in dt_list]
        for dt in dt_list:
            yr = dt.year
            month = f"{dt:%m}"
            day = f"{dt:%d}"
            getSST(yr, month, day, True)


retryError(tbl)

# max_date = datetime.date(2022, 2, 6)
# end_date = datetime.date(2022, 2, 5)
max_date = getMaxDate(tbl)
end_date = datetime.date.today()
delta = datetime.timedelta(days=1)
max_date += delta
## New data on one day delay
end_date -= delta


while max_date <= end_date:
    yr = max_date.year
    month = f"{max_date:%m}"
    day = f"{max_date:%d}"
    getSST(yr, month, day)
    max_date += delta


