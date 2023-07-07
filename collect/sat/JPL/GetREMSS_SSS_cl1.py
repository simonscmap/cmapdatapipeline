import sys
import os
import datetime
from time import sleep

sys.path.append("ingest")
from ingest import vault_structure as vs
from ingest import metadata

tbl = 'tblSSS_NRT_cl1'
vs.leafStruc(vs.satellite + tbl)
base_folder = f'{vs.satellite}{tbl}/raw/'

## https://data.remss.com/smap/SSS/V04.0/FINAL/L3/8day_running/2022/RSS_smap_SSS_L3_8day_running_2022_001_FNL_v04.0.nc

def get_SSS(date, retry=False):
    start_index = date.strftime('%Y%m%d')  
    yr = start_dt.year
    dayn = format(start_dt, "%j")
    dayn_str = dayn.zfill(3)
    file_url = f'https://data.remss.com/smap/SSS/V05.0/FINAL/L3/8day_running/{yr}/RSS_smap_SSS_L3_8day_running_{yr}_{dayn_str}_FNL_V05.0.nc'
    save_path = f'{vs.satellite + tbl}/raw/RSS_smap_SSS_L3_8day_running_{yr}_{dayn_str}_FNL_V05.0.nc'
    wget_str = f'wget --no-check-certificate "{file_url}" -O "{save_path}"'  
    try:
        os.system(wget_str)
        Original_Name = f'RSS_smap_SSS_L3_8day_running_{yr}_{dayn_str}_FNL_V05.0.nc'
            ## Remove empty downloads
        if os.path.getsize(save_path) == 0:
            print(f'empty download for {start_index}')
            if not retry:
                metadata.tblProcess_Queue_Download_Insert(f"{start_index}", tbl, 'Opedia', 'Rainier','Download Error')
                os.remove(save_path)
        else:
            if retry:
                Error_Date = f"{start_index}"
                metadata.tblProcess_Queue_Download_Error_Update(Error_Date, Original_Name,  tbl, 'Opedia', 'Rainier')
                print(f"Successful retry for {Error_Date}")
            else:
                metadata.tblProcess_Queue_Download_Insert(Original_Name, tbl, 'Opedia', 'Rainier')
    except:
        print("No file found for date: " + start_index )
        metadata.tblProcess_Queue_Download_Insert(f"{start_index}", tbl, 'Opedia', 'Rainier','No data')

        
start_date = input('Start date (format: 2021-01-01)\n')
end_date = input('End date (format: 2021-01-01)\n')
start_dt = datetime.datetime.strptime(start_date,'%Y-%m-%d')
end_dt = datetime.datetime.strptime(end_date,'%Y-%m-%d')

delta = datetime.timedelta(days=1)

## was v04.0, now v05.0

while start_dt <= end_dt:
    get_SSS(start_dt)
    sleep(1)    
    start_dt += delta



# import datetime
# from pytz import timezone
# ## After bulk ingest to cluster, add to tblIngestion_Queue
# qry = f"SELECT Path from dbo.tblProcess_Queue WHERE Table_Name = '{tbl}' AND Processed IS NOT NULL and Error_str IS NULL"
# all_paths = DB.dbRead(qry,'Rainier')
# for pth in all_paths['Path'].to_list():
#     Path = pth.strip()
#     metadata.tblIngestion_Queue_Staged_Update(Path, tbl, 'Opedia', 'Rainier')
# sr_str = datetime.datetime.now().astimezone(timezone('US/Pacific')).strftime("%Y-%m-%d %H:%M:%S")
# qry = f"UPDATE tblIngestion_Queue SET Started = '{sr_str}', Ingested = '{sr_str}' WHERE Table_Name = '{tbl}'"
# DB.DB_modify(qry,'Rainier')




