import sys
import os
import time
import datetime as dt

sys.path.append("ingest")
from ingest import vault_structure as vs
from ingest import credentials as cr
from ingest import metadata
from ingest import data_checks as dc


#https://oceandata.sci.gsfc.nasa.gov/cgi/getfile/AQUA_MODIS.20220226_20220305.L3m.8D.CHL.chlor_a.9km.NRT.nc

tbl = 'tblModis_CHL_NRT'

# vs.leafStruc(vs.satellite+tbl)
base_folder = f'{vs.satellite}{tbl}/raw/'

start_date = dt.date(2023, 1, 25) #20230125
end_date = dt.date.today()
end_date = dt.date(2023, 4, 7) #20230407
delta = dt.timedelta(days=8)

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
    file_url = f"https://oceandata.sci.gsfc.nasa.gov/cgi/getfile/AQUA_MODIS.{start_index}_{end_index}.L3m.8D.CHL.chlor_a.9km.NRT.nc"
    Original_Name = f"AQUA_MODIS.{start_index}_{end_index}.L3m.8D.CHL.chlor_a.9km.NRT.nc"
    save_path = base_folder+Original_Name
    wget_str = f'wget --user={cr.usr_earthdata} --password={cr.psw_earthdata} --auth-no-challenge=on "{file_url}" -O "{save_path}"'
    downloaded = os.path.isfile(base_folder + Original_Name) 
    if not downloaded:
        try:
            os.system(wget_str)
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
            time.sleep(2)
        except:
            print("No file found for date: " + start_index + "_" + end_index)
            metadata.tblProcess_Queue_Download_Insert(f"{start_index}", tbl, 'Opedia', 'Rainier','No data')
    else:
        print("Already downloaded: " +  start_index + "_" + end_index)

while start_date <= end_date:
    get_CHL(start_date)
    start_date = start_date + delta
    if start_date.month == 1 and start_date.day <=6:
        start_date = dt.date(start_date.year, 1, 1)

metadata.export_script_to_vault(tbl,'satellite','collect/sat/MODIS/GetMODIS_CHL_8day_NRT.py','collect.txt')  

import datetime
from pytz import timezone
## After bulk ingest to cluster, add to tblIngestion_Queue
qry = f"SELECT Path from dbo.tblProcess_Queue WHERE Table_Name = '{tbl}' AND Processed IS NOT NULL and Error_str IS NULL"
all_paths = DB.dbRead(qry,'Rainier')
for pth in all_paths['Path'].to_list():
    Path = pth.strip()
    metadata.tblIngestion_Queue_Staged_Update(Path, tbl, 'Opedia', 'Rainier')
sr_str = datetime.datetime.now().astimezone(timezone('US/Pacific')).strftime("%Y-%m-%d %H:%M:%S")
qry = f"UPDATE tblIngestion_Queue SET Started = '{sr_str}', Ingested = '{sr_str}' WHERE Table_Name = '{tbl}'"
DB.DB_modify(qry,'Rainier')