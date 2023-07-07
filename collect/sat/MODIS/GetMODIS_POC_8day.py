import sys
import os

sys.path.append("ingest")
from ingest import vault_structure as vs
from ingest import credentials as cr
from ingest import common as cm
from ingest import data_checks as dc

#https://oceandata.sci.gsfc.nasa.gov/directaccess/MODIS-Aqua/Mapped/8-Day/9km/poc/

## New URL. Only NRT version available after 2022 10 23
## https://oceandata.sci.gsfc.nasa.gov/cgi/getfile/AQUA_MODIS.20221016_20221023.L3b.8D.POC.nc 
## https://oceandata.sci.gsfc.nasa.gov/directdataaccess/Level-3%20Binned/Aqua-MODIS/2022/001/AQUA_MODIS.20220101_20220108.L3b.8D.POC.nc  

tbl = 'tblModis_POC'

base_folder = f'{vs.satellite}{tbl}/raw/'

max_sql_date = cm.getMax_SQL_date(tbl, 'Rainier')
doy = max_sql_date.timetuple().tm_yday
last_file_dl = cm.getLast_file_download(tbl, 'satellite')
os.chdir(base_folder)

def get_POC(year, day,base_folder):
    # start_index = day / 8
    if day % 8 == 0:
        start_index = day - 1
        print("########## day minus 1")
    # start_index = (start_index * 8)  + 1
    start_index = day
    end_index = start_index + 7
    if start_index == 361:
        end_index = 365
    if (year % 4 == 0) and (start_index == 361):
        end_index = 366 
    # start_index = int(start_index) - 1
    # end_index = int(end_index) - 1
    wget_str = (
    "wget --load-cookies ~/.urs_cookies --save-cookies ~/.urs_cookies --auth-no-challenge=on --keep-session-cookies --content-disposition https://oceandata.sci.gsfc.nasa.gov/cgi/getfile/A"
    + str(year)
    + str(start_index).zfill(3)
    + str(year)
    + str(end_index).zfill(3)
    + ".L3m_8D_POC_poc_9km.nc"
    )
    print(start_index)
    print(end_index)
    file_name = ("A"+ str(year)
        + str(start_index).zfill(3)
        + str(year)
        + str(end_index).zfill(3)
        + ".L3m_8D_POC_poc_9km.nc")
    downloaded = os.path.isfile(base_folder + file_name) 
    if not downloaded:
        try:
            os.system(wget_str)
        except:
            print("No file found for date: " + str(year) + str(start_index).zfill(3))
    else:
        print("Already downloaded: " + str(year) + str(start_index).zfill(3))



year_list = range(2020, 2021, 1)
startDay = int(1)
endDay = int(24)

year = 2006
endDay = int(16)

for year in year_list:
    ## Step through list by 8 to save calls
    for day in range(startDay, endDay + 1, 8):
        get_POC(year, day,base_folder)  
