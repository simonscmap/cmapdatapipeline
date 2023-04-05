import sys
import os
import time
import datetime as dt

sys.path.append("ingest")
from ingest import vault_structure as vs
from ingest import credentials as cr
from ingest import common as cm
from ingest import data_checks as dc

#https://oceandata.sci.gsfc.nasa.gov/directaccess/MODIS-Aqua/Mapped/8-Day/9km/chlor_a/

tbl = 'tblModis_CHL'

vs.leafStruc(vs.satellite+tbl)
base_folder = f'{vs.satellite}{tbl}/raw/'


os.chdir(base_folder)

def get_CHL(year, day,base_folder):
    # start_index = day / 8
    # if day % 8 == 0:
    #     start_index = day - 1
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
    + ".L3m_8D_CHL_chlor_a_9km.nc"
    )
    print(start_index)
    print(end_index)
    file_name = ("A"+ str(year)
        + str(start_index).zfill(3)
        + str(year)
        + str(end_index).zfill(3)
        + ".L3m_8D_CHL_chlor_a_9km.nc")
    downloaded = os.path.isfile(base_folder + file_name) 
    if not downloaded:
        try:
            os.system(wget_str)
            time.sleep(5)
        except:
            print("No file found for date: " + str(year) + str(start_index).zfill(3))
    else:
        print("Already downloaded: " + str(year) + str(start_index).zfill(3))




year_list = range(2008, 2014, 1)
startDay = int(1)
endDay = int(365)

year = 2022

last_import = 72
startDay = last_import + 1

endDay = startDay + 30

date_now = dt.date.today()
jld_str = date_now.strftime('%j')
jld_int = int(jld_str)
endDay = jld_int


for year in year_list:
    ## Step through list by 8 to save calls
    for day in range(startDay, endDay + 1, 8):
        get_CHL(year, day,base_folder)  
        
## Old file format available through 2022161 - 2022168 (June 10 - 17)
## New naming convention: https://oceandata.sci.gsfc.nasa.gov/cgi/getfile/AQUA_MODIS.20220720_20220727.L3m.8D.CHL.chlor_a.9km.NRT.nc

start_date = dt.date(2022, 6, 10)
end_date = dt.date.today()
delta = dt.timedelta(days=8)

def get_CHL(start_date,base_folder):
    start_index = start_date.strftime('%Y%m%d')
    date2 = start_date + dt.timedelta(days=7)
    end_index = date2.strftime('%Y%m%d')

    wget_str = (
    "wget --load-cookies ~/.urs_cookies --save-cookies ~/.urs_cookies --auth-no-challenge=on --keep-session-cookies --content-disposition https://oceandata.sci.gsfc.nasa.gov/cgi/getfile/AQUA_MODIS."
    + start_index +'_'
    + end_index
    + ".L3m.8D.CHL.chlor_a.9km.NRT.nc"
    )
    print(start_index)
    print(end_index)
    file_name = ("AQUA_MODIS."
    + start_index +'_'
    + end_index
    + ".L3m.8D.CHL.chlor_a.9km.NRT.nc")
    downloaded = os.path.isfile(base_folder + file_name) 
    if not downloaded:
        try:
            os.system(wget_str)
            time.sleep(5)
        except:
            print("No file found for date: " + start_index + "_" + end_index)
    else:
        print("Already downloaded: " +  start_index + "_" + end_index)

while start_date <= end_date:
    get_CHL(start_date,base_folder)
    start_date = start_date + delta