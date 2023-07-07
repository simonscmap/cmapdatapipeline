import sys
import os
import time
import requests

sys.path.append("ingest")

import vault_structure as vs
import DB
import common as cm
import credentials as cr

tbl = 'tblModis_PAR'

### Change working directory to collect folder for .netrc authentication
# modis_dir = os.path.join(os.getcwd(),'collect','sat','MODIS')
# os.chdir(modis_dir)

#https://oceandata.sci.gsfc.nasa.gov/directdataaccess/Level-3%20Mapped/Aqua-MODIS/2022/067/
#https://oceandata.sci.gsfc.nasa.gov/ob/getfile/AQUA_MODIS.20220308.L3m.DAY.PAR.par.9km.nc

#echo "machine urs.earthdata.nasa.gov login dharing password Earthdata11!" > ~/.netrc ; > ~/.urs_cookies chmod  0600 ~/.netrc

base_folder = f'{vs.satellite}{tbl}/raw/'
os.chdir(base_folder)

year = 2022
month = 3
day = 7
def get_PAR(year, month, day):
    wget_str = (
        "wget --user=dharing --password=Earthdata11! --auth-no-challenge=on  https://oceandata.sci.gsfc.nasa.gov/ob/getfile/AQUA_MODIS."
        + str(year)
        + str(month).zfill(2)
        + str(day).zfill(2)
        + ".L3m.DAY.PAR.par.9km.nc"
    )
    file_name = ("AQUA_MODIS."+ str(year)
        + str(month).zfill(2)
        + str(day).zfill(2)
        + ".L3m.DAY.PAR.par.9km.nc")
    downloaded = os.path.isfile(base_folder + file_name) 
    if not downloaded:
        try:
            os.system(wget_str)
            time.sleep(3)
        except:
            print("No file found for date: " + str(year) + str(month).zfill(2)+ str(day).zfill(2))
    else:
        print("Already downloaded: " + str(year) + str(month).zfill(2)+ str(day).zfill(2))

year_list = range(2020, 2021, 1)

startDay = int(16)
endDay = int(365)
year = 2020

for year in year_list:
    for day in range(startDay, endDay + 1):
        get_PAR(year, day)

