import sys
import os
import glob

from cmapingest import vault_structure as vs
from cmapingest import DB
from cmapingest import common as cm


tbl = 'tblModis_PAR'

base_folder = f'{vs.satellite}{tbl}/raw/'
os.chdir(base_folder)
max_sql_date = cm.getMax_SQL_date(tbl, 'Rainier')
last_file_dl = cm.getLast_file_download(tbl, 'satellite')

wget_search = 'wget -q --post-data="sensor=aqua&sdate=2022-01-01&dtype=L3b&addurl=1&results_as_file=0&search=*L3m_DAY_PAR_par_9km*" -O - https://oceandata.sci.gsfc.nasa.gov/api/file_search'
## https://oceandata.sci.gsfc.nasa.gov/directaccess/MODIS-Aqua/Mapped/Daily/9km/par/

def get_PAR(year, day):
    wget_str = (
        "wget --load-cookies ~/.urs_cookies --save-cookies ~/.urs_cookies --auth-no-challenge=on --keep-session-cookies --content-disposition https://oceandata.sci.gsfc.nasa.gov/cgi/getfile/A"
        + str(year)
        + str(day).zfill(3)
        + ".L3m_DAY_PAR_par_9km.nc"
    )
    try:
        os.system(wget_str)

    except:
        print("No file found for date: " + str(year) + str(day).zfill(3))


year_list = range(2020, 2021, 1)

startDay = int(28)
endDay = int(365)
year = 2020

for year in year_list:
    for day in range(startDay, endDay + 1):
        get_PAR(year, day)

