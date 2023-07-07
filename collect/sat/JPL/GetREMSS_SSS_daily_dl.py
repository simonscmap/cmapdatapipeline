import sys
import os
import datetime
from time import sleep

sys.path.append("ingest")
from ingest import vault_structure as vs

tbl = 'tblSSS_NRT'
vs.leafStruc(vs.satellite + tbl)

## https://data.remss.com/smap/SSS/V04.0/FINAL/L3/8day_running/2022/RSS_smap_SSS_L3_8day_running_2022_001_FNL_v04.0.nc


start_date = input('Start date (format: 2021-01-01)\n')
end_date = input('End date (format: 2021-01-01)\n')
start_dt = datetime.datetime.strptime(start_date,'%Y-%m-%d')
end_dt = datetime.datetime.strptime(end_date,'%Y-%m-%d')

delta = datetime.timedelta(days=1)

## was v04.0, now v05.0
ver = 'V05.0'
while start_dt <= end_dt:
    yr = start_dt.year
    dayn = format(start_dt, "%j")
    dayn_str = dayn.zfill(3)
    file_url = f'https://data.remss.com/smap/SSS/{ver}/FINAL/L3/8day_running/{yr}/RSS_smap_SSS_L3_8day_running_{yr}_{dayn_str}_FNL_{ver.lower()}.nc'
    save_path = f'{vs.satellite + tbl}/raw/RSS_smap_SSS_L3_8day_running_{yr}_{dayn_str}_FNL_{ver.lower()}.nc'
    wget_str = f'wget --no-check-certificate "{file_url}" -O "{save_path}"'
    os.system(wget_str)
    ## Remove empty downloads
    if os.path.getsize(save_path) == 0:
        print(f'empty download for {start_dt}')
        os.remove(save_path)
    sleep(3)    
    start_dt += delta








