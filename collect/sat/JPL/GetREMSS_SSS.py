import sys
import os
from tqdm import tqdm 
import datetime
import xarray as xr
from time import sleep

sys.path.append("ingest")
from ingest import vault_structure as vs

tbl = 'tblSSS_NRT'
vs.leafStruc(vs.satellite + tbl)

## https://data.remss.com/smap/SSS/V04.0/FINAL/L3/8day_running/2022/RSS_smap_SSS_L3_8day_running_2022_001_FNL_v04.0.nc
## https://podaac-opendap.jpl.nasa.gov/opendap/hyrax/SalinityDensity/smap/L3/RSS/V4/8day_running/SCI/


start_date = datetime.date(2019, 4, 23)
end_date = datetime.date(2022, 5, 1)
delta = datetime.timedelta(days=1)

while start_date <= end_date:
    yr = start_date.year
    dayn = format(start_date, "%j")
    dayn_str = dayn.zfill(3)

    file_url = f'https://data.remss.com/smap/SSS/V04.0/FINAL/L3/8day_running/{yr}/RSS_smap_SSS_L3_8day_running_{yr}_{dayn_str}_FNL_v04.0.nc'
    save_path = f'{vs.satellite + tbl}/raw/RSS_smap_SSS_L3_8day_running_{yr}_{dayn_str}_FNL_v04.0.nc'
    wget_str = f'wget --no-check-certificate "{file_url}" -O "{save_path}"'
    os.system(wget_str)
    ## Remove empty downloads
    if os.path.getsize(save_path) == 0:
        print(f'empty download for {start_date}')
        os.remove(save_path)
    sleep(5)    
    start_date += delta

## Download documentation
file_url='https://data.remss.com/smap/SSS/Release_V4.0.pdf'
txt_url='https://atmosphere-imager.gsfc.nasa.gov/sites/default/files/ModAtmo/MOD08_M3_fs_3044.txt'
save_path = f'{vs.satellite + tbl}/doc/Release_V4.0.pdf'
save_path = f'{vs.satellite + tbl}/doc/MOD08_M3_fs_3044.txt'
wget_str = f'wget --no-check-certificate "{txt_url}" -O "{save_path}"'
os.system(wget_str)







