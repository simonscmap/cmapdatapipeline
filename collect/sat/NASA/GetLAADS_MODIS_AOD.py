
import sys
import os

sys.path.append("ingest")

import credentials as cr
import vault_structure as vs


tbl = 'tblModis_AOD_REP'

vs.leafStruc(vs.satellite + tbl)

base_folder = f'{vs.satellite}/{tbl}/raw/'
yr_list = ['2020','2021','2022']

## wget downloaded all of /MYD08_M3, not just 2019
yr='2022'
for yr in yr_list:
    wget_str = f'wget -e robots=off -m -np -R .html,.tmp -nH --cut-dirs=3 "https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/61/MYD08_M3/{yr}/" --header "Authorization: Bearer {cr.nasa_earthdata_token}" -P .'


    os.chdir(base_folder)
    try:
        os.system(wget_str)

    except:
        print("No file found for date: " + yr)

## Download documentation
txt_url='https://atmosphere-imager.gsfc.nasa.gov/sites/default/files/ModAtmo/MOD08_M3_fs_3044.txt'
save_path = f'{vs.satellite + tbl}/doc/MOD08_M3_fs_3044.txt'
wget_str = f'wget --no-check-certificate "{txt_url}" -O "{save_path}"'
os.system(wget_str)

