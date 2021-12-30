import sys
import os
import zipfile
import shutil
from tqdm import tqdm 

sys.path.append("../../../ingest")
from ingest import vault_structure as vs

tbl_list = ['tblGeotraces_Seawater','tblGeotraces_Sensor','tblGeotraces_Aerosols','tblGeotraces_Precipitation','tblGeotraces_Cryosphere']

for tbl in tbl_list:
    vs.leafStruc(vs.cruise + tbl)

## Download unavailable Dec 18 - Jan 7 2022
dl_url = 'https://www.bodc.ac.uk/data/published_data_library/catalogue/10.5285/cf2d9ba9-d51d-3b7c-e053-8486abc0f5fd/'

save_path = f'{vs.cruise + tbl}/raw/all_geotraces'
wget_str = f'wget --no-check-certificate "{dl_url}" -O "{save_path}"'
os.system(wget_str)

## Intermediate solution until download link works again
## scp C:\Users\norla\Downloads\all_geotraces.zip exx@128.208.239.117:"'/data/CMAP Data Submission Dropbox/Simons CMAP/vault/observation/in-situ/cruise/tblGeotraces_Seawater/raw/'

raw_dir = f'{vs.cruise}tblGeotraces_Seawater/raw'
os.chdir(raw_dir) #change working dir to location with zipped files
os.listdir(raw_dir)
## Unzip first directory
for item in os.listdir(raw_dir):
    if zipfile.is_zipfile(item):
        with zipfile.ZipFile(item) as file: # treat the file as a zip
           file.extractall(raw_dir)

## Unzip subdirectories
zip_dir = raw_dir +'/idp2021'
os.chdir(zip_dir)
os.listdir(zip_dir)
#item = 'GEOTRACES_IDP2021_v1_aerosols_netcdf.zip'
for item in os.listdir(zip_dir):
    # if zipfile.is_zipfile(item) and '_netcdf' in item: is_zipfile says false for zip files
    if '_netcdf.zip' in item:        
        filename = os.path.basename(item)
        if 'seawater' in item:
            dest = vs.cruise + 'tblGeotraces_Seawater/raw' 
        elif 'precip' in item:
            dest = vs.cruise + 'tblGeotraces_Precipitation/raw'
        elif 'sensor' in item:
            dest = vs.cruise + 'tblGeotraces_Sensor/raw'
        elif 'aero' in item:
            dest = vs.cruise + 'tblGeotraces_Aerosols/raw'
        elif 'cryo' in item:
            dest = vs.cruise + 'tblGeotraces_Cryosphere/raw'
        with zipfile.ZipFile(item) as file: 
            ## adds new folders to GEOTRACES_IDP2021_v1 extracted folder 
            file.extractall(dest)


sub_list = ['/aerosols/netcdf','/cryosphere/netcdf','/precipitation/netcdf','/seawater/netcdf','/sensor/netcdf']
for sub in sub_list:
    if 'seawater' in sub:
        meta = vs.cruise + 'tblGeotraces_Seawater/metadata/' 
        dest = vs.cruise + 'tblGeotraces_Seawater/raw' 
    elif 'precip' in sub:
        meta = vs.cruise + 'tblGeotraces_Precipitation/metadata/'
        dest = vs.cruise + 'tblGeotraces_Precipitation/raw'
    elif 'sensor' in sub:
        meta = vs.cruise + 'tblGeotraces_Sensor/metadata/'
        dest = vs.cruise + 'tblGeotraces_Sensor/raw'
    elif 'aero' in sub:
        meta = vs.cruise + 'tblGeotraces_Aerosols/metadata/'
        dest = vs.cruise + 'tblGeotraces_Aerosols/raw'
    elif 'cryo' in sub:
        meta = vs.cruise + 'tblGeotraces_Cryosphere/metadata/'
        dest = vs.cruise + 'tblGeotraces_Cryosphere/raw'
    
    os.chdir(dest+'/GEOTRACES_IDP2021_v1'+sub)
    for item in os.listdir(dest+'/GEOTRACES_IDP2021_v1'+sub):
        if '.zip' in item:
            print(item)
            with zipfile.ZipFile(item) as file:
                file.extractall(dest)
        else:
            shutil.move(os.path.abspath(item), meta + item)
    os.chdir(dest+'/GEOTRACES_IDP2021_v1')
    for pdf in os.listdir(dest+'/GEOTRACES_IDP2021_v1'):
        if '.pdf' in pdf:
            shutil.move(os.path.abspath(pdf), meta + pdf)





                   


            

