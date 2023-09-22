import sys
import os
import zipfile
import shutil
from tqdm import tqdm 

sys.path.append("../../../ingest")
from ingest import vault_structure as vs

tbl_list = ['tblGeotraces_Seawater_IDP2021v2','tblGeotraces_Sensor_IDP2021v2','tblGeotraces_Aerosols_IDP2021v2','tblGeotraces_Precipitation_IDP2021v2','tblGeotraces_Cryosphere_IDP2021v2']

for tbl in tbl_list:
    vs.leafStruc(vs.cruise + tbl)

### https://www.bodc.ac.uk/data/published_data_library/catalogue/10.5285/ff46f034-f47c-05f9-e053-6c86abc0dc7e/
dl_url = 'https://www.bodc.ac.uk/data/published_data_library/catalogue/10.5285/123/RN-20230707081033_FF46F034F47C05F9E0536C86ABC0DC7E.zip'
tbl = 'tblGeotraces_Seawater_IDP2021v2'
save_path = f'{vs.cruise + tbl}/raw/all_geotraces'
wget_str = f'wget --no-check-certificate "{dl_url}" -O "{save_path}"'
os.system(wget_str)


raw_dir = f'{vs.cruise}{tbl}/raw'
os.chdir(raw_dir) #change working dir to location with zipped files
os.listdir(raw_dir)
## Unzip first directory
for item in os.listdir(raw_dir):
    if zipfile.is_zipfile(item):
        with zipfile.ZipFile(item) as file: # treat the file as a zip
           file.extractall(raw_dir)

## Unzip subdirectories
zip_dir = raw_dir +'/FF46F034F47C05F9E0536C86ABC0DC7E/ipd2021v2'
os.chdir(zip_dir)
os.listdir(zip_dir)
#item = 'GEOTRACES_IDP2021_v1_aerosols_netcdf.zip'
for item in os.listdir(zip_dir):
    # if zipfile.is_zipfile(item) and '_netcdf' in item: is_zipfile says false for zip files
    if '_netcdf.zip' in item:        
        filename = os.path.basename(item)
        if 'seawater' in item:
            dest = vs.cruise + 'tblGeotraces_Seawater_IDP2021v2/raw' 
        elif 'precip' in item:
            dest = vs.cruise + 'tblGeotraces_Precipitation_IDP2021v2/raw'
        elif 'sensor' in item:
            dest = vs.cruise + 'tblGeotraces_Sensor_IDP2021v2/raw'
        elif 'aero' in item:
            dest = vs.cruise + 'tblGeotraces_Aerosols_IDP2021v2/raw'
        elif 'cryo' in item:
            dest = vs.cruise + 'tblGeotraces_Cryosphere_IDP2021v2/raw'
        with zipfile.ZipFile(item) as file: 
            ## adds new folders to GEOTRACES_IDP2021_v1 extracted folder 
            file.extractall(dest)


# sub_list = ['/aerosols/netcdf','/cryosphere/netcdf','/precipitation/netcdf','/seawater/netcdf','/sensor/netcdf']
### Only Seawater and Aerosols remade with v2 versions
sub_list = ['/aerosols/netcdf','/seawater/netcdf']
for sub in sub_list:
    if 'seawater' in sub:
        meta = vs.cruise + 'tblGeotraces_Seawater_IDP2021v2/metadata/' 
        dest = vs.cruise + 'tblGeotraces_Seawater_IDP2021v2/raw' 
    elif 'precip' in sub:
        meta = vs.cruise + 'tblGeotraces_Precipitation_IDP2021v2/metadata/'
        dest = vs.cruise + 'tblGeotraces_Precipitation_IDP2021v2/raw'
    elif 'sensor' in sub:
        meta = vs.cruise + 'tblGeotraces_Sensor_IDP2021v2/metadata/'
        dest = vs.cruise + 'tblGeotraces_Sensor_IDP2021v2/raw'
    elif 'aero' in sub:
        meta = vs.cruise + 'tblGeotraces_Aerosols_IDP2021v2/metadata/'
        dest = vs.cruise + 'tblGeotraces_Aerosols_IDP2021v2/raw'
    elif 'cryo' in sub:
        meta = vs.cruise + 'tblGeotraces_Cryosphere_IDP2021v2/metadata/'
        dest = vs.cruise + 'tblGeotraces_Cryosphere_IDP2021v2/raw'
    
    os.chdir(dest+'/GEOTRACES_IDP2021_v2'+sub)
    for item in os.listdir(dest+'/GEOTRACES_IDP2021_v2'+sub):
        if '.zip' in item:
            print(item)
            with zipfile.ZipFile(item) as file:
                file.extractall(dest)
        else:
            shutil.move(os.path.abspath(item), meta + item)
    os.chdir(dest+'/GEOTRACES_IDP2021_v2')
    for pdf in os.listdir(dest+'/GEOTRACES_IDP2021_v2'):
        if '.pdf' in pdf:
            shutil.move(os.path.abspath(pdf), meta + pdf)





                   


            

