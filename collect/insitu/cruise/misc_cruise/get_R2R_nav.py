import sys
import os
import pandas as pd
import glob
import requests
import tarfile
sys.path.append("ingest")
import vault_structure as vs

### URL for R2R produced geocsv
url = f'https://service.rvdata.us/api/product/cruise_id/{cruise_name}?product_type=r2rnav'
noaa_url = df['actual_url'][0]
dl_url = f"{noaa_url}{cruise_name}_1min.geoCSV"

### URL for raw nav data if geocsv not available
url = f'https://service.rvdata.us/api/fileset/cruise_id/{cruise_name}?device_type=GNSS'


cruise_list = ['AE0816','AE0827','AE0829','AE1119','AE1318','AE1528','AE1529','AE1704']
cruise_list = ['ar43', 'ar62', 'ar29', 'en668', 'ar34b', 'rb1904', 'en661', 'ar61b', 'ar63', 'en657', 'ar31a', 'tn368', 'ar28b', 'ar39b', 'en655', 'en644', 'at46', 'en627', 'en649']
cruise_list = [x.upper() for x in cruise_list]
## 'rb1904','ar61b', 'ar31a'
cruise_list = ['en661', 'ar61b', 'ar63', 'en657', 'ar31a', 'tn368', 'ar28b', 'ar39b', 'en655', 'en644', 'at46', 'en627', 'en649']
cruise_list = [x.upper() for x in cruise_list]

cruise_name = 'AE1704'
cruise_name = 'AE1713'
cruise_name = 'AE1726'
cruise_name = 'KM2209'
def get_r2r_nav(cruise_name):
    vs.cruise_leaf_structure(vs.r2r_cruise+cruise_name)
    cruise_raw = f"{vs.r2r_cruise}{cruise_name}/raw/"
    try:
        url = f'https://service.rvdata.us/api/product/cruise_id/{cruise_name}?product_type=r2rnav'
        res = requests.get(url)
        data = res.json()['data']
        df = pd.DataFrame.from_dict(data)
        noaa_url = df['actual_url'][0]
        if '.tar.gz' in noaa_url:
            dl_url = df['url'][0]
            print(f"tar download for {cruise_name}")
            tar_unzip=True
        else: 
            dl_url = f"{noaa_url}/data/{cruise_name}_1min.geoCSV"
            tar_unzip=False
        wget_str = f'wget --no-check-certificate "{dl_url}" -P "{cruise_raw}" -np -nH' 
        os.system(wget_str)
        if tar_unzip:
            flist = glob.glob(cruise_raw+'*')
            os.chdir(cruise_raw)
            with tarfile.open(flist[0], "r:gz") as gzip_file:
                gzip_file.extractall()
    except:
        url = f'https://service.rvdata.us/api/fileset/cruise_id/{cruise_name}?device_type=GNSS'
        res = requests.get(url)
        data = res.json()['data']
        df = pd.DataFrame.from_dict(data)
        dl_url = df['download_url'][0]
        fileset_id = df['fileset_id'][0]
        # vs.cruise_leaf_structure(vs.r2r_cruise+cruise_name)
        wget_str = f'wget --no-check-certificate "{dl_url}" -P "{cruise_raw}" -np -nH'  
        os.system(wget_str)
        os.chdir(cruise_raw)
        os.system(f"tar -zxvf {fileset_id}")
        os.system(f"rm {fileset_id}")
        os.system(f"mv {cruise_name}/{fileset_id}/data/* '{cruise_raw}'")
        # flist = glob.glob(f"{cruise_raw}*.Raw")
        # df_raw = pd.read_csv(flist[0])


## link from download data button on ADCP data
## Need to find redirect links in API
## https://www.rvdata.us/search/cruise/AE1726    
# url = f'https://service.rvdata.us/api/fileset/cruise_id/{cruise_name}?device_type=ADCP'
# res = requests.get(url)
# data = res.json()['data']
# df = pd.DataFrame.from_dict(data)
# fileset_id = df['fileset_id'][0]    
def get_adcp_nav(url, cruise_name):
    cruise_raw = f"{vs.r2r_cruise}{cruise_name}/raw/"
    wget_str = f'wget --no-check-certificate "{url}" -O "{cruise_raw}traj.zip" -np -nH' 
    os.system(wget_str)
    flist = glob.glob(cruise_raw+'*.zip')
    os.chdir(cruise_raw)
    with tarfile.open(flist[0], "r:gz") as gzip_file:
        gzip_file.extractall()


cruise_name = 'AE1820'
get_r2r_nav(cruise_name)


url = 'https://service.rvdata.us/data/cruise/EN629/fileset/609235'
get_adcp_nav(url, cruise_name)


## geonav url is different
url = 'https://service.rvdata.us/data/cruise/AE0813/fileset/602547'
## which redirects you to 
dl_url = 'https://www.nodc.noaa.gov/archive/arc0127/0178281/1.1/data/0-data/AE0813_602547_r2rnav/data/AE0813_1min.geoCSV'
## accession id: 0178281

cruise_name = 'AE0816'
dl_url ='https://www.nodc.noaa.gov/archive/arc0127/0178289/1.1/data/0-data/AE0816_602548_r2rnav/data/AE0816_1min.geoCSV'

cruise_name = 'AE0829'
dl_url ='https://www.nodc.noaa.gov/archive/arc0127/0178293/1.1/data/0-data/AE0829_602552_r2rnav/data/AE0829_1min.geoCSV'

cruise_name = 'AE1119'
dl_url ='https://www.nodc.noaa.gov/archive/arc0127/0178849/1.1/data/0-data/AE1119_602613_r2rnav/data/AE1119_1min.geoCSV'

cruise_name = 'AE1528'
dl_url ='https://www.nodc.noaa.gov/archive/arc0127/0178383/1.1/data/0-data/AE1528_602741_r2rnav/data/AE1528_1min.geoCSV'

cruise_raw = f"{vs.r2r_cruise}{cruise_name}/raw/"
wget_str = f'wget --no-check-certificate "{dl_url}" -P "{cruise_raw}" -np -nH'  
os.system(wget_str)