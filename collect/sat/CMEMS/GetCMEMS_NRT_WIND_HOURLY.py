import sys
import os
import glob
from tqdm import tqdm

sys.path.append("ingest")

from ingest import vault_structure as vs
from ingest import DB

tbl = 'tblWind_NRT_hourly'

vs.leafStruc(vs.satellite+tbl)
base_folder = f'{vs.satellite}{tbl}/raw/'

meta_folder = f'{vs.satellite}{tbl}/metadata/'


output_dir = base_folder.replace(" ", "\\ ")

#cmems_obs-wind_glo_phy_nrt_l4_0.125deg_PT1H/
def wget_file(output_dir, yr, mnth, usr, psw):
    fpath = f"ftp://nrt.cmems-du.eu/Core/WIND_GLO_PHY_L4_NRT_012_004/cmems_obs-wind_glo_phy_nrt_l4_0.125deg_PT1H/{yr}/{mnth}/*"
    os.system(
        f"""wget --no-parent -nd -r -m --ftp-user={usr} --ftp-password={psw} {fpath}  -P  {output_dir}"""
    )

user ='mdehghaniashkez' 
pw = 'Jazireie08'
yr = '2022'
month_list = ['08','09']
mnth = '07'
for mnth in month_list:
    wget_file(output_dir, yr, mnth, user, pw)

year_list = range(1993, 2023, 1)

for year in tqdm(year_list):
    yr = str(year)
    
        
flist = glob.glob(base_folder+'*.nc')
len(flist)

## Download documentation
file_url='https://catalogue.marine.copernicus.eu/documents/QUID/CMEMS-WIND-QUID-012-004.pdf'
save_path = f'{meta_folder}CMEMS-WIND-QUID-012-004.pdf'
wget_str = f'wget --no-check-certificate "{file_url}" -O "{save_path}"'
os.system(wget_str)

file_url='https://catalogue.marine.copernicus.eu/documents/PUM/CMEMS-WIND-PUM-012-004.pdf'
save_path =f'{meta_folder}CMEMS-WIND-PUM-012-004.pdf'
wget_str = f'wget --no-check-certificate "{file_url}" -O "{save_path}"'
os.system(wget_str)


