
import os
import sys
import tqdm
sys.path.append("ingest")

import vault_structure as vs
import credentials as cr

"""

FINISHED --2021-06-16 16:45:22--
Total wall clock time: 5h 33m 30s
Downloaded: 5089 files, 51G in 4h 47m 28s (3.01 MB/s)
"""

tbl = 'tblWind_NRT'
vs.leafStruc(vs.satellite+tbl)
base_folder = f'{vs.satellite}{tbl}/raw/'
output_dir = base_folder.replace(" ", "\\ ")



def wget_file(output_dir, yr, usr, psw):
    fpath = f"ftp://nrt.cmems-du.eu/Core/WIND_GLO_WIND_L4_NRT_OBSERVATIONS_012_004/CERSAT-GLO-BLENDED_WIND_L4-V6-OBS_FULL_TIME_SERIE/{yr}/*"
    os.system(
        f"""wget --no-parent -nd -r -m --ftp-user={usr} --ftp-password={psw} {fpath}  -P  {output_dir}"""
    )
year_list = range(2021, 2022, 1)

usr_cmem = cr.usr_cmem
psw_cmem = cr.psw_cmem
for year in tqdm(year_list):
    yr = str(2021)
    wget_file(output_dir, yr, usr_cmem, psw_cmem)


output_dir = (vs.collected_data + "sat/CMEMS_NRT_Wind/").replace(" ", "\\ ")
ftp_link = "ftp://nrt.cmems-du.eu/Core/WIND_GLO_WIND_L4_NRT_OBSERVATIONS_012_004/CERSAT-GLO-BLENDED_WIND_L4-V6-OBS_FULL_TIME_SERIE/*"


def wget_file(fpath, output_dir, usr, psw):
    os.system(
        f"""wget --no-parent -nd -r -m --ftp-user={usr} --ftp-password={psw} {fpath}  -P  {output_dir}"""
    )



