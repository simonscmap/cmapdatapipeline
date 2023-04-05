import sys
import os
from tqdm import tqdm

sys.path.append("ingest")

from ingest import vault_structure as vs

tbl = 'tblAltimetry_REP_Signal'

base_folder = f'{vs.satellite}{tbl}/raw/'

vs.leafStruc(vs.satellite+tbl)

output_dir = base_folder.replace(" ", "\\ ")


def wget_file(output_dir, yr, usr, psw):
    fpath = f"ftp://my.cmems-du.eu/Core/SEALEVEL_GLO_PHY_L4_MY_008_047/cmems_obs-sl_glo_phy-ssh_my_allsat-l4-duacs-0.25deg_P1D/{yr}/*"
    os.system(
        f"""wget --no-parent -nd -r -m --ftp-user={usr} --ftp-password={psw} {fpath}  -P  {output_dir}"""
    )

user ='' 
pw = ''

year_list = range(1993, 2023, 1)

for year in tqdm(year_list):
    yr = str(year)
    wget_file(output_dir, yr, user, pw)
        



