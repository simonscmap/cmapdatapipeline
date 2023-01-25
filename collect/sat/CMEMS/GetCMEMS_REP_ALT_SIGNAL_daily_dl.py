import sys
import os
import datetime

directory = os.path.abspath('../../..')
sys.path.append(directory)

from ingest import vault_structure as vs

tbl = 'tblAltimetry_REP_Signal'
base_folder = f'{vs.satellite}{tbl}/raw/'
vs.leafStruc(vs.satellite+tbl)

output_dir = base_folder.replace(" ", "\\ ")

#ftp://my.cmems-du.eu/Core/SEALEVEL_GLO_PHY_L4_MY_008_047/cmems_obs-sl_glo_phy-ssh_my_allsat-l4-duacs-0.25deg_P1D/2021/12/dt_global_allsat_phy_l4_20211201_20220422.nc

def wget_file(output_dir, date, usr, psw):
    yr = f'{date:%Y}'
    mn = f'{date:%m}'
    dy = f'{date:%d}'
    fpath = f"ftp://my.cmems-du.eu/Core/SEALEVEL_GLO_PHY_L4_MY_008_047/cmems_obs-sl_glo_phy-ssh_my_allsat-l4-duacs-0.25deg_P1D/{yr}/{mn}/dt_global_allsat_phy_l4_{yr}{mn}{dy}_*"
    os.system(
        f"""wget --no-parent -nd -r -m --ftp-user={usr} --ftp-password={psw} {fpath}  -P  {output_dir}"""
    )

user =input('Copernicus username\n') 
pw = input('Copernicus password\n')
start_date = input('Start date (format: 2021-01-01)\n')
end_date = input('End date (format: 2021-01-01)\n')

start_dt = datetime.datetime.strptime(start_date,'%Y-%m-%d')
end_dt = datetime.datetime.strptime(end_date,'%Y-%m-%d')

while start_dt <= end_dt:
    wget_file(output_dir, start_dt, user, pw)
    start_dt += datetime.timedelta(days=1)
        



