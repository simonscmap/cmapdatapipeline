import os
import sys
sys.path.append("ingest")
import vault_structure as vs

tbl = 'tblHOT_ParticleFlux'
base_folder = f'{vs.cruise}{tbl}/raw/'
hot_ftp = 'particle_flux'
# os.chdir(base_folder)

hot_ftp_link = f"https://hahana.soest.hawaii.edu/FTP/hot/{hot_ftp}/"
## Need to quote base fold due to spaces in vault directory
wget_str = f'wget -P "{base_folder}" --no-parent -nd -r {hot_ftp_link}'
os.system(wget_str)

tbl = 'tblHOT_PP'
hot_pp_ftp_link = "https://hahana.soest.hawaii.edu/FTP/hot/primary_production/"
## Need to quote base fold due to spaces in vault directory
wget_str = f'wget -P "{base_folder}" --no-parent -nd -r {hot_pp_ftp_link}'
os.system(wget_str)