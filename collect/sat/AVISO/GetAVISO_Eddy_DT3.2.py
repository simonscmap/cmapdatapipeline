import sys
import os
import urllib.request

sys.path.append("ingest")
from ingest import vault_structure as vs

tbl = 'tblMesoscale_Eddy_'

sat_list = ['twosat','allsat']
ln_list = ['untracked', 'short', 'long']

## Create 6 new tables in vault
for sat in sat_list:
    for ln in ln_list:
        vs.leafStruc(vs.satellite+tbl+sat+'s_'+ln)


#ftp://ftp-access.aviso.altimetry.fr/value-added/eddy-trajectory
#https://tds.aviso.altimetry.fr/thredds/catalog/dataset-duacs-rep-value-added-eddy-trajectory/META3.2_DT_twosat/catalog.html
#https://tds.aviso.altimetry.fr/thredds/fileServer/dataset-duacs-rep-value-added-eddy-trajectory/META3.2_DT_twosat/META3.2_DT_twosat_Cyclonic_untracked_19930101_20210802.nc


cyc_list = ['Cyclonic', 'Anticyclonic']

for sat in sat_list:
    for ln in ln_list:
        base_folder = f'{vs.satellite}{tbl}{sat}s_{ln}/raw/'
        print(base_folder)
        os.chdir(base_folder)
        for cyc in cyc_list:
            url_base = f"ftp://dharing@uw.edu:NBxOn4@ftp-access.aviso.altimetry.fr/value-added/eddy-trajectory/delayed-time/META3.1exp_DT_{sat}/META3.1exp_DT_{sat}_{cyc}_{ln}_19930101_20200307.nc"
            urllib.request.urlretrieve(url_base, base_folder+f'META3.1exp_DT_{sat}_{cyc}_{ln}_19930101_20200307.nc')



