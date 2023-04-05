import sys
import os
import time
import urllib.request

sys.path.append("ingest")
from ingest import vault_structure as vs
from ingest import credentials as cr
from ingest import common as cm
from ingest import data_checks as dc


tbl = 'tblMesoscale_Eddy_twosat'

vs.leafStruc(vs.satellite+tbl)
base_folder = f'{vs.satellite}{tbl}/raw/'

tbl = 'tblMesoscale_Eddy_allsat'

vs.leafStruc(vs.satellite+tbl)
base_folder = f'{vs.satellite}{tbl}/raw/'

os.chdir(base_folder)
#ftp://ftp-access.aviso.altimetry.fr/value-added/eddy-trajectory
#https://tds.aviso.altimetry.fr/thredds/catalog/dataset-duacs-rep-value-added-eddy-trajectory/META3.2_DT_twosat/catalog.html
#https://tds.aviso.altimetry.fr/thredds/fileServer/dataset-duacs-rep-value-added-eddy-trajectory/META3.2_DT_twosat/META3.2_DT_twosat_Cyclonic_untracked_19930101_20210802.nc

sat = 'twosat'

cyc_list = ['Cyclonic', 'Anticyclonic']
ln_list = ['untracked', 'short', 'long']
cyc = 'Anticyclonic'
ln='long'

for cyc in cyc_list:
    for ln in ln_list:
        url_base = f"ftp://dharing@uw.edu:NBxOn4@ftp-access.aviso.altimetry.fr/value-added/eddy-trajectory/delayed-time/META3.1exp_DT_{sat}/META3.1exp_DT_{sat}_{cyc}_{ln}_19930101_20200307.nc"

        urllib.request.urlretrieve(url_base, base_folder+f'META3.1exp_DT_{sat}_{cyc}_{ln}_19930101_20200307.nc')



