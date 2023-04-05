
import os
import sys

sys.path.append("ingest")
from ingest import vault_structure as vs
from ingest import credentials as cr

tbl = 'tblKM1906_Gradients3_CTD'        
vs.leafStruc(vs.cruise + tbl)
base_folder = f'{vs.cruise}{tbl}/raw/'
os.chdir(base_folder)

wget_str = (
        "wget -nH --cut-dirs 5 -r --no-parent http://scope.soest.hawaii.edu/FTP/scope/ctd/gradients3/"
    )
os.system(wget_str)

wget_str = (
        "wget -nH --cut-dirs 5 -r --no-parent http://scope.soest.hawaii.edu/data/gradients/data/gradients3.sum"
    )
os.system(wget_str)

