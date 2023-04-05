
import os
import sys

sys.path.append("ingest")
from ingest import vault_structure as vs
from ingest import credentials as cr


tbl = 'tblMGL1704_Gradients2'   
vs.leafStruc(vs.cruise + tbl)
base_folder = f'{vs.cruise}{tbl}/raw/'
os.chdir(base_folder)

wget_str = (
        f"wget -nH --cut-dirs 5 -r --no-parent http://scope.soest.hawaii.edu/FTP/scope/water/gradients2.gof"
    )
os.system(wget_str)

wget_str = (
        "wget -nH --cut-dirs 5 -r --no-parent http://scope.soest.hawaii.edu/data/gradients/data/mgl1704.sum"
    )
os.system(wget_str)
