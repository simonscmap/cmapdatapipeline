import os
import sys

sys.path.append("cmapdata/ingest")
from ingest import vault_structure as vs

tbl = 'tblSR1917_TSG'   
vs.leafStruc(vs.cruise + tbl)
base_folder = f'{vs.cruise}{tbl}/raw/'
os.chdir(base_folder)

"""grab TSG for traj - https://www.rvdata.us/search/cruise/sr1917 https://service.rvdata.us/data/cruise/SR1917/fileset/134740"""


wget_str = 'wget -P  -np -nH --cut-dirs 8 -r  https://service.rvdata.us/data/cruise/SR1917/fileset/609220'
os.system(wget_str)
