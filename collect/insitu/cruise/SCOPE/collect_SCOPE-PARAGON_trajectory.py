import os
import sys

sys.path.append("cmapdata/ingest")

from ingest import vault_structure as vs

outputdir = vs.collected_data+'insitu/cruise/SCOPE/'
os.chdir(outputdir)
print(outputdir)
wget_str = (
    "wget -P '"
    + outputdir
    + "' -np -nH --cut-dirs 8 -r https://github.com/seaflow-uw/seaflow-sfl/blob/master/curated/SCOPE-PARAGON_740.sfl?raw=true"
)
os.system(wget_str)
os.system("mv SCOPE-PARAGON_740.sfl?raw=true paragon_seaflow.csv")  # renames folder

