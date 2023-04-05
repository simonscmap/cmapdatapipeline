import os
import sys
from zipfile import ZipFile
import shutil
import gzip

sys.path.append("ingest")
from ingest import vault_structure as vs
from ingest import credentials as cr

tbl = 'tblTN397_Gradients4_uw_tsg'

vs.leafStruc(vs.cruise + tbl)
base_folder = f'{vs.cruise}{tbl}/raw/'


tsg_link = "https://gradientscruise.org/datafiles/"
wget_str = f"wget -e robots=off -r --no-parent --user={cr.usr_g4} --password={cr.psw_g4} {tsg_link}"

os.chdir(base_folder)
os.system(wget_str)

with ZipFile(f"{base_folder}gradientscruise.org/datafiles/TN396.zip","r") as zip_ref:
    zip_ref.extractall(base_folder)
with ZipFile(f"{base_folder}gradientscruise.org/datafiles/TN397.zip","r") as zip_ref:
    zip_ref.extractall(base_folder)

flist = ['TN396.zip.file-listing.txt','TN397.zip.file-listing.txt']
for f in flist:
    shutil.move(f"{base_folder}gradientscruise.org/datafiles/{f}", f"{base_folder}{f}")    

shutil.rmtree(f"{base_folder}gradientscruise.org")

## Additional files from Chris
with gzip.open(f"{base_folder}TN397-geo.tab.gz", 'rb') as f_in:
    with open(f"{base_folder}TN397-geo.tab", 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

tbl = 'tblTN397_Gradients4_uw_par'
raw_folder = f'{vs.cruise}{tbl}/raw/'
with gzip.open(f"{base_folder}par_TN397.tsdata.gz", 'rb') as f_in:
    with open(f"{raw_folder}par_TN397.tsdata", 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)


tbl = 'tblTN397_Gradients4_CTD'        
vs.leafStruc(vs.cruise + tbl)
base_folder = f'{vs.cruise}{tbl}/raw/'
os.chdir(base_folder)

wget_str = (
        "wget -nH --cut-dirs 5 -r --no-parent http://scope.soest.hawaii.edu/FTP/scope/ctd/gradients4/"
    )
os.system(wget_str)

wget_str = (
        "wget -nH --cut-dirs 5 -r --no-parent http://scope.soest.hawaii.edu/data/gradients/data/gradients4.sum"
    )
os.system(wget_str)

tbl = 'tblTN397_Gradients4'   
vs.leafStruc(vs.cruise + tbl)
base_folder = f'{vs.cruise}{tbl}/raw/'
os.chdir(base_folder)

wget_str = (
        f"wget -nH --cut-dirs 5 -r --no-parent http://scope.soest.hawaii.edu/FTP/scope/water/gradients4.gof"
    )
os.system(wget_str)
