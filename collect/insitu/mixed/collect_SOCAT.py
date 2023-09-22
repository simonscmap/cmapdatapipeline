import os
import sys

sys.path.append("ingest")

import vault_structure as vs
import credentials as cr

tbl = 'tblSOCATv2022'
vs.leafStruc(vs.mixed+tbl)
base_folder = f'{vs.mixed}{tbl}/raw/'
output_dir = base_folder.replace(" ", "\\ ")

dl_url = 'https://www.ncei.noaa.gov/data/oceans/ncei/ocads/data/0253659/SOCATv2022.tsv'


wget_str = f'wget --no-check-certificate "{dl_url}" -P "{base_folder}"'
os.system(wget_str)

#head -n 7000 SOCATv2022.tsv >> header.tsv

