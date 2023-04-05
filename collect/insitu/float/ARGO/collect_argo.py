import os
import sys

import requests
from bs4 import BeautifulSoup
import re

sys.path.append("ingest")
from ingest import vault_structure as vs

tbl = 'tblArgoBGC_REP'
base_folder = f'{vs.float_dir}{tbl}/raw/'
output_dir = base_folder.replace(" ", "\\ ")

june_10_link = "https://www.seanoe.org/data/00311/42182/data/85023.tar.gz"
july_10_link = "https://www.seanoe.org/data/00311/42182/data/86141.tar.gz"


url = 'https://www.seanoe.org/data/00311/42182/#'
source_code = requests.get(url)
soup = BeautifulSoup(source_code.content, 'lxml')
links = dict([[i.contents[0], str(i.get('href'))] for i in soup.find_all('a', href=True)])

tar_links = {}
for key, value in links.items():
        if '.tar.gz' in value and (('2021-' in key and int(key.split('-')[1] ) >6) or '2022' in key):
            tar_links[key] = value


# output_dir = vs.collected_data + "insitu/float/ARGO/"
del tar_links['2022-06-10']

def wget_file(fpath, fname, output_dir):
    os.system("wget -O "  + " '" + output_dir + fname  + ".tar.gz' "+ fpath)

for key, value in tar_links.items():
    print(key)
    wget_file(value, key, output_dir)
    
