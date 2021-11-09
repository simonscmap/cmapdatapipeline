import os
import sys
import glob
import pandas as pd
import numpy as np
import requests
import urllib
import pycmap
import vault_structure as vs


token_prefix = 'Api-Key '
token = '41061240-e9ff-11e9-bf30-edd064890625'
query_url = 'https://simonscmap.dev/api/data/query?query='
query =  'select distinct table_name from dbo.tblVariables'
server = 'mariana'
urquery=urllib.parse.quote(query_url + query + 'servername=' +server)
response = requests.get(urquery, token_prefix + token)


api = pycmap.API(token=token)
df_datasets = api.query('EXEC uspDatasets')

model_dir = vs.vault + 'model/'

dir_list = np.sort(glob.glob(os.path.join(model_dir, '*')))
d = dir_list[0]

os.listdir(d)
if len(os.listdir(d+'/code')) == 0:
    print('empty')
len(os.listdir(d+'/rep'))
for root, dirs, files in os.walk(d):
    for 




