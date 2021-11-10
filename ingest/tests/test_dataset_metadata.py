import os
import sys
import glob
import datetime
import pandas as pd
import numpy as np
import requests
import urllib
import pycmap
import vault_structure as vs

pd.set_option('display.max_columns', None)

token_prefix = 'Api-Key '
token = '41061240-e9ff-11e9-bf30-edd064890625'
query_url = 'https://simonscmap.dev/api/data/query?query='
query =  'select distinct table_name from dbo.tblVariables'
server = 'mariana'
urquery=urllib.parse.quote(query_url + query + 'servername=' +server)
response = requests.get(urquery, token_prefix + token)


api = pycmap.API(token=token)
df_datasets = api.query('EXEC uspDatasets')


d = {'make':[], 'obs_type':[], 'table_name':[], 'raw':[], 'doc':[], 'rep':[], 'nrt':[], 'code':[], 'meta':[], 'stats':[], 'other':[]}
df_vault = pd.DataFrame(data=d)

vsub_list = ['model', 'observation/in-situ/cruise','observation/in-situ/float','observation/in-situ/station','observation/remote/satellite']

for vsub in vsub_list:
    vsub_dir = vs.vault + vsub + '/'
    obs_type = vsub
    for table_name in os.listdir(str(vsub_dir)):
        vault_list = os.listdir(os.path.join(str(vsub_dir),table_name))
        raw = doc = rep = nrt = code = meta = stats = other = None
        for vault_fol in vault_list: 
            file_count = len(os.listdir(os.path.join(str(vsub_dir),table_name,vault_fol)))
            if vault_fol == 'raw':
                raw = file_count
            elif vault_fol == 'doc':
                doc = file_count
            elif vault_fol == 'rep':
                rep = file_count
            elif vault_fol == 'nrt':
                nrt = file_count
            elif vault_fol == 'code':
                code = file_count
            elif vault_fol == 'metadata':
                meta = file_count
            elif vault_fol == 'stats':
                stats = file_count  
            else:
                if not other:
                    other = vault_fol
                else:
                    other = other +', '+ vault_fol 


        d = {'make':[vsub], 'obs_type':[obs_type], 'table_name':[table_name], 'raw':[raw], 'doc':[doc], 'rep':[rep], 'nrt':[nrt], 'code':[code], 'meta':[meta], 'stats':[stats], 'other':[other]}    
        temp_df = pd.DataFrame(data=d)
        df_vault = df_vault.append(temp_df, ignore_index = True)

df_vault.shape
df_datasets.shape

df_datasets.columns

df_join = df_vault.merge(df_datasets, how='outer', left_on='table_name', right_on='Table_Name')
df_join.shape

df_join.to_excel(vs.download_transfer +'dataset_meta_comparison.xlsx', index=False)


## dir is always full path
d1 = {'full_path':[],'dir':[], 'filename':[], 'mod_date':[]}
df_collected_data = pd.DataFrame(data=d1)

for root, dirs, files in os.walk(str(vs.collected_data)):
    for f in files:
        full_path = os.path.join(root, f)
        mod_date = datetime.datetime.utcfromtimestamp(os.path.getmtime(full_path)).strftime('%Y-%m-%d %H:%M:%S')
        d1 = {'full_path':[full_path],'dir':[root], 'filename':[f], 'mod_date':[mod_date]}
        temp_df = pd.DataFrame(data=d1)
        df_collected_data = df_collected_data.append(temp_df, ignore_index=True)

df_collected_data.to_excel(vs.download_transfer +'collected_data_files.xlsx', index=False)




