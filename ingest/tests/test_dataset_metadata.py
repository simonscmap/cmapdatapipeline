import os
import sys
import glob
import datetime
import pandas as pd
import numpy as np
import requests
import urllib
import filecmp
import pycmap
import vault_structure as vs

pd.set_option('display.max_columns', None)

token_prefix = 'Api-Key '
token = '41061240-e9ff-11e9-bf30-edd064890625'

query =  'select distinct table_name from dbo.tblVariables'

api = pycmap.API(token=token)
df_datasets = api.query('EXEC uspDatasets')

### Loop through Dropbox to check if all subdirectories exist and if they're populated
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

### Loop through Dropbox/collected_data for a list to match raw downloads to datasets
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

### Loop through Dropbox/app folder for user submitted datasets
d1 = {'full_path':[], 'filename':[], 'table_name':[], 'mod_date':[]}
df_app_data = pd.DataFrame(data=d1)

for root, dirs, files in os.walk(str(vs.app_data)):
    for f in files:
        full_path = os.path.join(root, f)
        mod_date = datetime.datetime.utcfromtimestamp(os.path.getmtime(full_path)).strftime('%Y-%m-%d %H:%M:%S')
        xls = pd.read_excel(full_path, sheet_name = 'dataset_meta_data')
        if 'dataset_short_name' in xls.columns:
            table_name = xls['dataset_short_name'][0]
        elif 'dataset_long_name' in xls.columns:
            table_name = xls['dataset_long_name'][0]
        else:
            table_name = None
        d1 = {'full_path':[full_path], 'filename':[f], 'table_name':[table_name], 'mod_date':[mod_date]}
        temp_df = pd.DataFrame(data=d1)
        df_app_data = df_app_data.append(temp_df, ignore_index=True)
df_app_data.to_excel(vs.download_transfer +'app_data_files.xlsx', index=False)

### Loop through Dropbox/staging folder for small datasets
d1 = {'full_path':[], 'filename':[], 'table_name':[], 'mod_date':[]}
df_staging_data = pd.DataFrame(data=d1)

for root, dirs, files in os.walk(str(vs.combined)):
    for f in files:
        if f.endswith('.xlsx'):
            full_path = os.path.join(root, f)
            mod_date = datetime.datetime.utcfromtimestamp(os.path.getmtime(full_path)).strftime('%Y-%m-%d %H:%M:%S')
            try:
                xls = pd.read_excel(full_path, sheet_name = 'dataset_meta_data')
                if 'dataset_short_name' in xls.columns:
                    table_name = xls['dataset_short_name'][0]
                elif 'dataset_long_name' in xls.columns:
                    table_name = xls['dataset_long_name'][0]
                else:
                    table_name = None
            except:
                table_name = 'No dataset_meta_data sheet'
            d1 = {'full_path':[full_path], 'filename':[f], 'table_name':[table_name], 'mod_date':[mod_date]}
            temp_df = pd.DataFrame(data=d1)
            df_staging_data = df_staging_data.append(temp_df, ignore_index=True)
df_staging_data.to_excel(vs.download_transfer +'staging_data_files.xlsx', index=False)

### Loop through cmapdata/ingest and /process for a list of all .py scripts to match to datasets
code_type = ['collect','process']
d2 = {'full_path':[],'code_type':[], 'section':[], 'filename':[], 'mod_date':[]}
df_scripts = pd.DataFrame(data=d2)
rep_dir = '/home/exx/Documents/CMAP/cmapdata/'
# rep_dir = '/home/exx/Documents/CMAP/cmapdata/' CHECK CMAPINGEST AND PROCESS
for ct in code_type:
    for root, dirs, files in os.walk(str(rep_dir) + ct):
        for f in files:
            full_path = os.path.join(root, f)
            mod_date = datetime.datetime.utcfromtimestamp(os.path.getmtime(full_path)).strftime('%Y-%m-%d %H:%M:%S')
            section = '/'.join(root.rsplit('/',2)[1:])
            d2 = {'full_path':[full_path],'code_type':[ct], 'section':[section],'filename':[f], 'mod_date':[mod_date]}
            temp_df = pd.DataFrame(data=d2)
            df_scripts = df_scripts.append(temp_df, ignore_index=True)

df_scripts.to_excel(vs.download_transfer +'process_files.xlsx', index=False)

d2 = {'full_path':[],'code_type':[], 'section':[], 'filename':[], 'mod_date':[]}
df_old_scripts = pd.DataFrame(data=d2)

### Loop through old repositories to check for scripts not in new cmapdata
code_type = ['cmapcollect','cmapprocess','DBIngest/dbInsert']
rep_dir = '/home/exx/Documents/CMAP/'
for ct in code_type:
    for root, dirs, files in os.walk(str(rep_dir) + ct):
        for f in files:
            if f.endswith('.py'):
                full_path = os.path.join(root, f)
                mod_date = datetime.datetime.utcfromtimestamp(os.path.getmtime(full_path)).strftime('%Y-%m-%d %H:%M:%S')
                section = '/'.join(root.rsplit('/',2)[1:])
                d2 = {'full_path':[full_path],'code_type':[ct], 'section':[section],'filename':[f], 'mod_date':[mod_date]}
                temp_df = pd.DataFrame(data=d2)
                df_old_scripts = df_old_scripts.append(temp_df, ignore_index=True)

df_old_scripts.to_excel(vs.download_transfer +'old_process_files.xlsx', index=False)

### Old vs new script comparison
f1 = '/home/exx/Documents/CMAP/cmapprocess/insitu/float/ARGO_Core.py'
f2 = '/home/exx/Documents/CMAP/cmapdata/process/insitu/float/ARGO/process_ARGO.py'

f1 = '/home/exx/Documents/CMAP/cmapdata/collect/sat/AVISO/GetAVISO_NRT_UV_SLA.py'
f2 = '/home/exx/Documents/CMAP/cmapcollect/sat/AVISO/GetAVISO_NRT_UV_SLA.py'
filecmp.cmp(f1, f2)


with open(f1, 'r') as file1:
    with open(f2, 'r') as file2:
        difference = set(file1).difference(file2)
        fl1 = file1.readlines()
        fl2 = file2.readlines()

        only_in_f1 = [i for i in fl1 if i not in fl2]
        only_in_f2 = [i for i in fl2 if i not in fl1]
        print(only_in_f1)
        print(only_in_f2)


with open(f1, 'r') as file1:
    fl1 = file1.readlines()
with open(f2, 'r') as file2: 
    fl2 = file2.readlines()

only_in_f1 = [i for i in fl1 if i not in fl2]
only_in_f2 = [i for i in fl2 if i not in fl1]

difference.discard('\n')
print(difference)

### Pull metadata from API, save as parquet files in /vault/meta
table_name = 'tblArgoMerge_REP'
api.get_dataset_metadata(table_name)
var_list = 
api.get_metadata(table_name, ['sst', 'argo_merge_salinity_adj'])