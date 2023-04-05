import sys
import os
import pandas as pd
import numpy as np
import glob
import re
from zipfile import ZipFile

sys.path.append("ingest")

from ingest import vault_structure as vs
from ingest import common as cmn
from ingest import DB

server = 'Rossby'
db_name = 'Opedia'

table_name = 'tblGOSHIP_Reported'

base_folder = vs.download_transfer

dataset_id = cmn.getDatasetID_Tbl_Name(table_name, db_name, server)
qry = f"""
SELECT  r.[Reference_ID]
    ,[Dataset_ID]
    ,[Reference]
    ,[Data_DOI]
    ,[DOI_Download_Link]
    ,[Entity_Name]
    ,[CMAP_Format]  
FROM [Opedia].[dbo].[tblDataset_References] r inner join 
[dbo].[tblDataset_DOI_Download] d on r.reference_id = d.reference_id
where Dataset_ID = {dataset_id}
"""
df_ref = DB.dbRead(qry, server)

wget_str = f'wget --no-check-certificate "{df_ref["DOI_Download_Link"][0]}" -O "{base_folder + df_ref["Entity_Name"][0]}"'
os.system(wget_str)

with ZipFile(f'{base_folder + df_ref["Entity_Name"][0]}',"r") as zip_ref:
    zip_ref.extractall(base_folder+'/doi_check/')

zip_list = glob.glob(base_folder+'doi_check/*')


def unzip_directory(directory):
    """" This function unzips (and then deletes) all zip files in a directory """
    for root, dirs, files in os.walk(directory):
        for filename in files:
            print(filename)
            if re.search(r'\.zip$', filename):
                to_path = os.path.join(root, filename.split('.zip')[0])
                zipped_file = os.path.join(root, filename)
                if not os.path.exists(to_path):
                    os.makedirs(to_path)
                    with ZipFile(zipped_file, 'r') as zfile:
                        zfile.extractall(path=to_path)
                    # deletes zip file
                    os.remove(zipped_file)

def exists_zip(directory):
    """ This function returns T/F whether any .zip file exists within the directory, recursively """
    is_zip = False
    for root, dirs, files in os.walk(directory):
        for filename in files:
            if re.search(r'\.zip$', filename):
                is_zip = True
    return is_zip

def unzip_directory_recursively(directory, max_iter=1000):
    print("Does the directory path exist? ", os.path.exists(directory))
    """ Calls unzip_directory until all contained zip files (and new ones from previous calls)
    are unzipped
    """
    iterate = 0
    while exists_zip(directory) and iterate < max_iter:
        unzip_directory(directory)
        iterate += 1
    pre = "Did not " if iterate < max_iter else "Did"
    print(pre, "time out based on max_iter limit of", max_iter, ". Took iterations:", iterate)


# unzip_directory(base_folder+'doi_check/reported/southern/SR03')
# unzip_directory_recursively(base_folder+'doi_check/reported')



def extract_nested_zip(zippedFile, toFolder):
    """ Extract a zip file including any nested zip files
        Delete the zip file(s) after extraction
    """
    with ZipFile(zippedFile, 'r') as zfile:
        zfile.extractall(path=toFolder)
    os.remove(zippedFile)
    for root, dirs, files in os.walk(toFolder):
        for filename in files:
            if re.search(r'\.zip$', filename):
                fileSpec = os.path.join(root, filename)
                extract_nested_zip(fileSpec, root)

# extract_nested_zip(base_folder + df_ref["Entity_Name"][0], base_folder+'doi_check/')
# extract_nested_zip(base_folder + df_ref["Entity_Name"][0], base_folder+'doi_check/')