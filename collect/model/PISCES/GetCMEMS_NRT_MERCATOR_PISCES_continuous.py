import sys
import os
import datetime
from ftplib import FTP
import requests
from urllib.parse import quote
import pandas as pd

sys.path.append("ingest")
sys.path.append("../../../")

import vault_structure as vs
import DB
import metadata
import credentials as cr
import api_checks as api

tbl = 'tblPisces_Forecast_cl1'
base_folder = f'{vs.model}{tbl}/raw/'
output_dir = base_folder.replace(" ", "\\ ")
user = cr.usr_cmem
pw = cr.psw_cmem


def query(sql):   
    url = f"https://cmapdatavalidation.com/cluster/query?sql={quote(sql)}"
    try:
        outPath = vs.download_transfer+'qry.parquet'
        headers = {
            "accept": "application/json'",
            "Authorization": f"{cr.pycmap_api_key}"
        }
        resp = requests.get(url, headers=headers, timeout=1000) 
        totalbits = 0
        with open(outPath, 'wb') as f:
            for chunk in resp.iter_content(chunk_size=1024):
                if chunk:
                    totalbits += 1024
                    print("Downloaded",totalbits*1025,"KB...")
                    f.write(chunk)
        print(pd.read_parquet(outPath))
    except Exception as e:        
        print(str(e))
    return 


def getMaxDate(tbl):
    ## Check tblIngestion_Queue for downloaded but not ingested
    qry = f"SELECT Path from dbo.tblIngestion_Queue WHERE Table_Name = '{tbl}' AND Ingested IS NULL"
    df_ing = DB.dbRead(qry, 'Rainier')

    ### Add in check for Process_Queue
    qry = f"SELECT Original_Name from dbo.tblProcess_Queue WHERE Table_Name = '{tbl}' AND Processed IS NULL AND Error_Str IS NULL"
    df_pro = DB.dbRead(qry, 'Rainier')

    if len(df_ing) == 0 and len(df_pro) == 0:
        ## Change to pull from cluster
        # qry = f"SELECT MAX(Time) as Time_max FROM {tbl}"
        # df_mx = DB.dbRead(qry, 'Rossby')
        # max_date = df_mx['Time_max'][0]
        qry = f"SELECT max(path) mx from dbo.tblIngestion_Queue WHERE Table_Name = '{tbl}' AND Ingested IS NOT NULL"
        mx_path = DB.dbRead(qry,'Rainier')
        path_date = mx_path['mx'][0].split('.parquet')[0].rsplit(tbl+'_',1)[1]
        yr, mo, day = path_date.split('_')
        max_date = datetime.date(int(yr),int(mo),int(day))  
    elif len(df_ing)>0:
        last_path = df_ing['Path'].max()
        path_date = last_path.split('.parquet')[0].rsplit(tbl+'_',1)[1]
        yr, mo, day = path_date.split('_')
        max_date = datetime.date(int(yr),int(mo),int(day))        
    else:
        last_path = df_pro['Original_Name'].max()    
        path_date = last_path.split('_mean_')[1].split('.nc',1)[0]
        yr = path_date[:4]
        mo = path_date[4:6]
        day = path_date[6:8]
        max_date = datetime.date(int(yr),int(mo),int(day))
    return max_date


#ftp://mdehghaniashkez@nrt.cmems-du.eu/Core/GLOBAL_ANALYSIS_FORECAST_BIO_001_028/global-analysis-forecast-bio-001-028-daily/2020/11/mercatorbiomer4v2r1_global_mean_20201101.nc
def wget_file(output_dir, yr, mnth, day, usr, psw, retry=False):
    fpath = f"ftp://nrt.cmems-du.eu/Core/GLOBAL_ANALYSIS_FORECAST_BIO_001_028/global-analysis-forecast-bio-001-028-daily/{yr}/{mnth}/mercatorbiomer4v2r1_global_mean_{yr}{mnth}{day}*"  
    Original_Name = f'mercatorbiomer4v2r1_global_mean_{yr}{mnth}{day}.nc'
    try:  
        os.system(
            f"""wget --no-parent -nd -r -m --ftp-user={usr} --ftp-password={psw} {fpath} -P {output_dir}"""
        )
        save_path = output_dir.replace("\\ ", " ")+Original_Name
        ## Remove empty downloads
        if os.path.getsize(save_path) == 0:
            print(f'empty download for {yr}{mnth}{day}')
            if not retry:
                metadata.tblProcess_Queue_Download_Insert(f"{yr}_{mnth}_{day}", tbl, 'Opedia', 'Rainier','Empty File')
            os.remove(save_path)
        else:
            if retry:
                Error_Date = f"{yr}_{mnth}_{day}"
                metadata.tblProcess_Queue_Download_Error_Update(Error_Date, Original_Name, tbl, 'Opedia', 'Rainier')
                print(f"Successful retry for {Error_Date}")      
            metadata.tblProcess_Queue_Download_Insert(Original_Name, tbl, 'Opedia', 'Rainier')
    except:
        if not retry:
            metadata.tblProcess_Queue_Download_Insert(f"{yr}_{mnth}_{day}", tbl, 'Opedia', 'Rainier','Download Error')

def retryError(tbl):
    qry = f"SELECT Original_Name from dbo.tblProcess_Queue WHERE Table_Name = '{tbl}' AND Error_Str IS NOT NULL"
    df_err = DB.dbRead(qry, 'Rainier')
    dt_list = df_err['Original_Name'].to_list()
    dt_list = [datetime.datetime.strptime(x.strip(), '%Y_%m_%d').date() for x in dt_list]
    for dt in dt_list:
        yr = dt.year
        mnth = f"{dt:%m}"
        day = f"{dt:%d}"
        wget_file(output_dir, yr, mnth, day, user, pw, True)

def wgetOverwrite(output_dir, yr, mnth, day, usr, psw):
    fpath = f"ftp://nrt.cmems-du.eu/Core/GLOBAL_ANALYSIS_FORECAST_BIO_001_028/global-analysis-forecast-bio-001-028-daily/{yr}/{mnth}/mercatorbiomer4v2r1_global_mean_{yr}{mnth}{day}*"  
    Original_Name = f'mercatorbiomer4v2r1_global_mean_{yr}{mnth}{day}.nc'
    ###### TEST ON DATE ALREADY UP TO DATE
    ###### MAKE SURE ERROR NOT INCLUDED IF DATA ALREADY UP TO DATE
    try:  
        os.system(
            f"""wget --no-parent -nd -r -m --ftp-user={usr} --ftp-password={psw} {fpath} -P {output_dir}"""
        )
        save_path = output_dir.replace("\\ ", " ")+Original_Name
        ## Remove empty downloads
        ####### Need original name for errors because file already exists. Differs from other error logic where name is date
        if os.path.getsize(save_path) == 0:
            print(f'empty download for {yr}{mnth}{day}')
            metadata.tblProcess_Queue_Overwrite(Original_Name, tbl, 'Opedia', 'Rainier','Empty File')
            os.remove(save_path)
        else:
            query(f"delete from tblPisces_Forecast_cl1 where time='{yr}-{mnth}-{day}T12:00:00'")
            metadata.tblProcess_Queue_Overwrite(Original_Name, tbl, 'Opedia', 'Rainier')
    except:
            metadata.tblProcess_Queue_Overwrite(Original_Name, tbl, 'Opedia', 'Rainier','Download Error')


def testDownload(yr, mnth, day, usr, psw):
    test_dir = vs.download_transfer.replace(" ", "\\ ")
    fpath = f"ftp://nrt.cmems-du.eu/Core/GLOBAL_ANALYSIS_FORECAST_BIO_001_028/global-analysis-forecast-bio-001-028-daily/{yr}/{mnth}/mercatorbiomer4v2r1_global_mean_{yr}{mnth}{day}*"  
    os.system(
        f"""wget --no-parent -nd -r -m --ftp-user={usr} --ftp-password={psw} {fpath} -O {test_dir}PiscesTest{yr}{mnth}{day}.nc"""
    )

# testDownload('2023', '02', '08', user, pw)
max_date = getMaxDate(tbl)
end_date = datetime.date.today()
delta = datetime.timedelta(days=1)
max_date += delta
end_date +=datetime.timedelta(days=9)

min_time, max_time = api.temporalRange(tbl)
overwrite_end = datetime.datetime.strptime(max_time.split('T')[0], '%Y-%m-%d').date()
overwrite_start = overwrite_end - datetime.timedelta(days=16)

while overwrite_start <= overwrite_end:
    yr = overwrite_start.year
    mo = f"{overwrite_start:%m}"
    day = f"{overwrite_start:%d}"   
    wgetOverwrite(output_dir, yr, mo, day, user, pw)
    overwrite_start+= delta



while max_date <= end_date:
    yr = max_date.year
    mo = f"{max_date:%m}"
    day = f"{max_date:%d}"    
    wget_file(output_dir, yr, mo, day, user, pw)
    max_date += delta

            



