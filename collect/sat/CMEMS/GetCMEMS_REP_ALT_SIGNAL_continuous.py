import sys
import os
import datetime
import glob
import time
import ftplib 

from multiprocessing import Pool

sys.path.append("ingest")
sys.path.append("../../../")

import vault_structure as vs
import credentials as cr
import DB
import metadata
import api_checks as api


tbl = 'tblAltimetry_REP_Signal'
base_folder = f'{vs.satellite}{tbl}/raw/'
# vs.leafStruc(vs.satellite+tbl)

output_dir = base_folder.replace(" ", "\\ ")

def checkLatestFTPData():
    ftp = ftplib.FTP('my.cmems-du.eu', cr.usr_cmem, cr.psw_cmem)
    ftp.cwd('/Core/SEALEVEL_GLO_PHY_L4_MY_008_047/cmems_obs-sl_glo_phy-ssh_my_allsat-l4-duacs-0.25deg_P1D')
    ftp_content = ftp.nlst()
    latest_yr = ftp_content[-1:][0]
    ftp.cwd(latest_yr)
    ftp_content_yr = ftp.nlst()
    latest_mnt = ftp_content_yr[-1:][0]
    ftp.cwd(latest_mnt)
    latest_dt = ftp.nlst()
    latest_file = latest_dt[-1:][0]
    last_import = DB.dbRead("SELECT MAX(Original_Name) fl FROM tblProcess_Queue WHERE Table_Name = 'tblAltimetry_REP_Signal'",'Rainier')
    if latest_file == last_import['fl'][0].replace(' ',''):
        print("No new data for tblAltimetry_REP_Signal")
    latest_date = latest_file.rsplit('_',2)[1]
    max_date = datetime.date(int(latest_yr),int(latest_mnt),int(latest_date[-2:]))
    return max_date
    

def getMaxDate(tbl):
    ## Check tblIngestion_Queue for downloaded but not ingested
    qry = f"SELECT Path from dbo.tblIngestion_Queue WHERE Table_Name = '{tbl}' AND Ingested IS NULL"
    df_ing = DB.dbRead(qry, 'Rainier')
    if len(df_ing) == 0:
        qry = f"SELECT max(path) mx from dbo.tblIngestion_Queue WHERE Table_Name = '{tbl}' AND Ingested IS NOT NULL"
        mx_path = DB.dbRead(qry,'Rainier')
        path_date = mx_path['mx'][0].split('.parquet')[0].rsplit('rep/',1)[1].rsplit('_',2)[1]
        max_path_date = datetime.date(int(path_date[:-4]),int(path_date[4:6]),int(path_date[-2:])) 
        qry = f"SELECT max(original_name) mx from dbo.tblProcess_Queue WHERE Table_Name = '{tbl}' AND Error_str IS NOT NULL"
        mx_name = DB.dbRead(qry,'Rainier')
        if mx_name['mx'][0] == None:
            max_name_date = datetime.date(1900,1,1)
        else: 
            yr, mo, day = mx_name['mx'][0].strip().split('_')
            max_name_date = datetime.date(int(yr),int(mo),int(day))  
        max_data_date = api.maxDateCluster(tbl)   

        max_date = max([max_path_date,max_name_date,max_data_date])
    else:
        last_path = df_ing['Path'].max()
        path_date = last_path.split('.parquet')[0].rsplit(tbl+'_',1)[1]
        yr, mo, day = path_date.split('_')
        max_date = datetime.date(int(yr),int(mo),int(day))
    return max_date



def wget_file(date,retry=False):
    yr = f'{date:%Y}'
    mn = f'{date:%m}'
    dy = f'{date:%d}'
    start_index = date.strftime('%Y_%m_%d')
    fpath = f"ftp://my.cmems-du.eu/Core/SEALEVEL_GLO_PHY_L4_MY_008_047/cmems_obs-sl_glo_phy-ssh_my_allsat-l4-duacs-0.25deg_P1D/{yr}/{mn}/dt_global_allsat_phy_l4_{yr}{mn}{dy}_*"    
    wget_str = f"""wget --no-parent -nd -r -m --ftp-user={cr.usr_cmem} --ftp-password={cr.psw_cmem} {fpath}  -P  {output_dir}"""
    try:
        os.system(wget_str)
        pname = glob.glob(f"{base_folder}dt_global_allsat_phy_l4_{yr}{mn}{dy}*")
        Original_Name = pname[0].split(f"{base_folder}")[1]
        save_path = base_folder+Original_Name
            ## Remove empty downloads
        if os.path.getsize(save_path) == 0:
            print(f'empty download for {start_index}')
            os.remove(save_path)
            if not retry:
                metadata.tblProcess_Queue_Download_Insert(f"{start_index}", tbl, 'Opedia', 'Rainier','Download Error')
                
        else:
            if retry:
                Error_Date = f"{start_index}"
                metadata.tblProcess_Queue_Download_Error_Update(Error_Date, Original_Name,  tbl, 'Opedia', 'Rainier')
                print(f"Successful retry for {Error_Date}")
            else:
                metadata.tblProcess_Queue_Download_Insert(Original_Name, tbl, 'Opedia', 'Rainier')
    except:
        print("No file found for date: " + start_index )
        metadata.tblProcess_Queue_Download_Insert(f"{start_index}", tbl, 'Opedia', 'Rainier','No data')

def retryError(tbl,output_dir):
    qry = f"SELECT Original_Name from dbo.tblProcess_Queue WHERE Table_Name = '{tbl}' AND Error_Str IS NOT NULL"
    df_err = DB.dbRead(qry, 'Rainier')
    dt_list = df_err['Original_Name'].to_list()
    if len(dt_list)>0:
        dt_list = [datetime.datetime.strptime(x.strip(), '%Y_%m_%d').date() for x in dt_list]
        for dt in dt_list:
            wget_file(output_dir, dt, True)

# retryError(tbl)



max_date = getMaxDate(tbl)
print(f"Last {tbl} data downloaded: {max_date}")
end_date = checkLatestFTPData()

if max_date != end_date:
    delta = datetime.timedelta(days=1)
    max_date += delta
    end_date -= delta

    while max_date <= end_date:
        wget_file(max_date)
        max_date += delta
        time.sleep(.1)
        



