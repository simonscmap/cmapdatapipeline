#### DEVNOTE Dec 2nd, 20 ####
# Two tables (BGC) and (CORE)
# How to ID BGC? (Header?) fname list?
# According to this: https://euroargodev.github.io/argoonlineschool/L21_ArgoDatabyFloat_Prof.html
#     Then _prof suffix contains *all* profile data. In the future, use wget? to get not all profiles

# Write two functions. 
#1. Get all WMO in tblARGOTABLE, so new ones can be added
#2. Loop through all WMO, get header to group by CORE or BGC
#
# Next steps: once have list(s) of core/bgc
# clean function
# insert into test tbl
# create two metadata files
# insert metadata
#... etc..

 
 #dev note Dec 11th. Apparently an "S"prefix to the proj are 'synthetic' ie. containing both core and BGC. 
 #An index of the synthetic should be available at ftp://ftp.ifremer.fr/ifremer/argo/argo_synthetic-profile_index.txt
 # I've emailed to request auth for the FTP.


 #devnote on dec 21st: Dump metadata
 #which cols are data
 #which cols are metadata
 #loop through floats
 #for col in float, extract unique cols
 
#dev note dec 28th:
# check data type columns in table
#check that any cols are string stripped
# for file in synthetic list, 
# insert into tblArgoBGC_REP and tblARGO_Metadata
# function to insert metadata

#dev note dec 29th
#metadata insert worked, maybe has formatting issues, check json vs STATS table
#data inserts up to row 270, then has #@ Row 271, Column 151: Invalid character value for cast specification @#
#changed all table to nvarchar(max), no effect

#dev note dec 29th,later
# the columns with issues are:    SCIENTIFIC_CALIB_COEFFICIENT'],['SCIENTIFIC_CALIB_COMMENT','SCIENTIFIC_CALIB_EQUATION']
# what is the sql data type that will work with them...


# dev note Jan 21st, USE SYNTHETIC, don't get tricked by bio



#dev note: March 5th: processing gets hung up on memory overflow.
#Can't do pandas memory managment tricks on df in memory, becasue size balooons over 32gb.
#Next bad idea(s). 
# 1) If len(df/xdf) > n: 
#     save as csv. import with better col dtypes
#     If that doesn't work, import in chunks, process, 
# 2) - xarray to csv
#    - import with chunksize
#       - import csv specify dtypes 
#       - only important columns
#       - run cleaning func
#       - append to list
#    - combine list to df and add new cols
#    - export df and ingest into DB

#dev note as of march 8th:
"""
1. get list of floats not ingested;
2. write func, import nc with xarray
3. if nc.bytes > n:
      chunk/parquet
      df = read_parquet
   else:
       df = xdf.to_dataframe().reset_index
4. pass df to func that builds metadata df
5. pass df to func that appends columns
6. df to vault
7. df/metadata_df ingest into DB

"""


"""
!!!! FROM https://argo.ucsd.edu/data/data-faq/#RorD: !!!!!
What floats have additional sensors like oxygen and how do I get this data?
There is an argo_synthetic-profile_index.txt list available on the French Argo GDAC ftp which lists all the synthetic profile file names, date, latitude, longitude, ocean, profiler type, institution, ocean state parameters such as TEMP, PRES, DOXY, CHLA etc., the data mode of each parameter, and the date of file update. This can be searched, just like the simple argo_profile_index.txt list, for floats in the region of interest with the desired ocean state parameters. Refer to Reference Table 3: Parameter code table in the Argo User’s Manual for the name of the different parameters to search for.

To find the intermediate parameters, like TEMP_DOXY, etc, the argo_bio-profile_index.txt on the Argo GDAC ftps must be searched. The same information is included except that the intermediate parameters are listed instead.
"""


import urllib
import xarray as xr
import pandas as pd
import pycmap
import glob
import os
import shutil
from cmapingest import data
from cmapingest import DB
from cmapingest import common as cmn
from cmapingest import vault_structure as vs
from tqdm import tqdm
import numpy as np
import json
import gc
import random
import vaex


argo_base_dir = vs.collected_data + "ARGO/"
# argo_base_dir = '/home/nrhagen/Documents/CMAP/cmapprocess/ARGO/argo_data_temp/'
argo_daac_list = ['aoml','bodc','coriolis','csio','csiro','incois','jma','kma','kordi','meds','nmdis']

bgc_metadata_columns = [
'CONFIG_MISSION_NUMBER', 
'DATA_CENTRE',
'DATA_TYPE',
'DATE_CREATION',
'DATE_UPDATE',
'DIRECTION',
'FIRMWARE_VERSION',
'FLOAT_SERIAL_NO',
'FORMAT_VERSION',
'HANDBOOK_VERSION',
'PARAMETER', 
'PARAMETER_DATA_MODE',
'PI_NAME',
'PLATFORM_NUMBER',
'PLATFORM_TYPE',
'POSITIONING_SYSTEM',
'PROJECT_NAME',
'REFERENCE_DATE_TIME',
'STATION_PARAMETERS',
'WMO_INST_TYPE']

bgc_template_df = pd.DataFrame(columns=['BBP470','BBP470_ADJUSTED','BBP470_ADJUSTED_ERROR','BBP470_ADJUSTED_QC','BBP470_QC','BBP470_dPRES','BBP532','BBP532_ADJUSTED','BBP532_ADJUSTED_ERROR','BBP532_ADJUSTED_QC','BBP532_QC','BBP532_dPRES','BBP700','BBP700_2','BBP700_2_ADJUSTED','BBP700_2_ADJUSTED_ERROR','BBP700_2_ADJUSTED_QC','BBP700_2_QC','BBP700_2_dPRES','BBP700_ADJUSTED','BBP700_ADJUSTED_ERROR','BBP700_ADJUSTED_QC','BBP700_QC','BBP700_dPRES','BISULFIDE','BISULFIDE_ADJUSTED','BISULFIDE_ADJUSTED_ERROR','BISULFIDE_ADJUSTED_QC','BISULFIDE_dPRES','CNDC','CNDC_ADJUSTED','CNDC_ADJUSTED_ERROR','CNDC_ADJUSTED_QC','CNDC_dPRES','CDOM','CDOM_ADJUSTED','CDOM_ADJUSTED_ERROR','CDOM_ADJUSTED_QC','CDOM_QC','CDOM_dPRES','CHLA','CHLA_ADJUSTED','CHLA_ADJUSTED_ERROR','CHLA_ADJUSTED_QC','CHLA_QC','CHLA_dPRES','CONFIG_MISSION_NUMBER','CP660','CP660_ADJUSTED','CP660_ADJUSTED_ERROR','CP660_ADJUSTED_QC','CP660_QC','CP660_dPRES','CYCLE_NUMBER','DATA_CENTRE','DATA_TYPE','DATE_CREATION','DATE_UPDATE','DIRECTION','DOWNWELLING_PAR','DOWNWELLING_PAR_ADJUSTED','DOWNWELLING_PAR_ADJUSTED_ERROR','DOWNWELLING_PAR_ADJUSTED_QC','DOWNWELLING_PAR_QC','DOWNWELLING_PAR_dPRES','DOWN_IRRADIANCE380','DOWN_IRRADIANCE380_ADJUSTED','DOWN_IRRADIANCE380_ADJUSTED_ERROR','DOWN_IRRADIANCE380_ADJUSTED_QC','DOWN_IRRADIANCE380_QC','DOWN_IRRADIANCE380_dPRES','DOWN_IRRADIANCE412','DOWN_IRRADIANCE412_ADJUSTED','DOWN_IRRADIANCE412_ADJUSTED_ERROR','DOWN_IRRADIANCE412_ADJUSTED_QC','DOWN_IRRADIANCE412_QC','DOWN_IRRADIANCE412_dPRES','DOWN_IRRADIANCE443','DOWN_IRRADIANCE443_ADJUSTED','DOWN_IRRADIANCE443_ADJUSTED_ERROR','DOWN_IRRADIANCE443_ADJUSTED_QC','DOWN_IRRADIANCE443_QC','DOWN_IRRADIANCE443_dPRES','DOWN_IRRADIANCE490','DOWN_IRRADIANCE490_ADJUSTED','DOWN_IRRADIANCE490_ADJUSTED_ERROR','DOWN_IRRADIANCE490_ADJUSTED_QC','DOWN_IRRADIANCE490_QC','DOWN_IRRADIANCE490_dPRES','DOWN_IRRADIANCE555','DOWN_IRRADIANCE555_ADJUSTED','DOWN_IRRADIANCE555_ADJUSTED_ERROR','DOWN_IRRADIANCE555_ADJUSTED_QC','DOWN_IRRADIANCE555_QC','DOWN_IRRADIANCE555_dPRES','DOXY','DOXY2','DOXY2_ADJUSTED','DOXY2_ADJUSTED_ERROR','DOXY2_ADJUSTED_QC','DOXY2_QC','DOXY2_dPRES','DOXY_ADJUSTED','DOXY_ADJUSTED_ERROR','DOXY_ADJUSTED_QC','DOXY_QC','DOXY_dPRES','FIRMWARE_VERSION','FLOAT_SERIAL_NO','FORMAT_VERSION','HANDBOOK_VERSION','JULD','JULD_LOCATION','JULD_QC','LATITUDE','LONGITUDE','NITRATE','NITRATE_ADJUSTED','NITRATE_ADJUSTED_ERROR','NITRATE_ADJUSTED_QC','NITRATE_QC','NITRATE_dPRES','PARAMETER','PARAMETER_DATA_MODE','PH_IN_SITU_TOTAL','PH_IN_SITU_TOTAL_ADJUSTED','PH_IN_SITU_TOTAL_ADJUSTED_ERROR','PH_IN_SITU_TOTAL_ADJUSTED_QC','PH_IN_SITU_TOTAL_QC','PH_IN_SITU_TOTAL_dPRES','PI_NAME','PLATFORM_NUMBER','PLATFORM_TYPE','POSITIONING_SYSTEM','POSITION_QC','PRES','PRES_ADJUSTED','PRES_ADJUSTED_ERROR','PRES_ADJUSTED_QC','PRES_QC','PROFILE_BBP470_QC','PROFILE_BBP532_QC','PROFILE_BBP700_2_QC','PROFILE_BBP700_QC','PROFILE_BISULFIDE_QC','PROFILE_CNDC_QC','PROFILE_CDOM_QC','PROFILE_CHLA_QC','PROFILE_CP660_QC','PROFILE_DOWNWELLING_PAR_QC','PROFILE_DOWN_IRRADIANCE380_QC','PROFILE_DOWN_IRRADIANCE412_QC','PROFILE_DOWN_IRRADIANCE443_QC','PROFILE_DOWN_IRRADIANCE490_QC','PROFILE_DOWN_IRRADIANCE555_QC','PROFILE_DOXY2_QC','PROFILE_DOXY_QC','PROFILE_NITRATE_QC','PROFILE_PH_IN_SITU_TOTAL_QC','PROFILE_PRES_QC','PROFILE_PSAL_QC','PROFILE_TEMP_QC','PROFILE_TURBIDITY_QC','PROFILE_UP_RADIANCE412_QC','PROFILE_UP_RADIANCE443_QC','PROFILE_UP_RADIANCE490_QC','PROFILE_UP_RADIANCE555_QC','PROJECT_NAME','PSAL','PSAL_ADJUSTED','PSAL_ADJUSTED_ERROR','PSAL_ADJUSTED_QC','PSAL_QC','PSAL_dPRES','REFERENCE_DATE_TIME','SCIENTIFIC_CALIB_COEFFICIENT','SCIENTIFIC_CALIB_COMMENT','SCIENTIFIC_CALIB_DATE','SCIENTIFIC_CALIB_EQUATION','STATION_PARAMETERS','TEMP','TEMP_ADJUSTED','TEMP_ADJUSTED_ERROR','TEMP_ADJUSTED_QC','TEMP_QC','TEMP_dPRES','TURBIDITY','TURBIDITY_ADJUSTED','TURBIDITY_ADJUSTED_ERROR','TURBIDITY_ADJUSTED_QC','TURBIDITY_QC','TURBIDITY_dPRES','UP_RADIANCE412','UP_RADIANCE412_ADJUSTED','UP_RADIANCE412_ADJUSTED_ERROR','UP_RADIANCE412_ADJUSTED_QC','UP_RADIANCE412_QC','UP_RADIANCE412_dPRES','UP_RADIANCE443','UP_RADIANCE443_ADJUSTED','UP_RADIANCE443_ADJUSTED_ERROR','UP_RADIANCE443_ADJUSTED_QC','UP_RADIANCE443_QC','UP_RADIANCE443_dPRES','UP_RADIANCE490','UP_RADIANCE490_ADJUSTED','UP_RADIANCE490_ADJUSTED_ERROR','UP_RADIANCE490_ADJUSTED_QC','UP_RADIANCE490_QC','UP_RADIANCE490_dPRES','UP_RADIANCE555','UP_RADIANCE555_ADJUSTED','UP_RADIANCE555_ADJUSTED_ERROR','UP_RADIANCE555_ADJUSTED_QC','UP_RADIANCE555_QC','UP_RADIANCE555_dPRES','WMO_INST_TYPE'])

bgc_data_columns = [
'BBP470',
'BBP470_ADJUSTED',
'BBP470_ADJUSTED_ERROR',
'BBP470_ADJUSTED_QC',
'BBP470_QC',
'BBP470_dPRES',
'BBP532',
'BBP532_ADJUSTED',
'BBP532_ADJUSTED_ERROR',
'BBP532_ADJUSTED_QC',
'BBP532_QC',
'BBP532_dPRES',
'BBP700',
'BBP700_2',
'BBP700_2_ADJUSTED',
'BBP700_2_ADJUSTED_ERROR',
'BBP700_2_ADJUSTED_QC',
'BBP700_2_QC',
'BBP700_2_dPRES',
'BBP700_ADJUSTED',
'BBP700_ADJUSTED_ERROR',
'BBP700_ADJUSTED_QC',
'BBP700_QC',
'BBP700_dPRES',
'BISULFIDE',
'BISULFIDE_ADJUSTED',
'BISULFIDE_ADJUSTED_ERROR',
'BISULFIDE_ADJUSTED_QC',
'BISULFIDE_dPRES',
'CNDC',
'CNDC_ADJUSTED',
'CNDC_ADJUSTED_ERROR',
'CNDC_ADJUSTED_QC',
'CNDC_dPRES',
'CDOM',
'CDOM_ADJUSTED',
'CDOM_ADJUSTED_ERROR',
'CDOM_ADJUSTED_QC',
'CDOM_QC',
'CDOM_dPRES',
'CHLA',
'CHLA_ADJUSTED',
'CHLA_ADJUSTED_ERROR',
'CHLA_ADJUSTED_QC',
'CHLA_QC',
'CHLA_dPRES',
'CP660',
'CP660_ADJUSTED',
'CP660_ADJUSTED_ERROR',
'CP660_ADJUSTED_QC',
'CP660_QC',
'CP660_dPRES',
'DOWNWELLING_PAR',
'DOWNWELLING_PAR_ADJUSTED',
'DOWNWELLING_PAR_ADJUSTED_ERROR',
'DOWNWELLING_PAR_ADJUSTED_QC',
'DOWNWELLING_PAR_QC',
'DOWNWELLING_PAR_dPRES',
'DOWN_IRRADIANCE380',
'DOWN_IRRADIANCE380_ADJUSTED',
'DOWN_IRRADIANCE380_ADJUSTED_ERROR',
'DOWN_IRRADIANCE380_ADJUSTED_QC',
'DOWN_IRRADIANCE380_QC',
'DOWN_IRRADIANCE380_dPRES',
'DOWN_IRRADIANCE412',
'DOWN_IRRADIANCE412_ADJUSTED',
'DOWN_IRRADIANCE412_ADJUSTED_ERROR',
'DOWN_IRRADIANCE412_ADJUSTED_QC',
'DOWN_IRRADIANCE412_QC',
'DOWN_IRRADIANCE412_dPRES',
'DOWN_IRRADIANCE443',
'DOWN_IRRADIANCE443_ADJUSTED',
'DOWN_IRRADIANCE443_ADJUSTED_ERROR',
'DOWN_IRRADIANCE443_ADJUSTED_QC',
'DOWN_IRRADIANCE443_QC',
'DOWN_IRRADIANCE443_dPRES',
'DOWN_IRRADIANCE490',
'DOWN_IRRADIANCE490_ADJUSTED',
'DOWN_IRRADIANCE490_ADJUSTED_ERROR',
'DOWN_IRRADIANCE490_ADJUSTED_QC',
'DOWN_IRRADIANCE490_QC',
'DOWN_IRRADIANCE490_dPRES',
'DOWN_IRRADIANCE555',
'DOWN_IRRADIANCE555_ADJUSTED',
'DOWN_IRRADIANCE555_ADJUSTED_ERROR',
'DOWN_IRRADIANCE555_ADJUSTED_QC',
'DOWN_IRRADIANCE555_QC',
'DOWN_IRRADIANCE555_dPRES',
'DOXY',
'DOXY2',
'DOXY2_ADJUSTED',
'DOXY2_ADJUSTED_ERROR',
'DOXY2_ADJUSTED_QC',
'DOXY2_QC',
'DOXY2_dPRES',
'DOXY_ADJUSTED',
'DOXY_ADJUSTED_ERROR',
'DOXY_ADJUSTED_QC',
'DOXY_QC',
'DOXY_dPRES',
'JULD_QC',
'NITRATE',
'NITRATE_ADJUSTED',
'NITRATE_ADJUSTED_ERROR',
'NITRATE_ADJUSTED_QC',
'NITRATE_QC',
'NITRATE_dPRES',
'PH_IN_SITU_TOTAL',
'PH_IN_SITU_TOTAL_ADJUSTED',
'PH_IN_SITU_TOTAL_ADJUSTED_ERROR',
'PH_IN_SITU_TOTAL_ADJUSTED_QC',
'PH_IN_SITU_TOTAL_QC',
'PH_IN_SITU_TOTAL_dPRES',
'POSITION_QC',
'PRES',
'PRES_ADJUSTED',
'PRES_ADJUSTED_ERROR',
'PRES_ADJUSTED_QC',
'PRES_QC',
'PROFILE_BBP470_QC',
'PROFILE_BBP532_QC',
'PROFILE_BBP700_2_QC',
'PROFILE_BBP700_QC',
'PROFILE_BISULFIDE_QC',
'PROFILE_CNDC_QC',
'PROFILE_CDOM_QC',
'PROFILE_CHLA_QC',
'PROFILE_CP660_QC',
'PROFILE_DOWNWELLING_PAR_QC',
'PROFILE_DOWN_IRRADIANCE380_QC',
'PROFILE_DOWN_IRRADIANCE412_QC',
'PROFILE_DOWN_IRRADIANCE443_QC',
'PROFILE_DOWN_IRRADIANCE490_QC',
'PROFILE_DOWN_IRRADIANCE555_QC',
'PROFILE_DOXY2_QC',
'PROFILE_DOXY_QC',
'PROFILE_NITRATE_QC',
'PROFILE_PH_IN_SITU_TOTAL_QC',
'PROFILE_PRES_QC',
'PROFILE_PSAL_QC',
'PROFILE_TEMP_QC',
'PROFILE_TURBIDITY_QC',
'PROFILE_UP_RADIANCE412_QC',
'PROFILE_UP_RADIANCE443_QC',
'PROFILE_UP_RADIANCE490_QC',
'PROFILE_UP_RADIANCE555_QC',
'PSAL',
'PSAL_ADJUSTED',
'PSAL_ADJUSTED_ERROR',
'PSAL_ADJUSTED_QC',
'PSAL_QC',
'PSAL_dPRES',
'SCIENTIFIC_CALIB_COEFFICIENT',
'SCIENTIFIC_CALIB_COMMENT',
'SCIENTIFIC_CALIB_DATE',
'SCIENTIFIC_CALIB_EQUATION',
'TEMP',
'TEMP_ADJUSTED',
'TEMP_ADJUSTED_ERROR',
'TEMP_ADJUSTED_QC',
'TEMP_QC',
'TEMP_dPRES',
'TURBIDITY',
'TURBIDITY_ADJUSTED',
'TURBIDITY_ADJUSTED_ERROR',
'TURBIDITY_ADJUSTED_QC',
'TURBIDITY_QC',
'TURBIDITY_dPRES',
'UP_RADIANCE412',
'UP_RADIANCE412_ADJUSTED',
'UP_RADIANCE412_ADJUSTED_ERROR',
'UP_RADIANCE412_ADJUSTED_QC',
'UP_RADIANCE412_QC',
'UP_RADIANCE412_dPRES',
'UP_RADIANCE443',
'UP_RADIANCE443_ADJUSTED',
'UP_RADIANCE443_ADJUSTED_ERROR',
'UP_RADIANCE443_ADJUSTED_QC',
'UP_RADIANCE443_QC',
'UP_RADIANCE443_dPRES',
'UP_RADIANCE490',
'UP_RADIANCE490_ADJUSTED',
'UP_RADIANCE490_ADJUSTED_ERROR',
'UP_RADIANCE490_ADJUSTED_QC',
'UP_RADIANCE490_QC',
'UP_RADIANCE490_dPRES',
'UP_RADIANCE555',
'UP_RADIANCE555_ADJUSTED',
'UP_RADIANCE555_ADJUSTED_ERROR',
'UP_RADIANCE555_ADJUSTED_QC',
'UP_RADIANCE555_QC',
'UP_RADIANCE555_dPRES']


def get_argo_synthetic():
    """[summary]

    Returns:
        list: list of argo float ID's that are synthetic
    """
    df = pd.read_csv("argo_synthetic-profile_index.txt",sep=',',skiprows=8)
    #splits float name column into only float ID. aoml/1900722/profiles/SD1900722_001.nc -> daac/1900722/
    syn_file_names = df['file'].str.split('profiles',expand=True)[0].unique()
    return syn_file_names


def get_synthetic_headers(argo_synthetic_list):
    bgc_vars_list = []
    missed_syn_list = []
    for syn_float in tqdm(argo_synthetic_list):
        try:
            float_profile = xr.open_dataset(argo_base_dir + syn_float +  syn_float.split('/')[1] +"_Sprof.nc")#.to_dataframe().reset_index()
            float_vars = list(float_profile.keys())
            bgc_vars_list.extend(float_vars)
        except Exception as e:
            print(e)
            missed_syn_list.append(syn_float)
    bgc_vars_list = np.sort(list(set(bgc_vars_list)))
    return bgc_vars_list, missed_syn_list



def glob_floats_daac(daac):
    base_dir = argo_base_dir + daac + "/"
    float_list = os.listdir(base_dir)
    return float_list

def decode_xarray_bytes(xdf):
    """decodes any b' strings in xarray to improve pandas memory issues"""
    for col in list(xdf):
        if xdf[col].dtype == 'O':
            try:
                xdf[col] = xdf[col].astype(str)
            except:
                xdf[col] = xdf[col].str.decode('cp1252').str.strip()
    return xdf

def rename_bgc_cols(df):
    rename_df = df.rename(columns={'JULD': 'time', 'LATITUDE': 'lat', 'LONGITUDE': 'lon', 'CYCLE_NUMBER': 'cycle'})
    return rename_df

def slow_gc_data_decode(df):
    for col in list(df.select_dtypes([np.object])):
        df[col] = df[col].str.decode('utf-8')
        gc.collect()
    return df

def replace_comm_delimiter(df):
    sci_cols = ['SCIENTIFIC_CALIB_COEFFICIENT','SCIENTIFIC_CALIB_COMMENT', 'SCIENTIFIC_CALIB_EQUATION']
    for col in sci_cols:
        if len(df[col])>0:
            df[col] =df[col].astype(str).str.replace(',',';')
    return df

def reorder_bgc_data(df):
    """Reordered a BGC dataframe to move ST coodinates first and sci equations last

    Args:
        df (Pandas DataFrame): Input ARGO BGC DataFrame

    Returns:
        df (Pandas DataFrame): Reordered DataFrame
    """
    st_col_list = ['time','lat','lon','depth','year','month','week','dayofyear','float_id','cycle']
    st_cols =df[st_col_list]
    non_st_cols = df.drop(st_col_list, axis=1)
    reorder_df = pd.concat([st_cols, non_st_cols], axis=1, sort=False)
    
    sci_col_list = ["SCIENTIFIC_CALIB_COEFFICIENT","SCIENTIFIC_CALIB_COMMENT","SCIENTIFIC_CALIB_DATE","SCIENTIFIC_CALIB_EQUATION"]
    sci_cols =reorder_df[sci_col_list]
    non_sci_cols = reorder_df.drop(sci_col_list, axis=1)
    neworder_df = pd.concat([non_sci_cols,sci_cols], axis=1, sort=False)
    return neworder_df


def add_climatology_cols(df):
    """      
     [year]
      ,[month]
      ,[week]
      ,[dayofyear]

    Args:
        df ([type]): [description]

    Returns:
        [type]: [description]
    """
    return df

def large_xarray_to_multi_parquet(xdf,float_id):
    """This function is for when the xarray to_dataframe method crashes to memory issues"""
    for lvl in tqdm(xdf.N_PARAM.values.tolist()):
        df = xdf.sel(N_PARAM=lvl).to_dataframe()
        df = df.reset_index()
        df = process_chunked_df(df,float_id)
        df.to_parquet(f"temp/{str(lvl)}.parquet",use_deprecated_int96_timestamps=True)

def xarray_to_csv_direct(xdf):
    df = xdf.to_dataframe()
    df = df.reset_index()
    return df

def xarray_to_df(file_path):
    """loads xarray and converts to dataframe. Warning causes memory issues on large datasets"""
    df = xr.open_dataset(file_path).to_dataframe()
    df = df.reset_index()
    return df

def xarray_to_export_csv(file_path):
    df = xr.open_dataset(file_path).to_dataframe().reset_index()
    df.to_csv("temp.csv",index=False)
    gc.collect()

def load_multi_parquet():
    """This function loads all the parquet files saved from the function 'large_xarray_to_multi_parquet' into a pandas dataframe"""
    files = glob.glob('temp/*.parquet')
    df = pd.concat([pd.read_parquet(fp) for fp in files])
    return df
def remove_multi_temp_files():
    shutil.rmtree('temp/')
    os.mkdir('temp/')

def load_xarrayed_csv_as_chunked(chunksize=1000000):
    df_chunk = pd.read_csv(r'temp.csv', chunksize=chunksize)
    return df_chunk

def get_float_id_from_path(float_path):
    float_id = float_path.split("_Sprof")[0].rsplit("/")[-1]
    return float_id

def get_bgc_template_df():
    bgc_template_df = pd.DataFrame(columns=['BBP470','BBP470_ADJUSTED','BBP470_ADJUSTED_ERROR','BBP470_ADJUSTED_QC','BBP470_QC','BBP470_dPRES','BBP532','BBP532_ADJUSTED','BBP532_ADJUSTED_ERROR','BBP532_ADJUSTED_QC','BBP532_QC','BBP532_dPRES','BBP700','BBP700_2','BBP700_2_ADJUSTED','BBP700_2_ADJUSTED_ERROR','BBP700_2_ADJUSTED_QC','BBP700_2_QC','BBP700_2_dPRES','BBP700_ADJUSTED','BBP700_ADJUSTED_ERROR','BBP700_ADJUSTED_QC','BBP700_QC','BBP700_dPRES','BISULFIDE','BISULFIDE_ADJUSTED','BISULFIDE_ADJUSTED_ERROR','BISULFIDE_ADJUSTED_QC','BISULFIDE_dPRES','CNDC','CNDC_ADJUSTED','CNDC_ADJUSTED_ERROR','CNDC_ADJUSTED_QC','CNDC_dPRES','CDOM','CDOM_ADJUSTED','CDOM_ADJUSTED_ERROR','CDOM_ADJUSTED_QC','CDOM_QC','CDOM_dPRES','CHLA','CHLA_ADJUSTED','CHLA_ADJUSTED_ERROR','CHLA_ADJUSTED_QC','CHLA_QC','CHLA_dPRES','CONFIG_MISSION_NUMBER','CP660','CP660_ADJUSTED','CP660_ADJUSTED_ERROR','CP660_ADJUSTED_QC','CP660_QC','CP660_dPRES','CYCLE_NUMBER','DATA_CENTRE','DATA_TYPE','DATE_CREATION','DATE_UPDATE','DIRECTION','DOWNWELLING_PAR','DOWNWELLING_PAR_ADJUSTED','DOWNWELLING_PAR_ADJUSTED_ERROR','DOWNWELLING_PAR_ADJUSTED_QC','DOWNWELLING_PAR_QC','DOWNWELLING_PAR_dPRES','DOWN_IRRADIANCE380','DOWN_IRRADIANCE380_ADJUSTED','DOWN_IRRADIANCE380_ADJUSTED_ERROR','DOWN_IRRADIANCE380_ADJUSTED_QC','DOWN_IRRADIANCE380_QC','DOWN_IRRADIANCE380_dPRES','DOWN_IRRADIANCE412','DOWN_IRRADIANCE412_ADJUSTED','DOWN_IRRADIANCE412_ADJUSTED_ERROR','DOWN_IRRADIANCE412_ADJUSTED_QC','DOWN_IRRADIANCE412_QC','DOWN_IRRADIANCE412_dPRES','DOWN_IRRADIANCE443','DOWN_IRRADIANCE443_ADJUSTED','DOWN_IRRADIANCE443_ADJUSTED_ERROR','DOWN_IRRADIANCE443_ADJUSTED_QC','DOWN_IRRADIANCE443_QC','DOWN_IRRADIANCE443_dPRES','DOWN_IRRADIANCE490','DOWN_IRRADIANCE490_ADJUSTED','DOWN_IRRADIANCE490_ADJUSTED_ERROR','DOWN_IRRADIANCE490_ADJUSTED_QC','DOWN_IRRADIANCE490_QC','DOWN_IRRADIANCE490_dPRES','DOWN_IRRADIANCE555','DOWN_IRRADIANCE555_ADJUSTED','DOWN_IRRADIANCE555_ADJUSTED_ERROR','DOWN_IRRADIANCE555_ADJUSTED_QC','DOWN_IRRADIANCE555_QC','DOWN_IRRADIANCE555_dPRES','DOXY','DOXY2','DOXY2_ADJUSTED','DOXY2_ADJUSTED_ERROR','DOXY2_ADJUSTED_QC','DOXY2_QC','DOXY2_dPRES','DOXY_ADJUSTED','DOXY_ADJUSTED_ERROR','DOXY_ADJUSTED_QC','DOXY_QC','DOXY_dPRES','FIRMWARE_VERSION','FLOAT_SERIAL_NO','FORMAT_VERSION','HANDBOOK_VERSION','JULD','JULD_LOCATION','JULD_QC','LATITUDE','LONGITUDE','NITRATE','NITRATE_ADJUSTED','NITRATE_ADJUSTED_ERROR','NITRATE_ADJUSTED_QC','NITRATE_QC','NITRATE_dPRES','PARAMETER','PARAMETER_DATA_MODE','PH_IN_SITU_TOTAL','PH_IN_SITU_TOTAL_ADJUSTED','PH_IN_SITU_TOTAL_ADJUSTED_ERROR','PH_IN_SITU_TOTAL_ADJUSTED_QC','PH_IN_SITU_TOTAL_QC','PH_IN_SITU_TOTAL_dPRES','PI_NAME','PLATFORM_NUMBER','PLATFORM_TYPE','POSITIONING_SYSTEM','POSITION_QC','PRES','PRES_ADJUSTED','PRES_ADJUSTED_ERROR','PRES_ADJUSTED_QC','PRES_QC','PROFILE_BBP470_QC','PROFILE_BBP532_QC','PROFILE_BBP700_2_QC','PROFILE_BBP700_QC','PROFILE_BISULFIDE_QC','PROFILE_CNDC_QC','PROFILE_CDOM_QC','PROFILE_CHLA_QC','PROFILE_CP660_QC','PROFILE_DOWNWELLING_PAR_QC','PROFILE_DOWN_IRRADIANCE380_QC','PROFILE_DOWN_IRRADIANCE412_QC','PROFILE_DOWN_IRRADIANCE443_QC','PROFILE_DOWN_IRRADIANCE490_QC','PROFILE_DOWN_IRRADIANCE555_QC','PROFILE_DOXY2_QC','PROFILE_DOXY_QC','PROFILE_NITRATE_QC','PROFILE_PH_IN_SITU_TOTAL_QC','PROFILE_PRES_QC','PROFILE_PSAL_QC','PROFILE_TEMP_QC','PROFILE_TURBIDITY_QC','PROFILE_UP_RADIANCE412_QC','PROFILE_UP_RADIANCE443_QC','PROFILE_UP_RADIANCE490_QC','PROFILE_UP_RADIANCE555_QC','PROJECT_NAME','PSAL','PSAL_ADJUSTED','PSAL_ADJUSTED_ERROR','PSAL_ADJUSTED_QC','PSAL_QC','PSAL_dPRES','REFERENCE_DATE_TIME','SCIENTIFIC_CALIB_COEFFICIENT','SCIENTIFIC_CALIB_COMMENT','SCIENTIFIC_CALIB_DATE','SCIENTIFIC_CALIB_EQUATION','STATION_PARAMETERS','TEMP','TEMP_ADJUSTED','TEMP_ADJUSTED_ERROR','TEMP_ADJUSTED_QC','TEMP_QC','TEMP_dPRES','TURBIDITY','TURBIDITY_ADJUSTED','TURBIDITY_ADJUSTED_ERROR','TURBIDITY_ADJUSTED_QC','TURBIDITY_QC','TURBIDITY_dPRES','UP_RADIANCE412','UP_RADIANCE412_ADJUSTED','UP_RADIANCE412_ADJUSTED_ERROR','UP_RADIANCE412_ADJUSTED_QC','UP_RADIANCE412_QC','UP_RADIANCE412_dPRES','UP_RADIANCE443','UP_RADIANCE443_ADJUSTED','UP_RADIANCE443_ADJUSTED_ERROR','UP_RADIANCE443_ADJUSTED_QC','UP_RADIANCE443_QC','UP_RADIANCE443_dPRES','UP_RADIANCE490','UP_RADIANCE490_ADJUSTED','UP_RADIANCE490_ADJUSTED_ERROR','UP_RADIANCE490_ADJUSTED_QC','UP_RADIANCE490_QC','UP_RADIANCE490_dPRES','UP_RADIANCE555','UP_RADIANCE555_ADJUSTED','UP_RADIANCE555_ADJUSTED_ERROR','UP_RADIANCE555_ADJUSTED_QC','UP_RADIANCE555_QC','UP_RADIANCE555_dPRES','WMO_INST_TYPE'])
    return bgc_template_df

def add_cols_to_cleaned_df(df):
    """trims down df to only data columns"""

    core_cols  = ['time','lat','lon','depth','year','month','week','dayofyear','float_id','cycle']
    template_cols = core_cols + bgc_data_columns
    template_df   = pd.DataFrame(columns=template_cols)
    df = template_df.append(df)[template_cols]
    return df

def argo_size_decider(xdf,byte_lim =200000000):
    """ Simple function that gets xarray dataset size and returns bool based on size"""
    fsize = xdf.nbytes
    if fsize > byte_lim:
        ds_large = True
    else:
        ds_large = False
    return ds_large

    
def format_bgc_metadata(df,float_id):
    """Split off metadata columns from argo dataframe, find unique values, add to dictionary

    Args:
        df (Pandas DataFrame): Argo float DataFrame

    Returns:
       bgc_metadata_df pandas DataFrame: DataFrame of kvp for all argo metadata
    """
    mdf = df[bgc_metadata_columns]
    bgc_metadata_dict = {}
    for col in list(mdf):
        bgc_metadata_dict[col] = list(pd.Series(mdf[col].unique()).astype(str).str.strip())
        bgc_metadata_dict[col] = list(pd.Series(mdf[col].unique()).astype(str).str.strip().replace("'",'"'))
    bgc_metadata_dict = json.dumps(bgc_metadata_dict) 
    bgc_metadata_df = pd.DataFrame({"float_id": [float_id], "Metadata_Dict": [bgc_metadata_dict]})
    return bgc_metadata_df

def process_chunked_df(df,float_id):
    """ Inputs raw argo dataframe and cleans and reorders data"""
    df.loc[:, df.columns != 'JULD'] = df.loc[:, df.columns != 'time'].apply(pd.to_numeric, errors='ignore',downcast='signed')

    """adds depth as column"""
    df["depth"] =  df['PRES']

    """adds float ID column from float_path_name"""
    df["float_id"] = int(float_id)

    """rename ST cols"""
    df = rename_bgc_cols(df)

    """drops any invalid ST rows"""
    df = df.dropna(subset=['time', 'lat','lon','depth'])
    """adds climatology day,month,week,doy columns"""
    df = data.add_day_week_month_year_clim(df)
    """reorders bgc_df with ST index leading followed by float_id and cycle"""
    df = reorder_bgc_data(df)
    """strips any whitespace from col values"""
    df = cmn.strip_whitespace_data(df)
    """removes comma delmim from sci cols"""
    df = replace_comm_delimiter(df)
    """removes any inf vals"""
    df = df.replace([np.inf, -np.inf], np.nan) 
    return df




def format_split_bgc(df,float_id, bgc_template_df):
    """calls func that returns DataFrame of metadata"""
    bgc_metadata_df = format_bgc_metadata(df_concat,float_id)

     # """"Adds any missing columns to data dataframe"""
    df_concat = bgc_template_df.append(df_concat)

    """trims down df to only data columns"""
    df_concat = df_concat[bgc_data_columns]

    return float_id, df_concat, bgc_metadata_df


def cleaned_bgc_to_vault_data(float_id,cleaned_bgc_df):
    vault_data_dir = '/data/CMAP Data Submission Dropbox/Simons CMAP/vault/observation/in-situ/float/tblArgoBGC_REP/rep'
    cleaned_bgc_df.to_csv(vault_data_dir + float_id + ".csv",index=False,sep=',')


def cleaned_bgc_to_vault_metadata(float_id,bgc_metadata_df):
    vault_metadata_dir = argo_base_dir + 'vault/metadata/'
    bgc_metadata_df.to_csv(vault_metadata_dir + float_id + ".csv",index=False)

def insert_data_DB(cleaned_bgc_df):
    data.data_df_to_db(cleaned_bgc_df, 'tblArgoBGC_REP', 'Rainier')
    data.data_df_to_db(cleaned_bgc_df, 'tblArgoBGC_REP', 'Mariana')

def insert_metadata_DB(bgc_metadata_df):
    DB.toSQLpandas(bgc_metadata_df, "tblArgo_Metadata", "Rainier")
    DB.toSQLpandas(bgc_metadata_df, "tblArgo_Metadata", "Mariana")



# filelist = glob.glob(vs.collected_data + "ARGO/BGC/" +"**/*.nc",recursive=True)
filelist = glob.glob(argo_base_dir + "BGC/" +"**/*.nc",recursive=True)

def get_diff_DB_flist(filelist):
    flist_ids = set(pd.Series(filelist).str.split("/",expand=True).iloc[:,-1].str.split("_",expand=True)[0].to_list())
    query = """ SELECT DISTINCT(float_id) from tblArgo_Metadata """
    vault_ids = DB.dbRead(query)
    vault_list = set(vault_ids["float_id"].astype(str).to_list())
    diff_set = list(vault_list ^ flist_ids)
    return diff_set

def get_diff_DB_flist_table(filelist):
    flist_ids = set(pd.Series(filelist).str.split("/",expand=True).iloc[:,-1].str.split("_",expand=True)[0].to_list())
    query = """ SELECT DISTINCT(float_id) from tblArgoBGC_Rep """
    vault_ids = DB.dbRead(query,"Rainier")
    vault_list = set(vault_ids["float_id"].astype(str).to_list())
    diff_set = list(vault_list ^ flist_ids)
    return diff_set

# fil = "/home/nrhagen/Documents/CMAP/cmapprocess/ARGO/argo_data_temp/BGC/coriolis/6901766_Sprof.nc"
# # fil = "/home/nrhagen/Documents/CMAP/cmapprocess/ARGO/argo_data_temp/BGC/incois/2902264_Sprof.nc"
# # fil = "/home/nrhagen/Documents/CMAP/cmapprocess/ARGO/argo_data_temp/BGC/incois/2902156_Sprof.nc"
# fil = "/home/nrhagen/Documents/CMAP/cmapprocess/ARGO/argo_data_temp/BGC/incois/2902202_Sprof.nc"
# fil = "/home/nrhagen/Documents/CMAP/cmapprocess/ARGO/argo_data_temp/BGC/coriolis/6901485_Sprof.nc"
# fil = '/data/CMAP Data Submission Dropbox/Simons CMAP/collected_data/ARGO/BGC/meds/4900883_Sprof.nc'





# """ MAIN PROCESSING BLOCK"""
# missed_file_list = []
diff_set = get_diff_DB_flist_table(filelist)


# random.shuffle(filelist)




# ['5903955',
#  '6901174',
#  '6902737',
#  '6901654',
#  '6901182',
#  '7900344',
#  '4900883',
#  '3901498',
#  '6902735',
#  '4900627',
#  '6901180',
#  '6900807',
#  '2901705',
#  '6901181',
#  '6901511',
#  '3901496',
#  '5903679',
#  '6901151',
#  '5903678',
#  '6902671',
#  '6901581',
#  '3901531']




"""break into dataframes, clean, ingest..."""
for fil in tqdm(filelist):
    fil_ID = fil.rsplit("/")[-1].split("_")[0]
    if fil_ID in diff_set:
        print("####")
        print(fil)
        print("####")
        try:
            xdf = xr.open_dataset(fil)
            xdf = decode_xarray_bytes(xdf)
            df = xdf.to_dataframe().reset_index()
            df = process_chunked_df(df,fil_ID)
            df = add_cols_to_cleaned_df(df)
            df = reorder_bgc_data(df)
            cleaned_bgc_to_vault_data(fil_ID,df)
            insert_data_DB(df)
        except Exception as e:
            print(fil)
            print(e)
    else:
        pass
        # print("This float is already in the DB: " + fil_ID)


# fil = '/data/CMAP Data Submission Dropbox/Simons CMAP/collected_data/ARGO/BGC/meds/4900883_Sprof.nc'
# fil_ID = fil.rsplit("/")[-1].split("_")[0]
# xdf = xr.open_dataset(fil)
# xdf = decode_xarray_bytes(xdf)
# df = xdf.to_dataframe().reset_index()

# df = process_chunked_df(df,fil_ID)
# df = add_cols_to_cleaned_df(df)
# df = reorder_bgc_data(df)
# cleaned_bgc_to_vault_data(fil_ID,df)
# insert_data_DB(df)

"""for too large memory floats, this block for addl large metadata"""

# for fil in tqdm(filelist):
#     fil_ID = fil.rsplit("/")[-1].split("_")[0]
#     if fil_ID in diff_set:
#         print("####")
#         print(fil)
#         print("####")
#         try:
#             #1. load xarray and choose which size processing func
#             xdf = xr.open_dataset(fil)
#             xdf = decode_xarray_bytes(xdf)
#             df = xdf.sel(N_PARAM=0).to_dataframe()
#             df = df.reset_index()
#             df = process_chunked_df(df,fil_ID)
#             #2a parse xdf with parquet in chunks
#             metadata_df = format_bgc_metadata(df,fil_ID)
#             #5 ingest df and metadata into DB 
#             insert_data_metadata_DB(df,metadata_df)

#         except Exception as e:
#             print(fil)
#             print(e)
#     else:
#         print("This float is already in the DB: " + fil_ID)











# for fil in tqdm(filelist):
#     fil_ID = fil.rsplit("/")[-1].split("_")[0]
#     if fil_ID in diff_set:
#         try:
#             #1. load xarray and choose which size processing func
#             xdf = xr.open_dataset(fil)
#             xdf = decode_xarray_bytes(xdf)
#             ds_large = argo_size_decider(xdf,byte_lim = 100000000)
#             if ds_large == True:
#                 missed_file_list.append(fil)
#                 large_xarray_to_multi_parquet(xdf,fil_ID)
#                 # df = load_multi_parquet()
#                 vdf = vaex.open('temp/*.parquet')
#                 df = vdf.to_pandas_df()
#                 remove_multi_temp_files()
#                 #2a parse xdf with parquet in chunks
#             else:
#                 #2b parse xdf in single 
#                 df = xarray_to_csv_direct(xdf)
#                 df = process_chunked_df(df,fil_ID)
#                 #3 final cleanup col additions and metadata dataframe
#             metadata_df = format_bgc_metadata(df,fil_ID)
#             df.loc[:, df.columns != 'time'] = df.loc[:, df.columns != 'time'].apply(pd.to_numeric, errors='ignore',downcast='signed')
                    
#             df = add_cols_to_cleaned_df(df)

#             df = reorder_bgc_data(df)
#             #4 save data to vault
#             cleaned_bgc_to_vault(fil_ID,df,metadata_df)
#             #5 ingest df and metadata into DB 
#             insert_data_metadata_DB(df,metadata_df)

#         except Exception as e:
#             print(fil)
#             print(e)
#             missed_file_list.append(fil)
#     else:
#         print("This float is already in the DB: " + fil_ID)
# pd.Series(mfl).to_csv("missed_file_list.csv",index=False)




























# missed_file_list = []

# for fil in tqdm(filelist):
#     fil_ID = fil.rsplit("/")[-1].split("_")[0]
#     if fil_ID in diff_set:
#         try:
#             xdf = xr.open_dataset(fil)
#             xdf = decode_xarray_bytes(xdf)
#             print(fil, ' ingesting...')
#             bgc_template_df = pd.DataFrame(columns=['BBP470','BBP470_ADJUSTED','BBP470_ADJUSTED_ERROR','BBP470_ADJUSTED_QC','BBP470_QC','BBP470_dPRES','BBP532','BBP532_ADJUSTED','BBP532_ADJUSTED_ERROR','BBP532_ADJUSTED_QC','BBP532_QC','BBP532_dPRES','BBP700','BBP700_2','BBP700_2_ADJUSTED','BBP700_2_ADJUSTED_ERROR','BBP700_2_ADJUSTED_QC','BBP700_2_QC','BBP700_2_dPRES','BBP700_ADJUSTED','BBP700_ADJUSTED_ERROR','BBP700_ADJUSTED_QC','BBP700_QC','BBP700_dPRES','BISULFIDE','BISULFIDE_ADJUSTED','BISULFIDE_ADJUSTED_ERROR','BISULFIDE_ADJUSTED_QC','BISULFIDE_dPRES','CNDC','CNDC_ADJUSTED','CNDC_ADJUSTED_ERROR','CNDC_ADJUSTED_QC','CNDC_dPRES','CDOM','CDOM_ADJUSTED','CDOM_ADJUSTED_ERROR','CDOM_ADJUSTED_QC','CDOM_QC','CDOM_dPRES','CHLA','CHLA_ADJUSTED','CHLA_ADJUSTED_ERROR','CHLA_ADJUSTED_QC','CHLA_QC','CHLA_dPRES','CONFIG_MISSION_NUMBER','CP660','CP660_ADJUSTED','CP660_ADJUSTED_ERROR','CP660_ADJUSTED_QC','CP660_QC','CP660_dPRES','CYCLE_NUMBER','DATA_CENTRE','DATA_TYPE','DATE_CREATION','DATE_UPDATE','DIRECTION','DOWNWELLING_PAR','DOWNWELLING_PAR_ADJUSTED','DOWNWELLING_PAR_ADJUSTED_ERROR','DOWNWELLING_PAR_ADJUSTED_QC','DOWNWELLING_PAR_QC','DOWNWELLING_PAR_dPRES','DOWN_IRRADIANCE380','DOWN_IRRADIANCE380_ADJUSTED','DOWN_IRRADIANCE380_ADJUSTED_ERROR','DOWN_IRRADIANCE380_ADJUSTED_QC','DOWN_IRRADIANCE380_QC','DOWN_IRRADIANCE380_dPRES','DOWN_IRRADIANCE412','DOWN_IRRADIANCE412_ADJUSTED','DOWN_IRRADIANCE412_ADJUSTED_ERROR','DOWN_IRRADIANCE412_ADJUSTED_QC','DOWN_IRRADIANCE412_QC','DOWN_IRRADIANCE412_dPRES','DOWN_IRRADIANCE443','DOWN_IRRADIANCE443_ADJUSTED','DOWN_IRRADIANCE443_ADJUSTED_ERROR','DOWN_IRRADIANCE443_ADJUSTED_QC','DOWN_IRRADIANCE443_QC','DOWN_IRRADIANCE443_dPRES','DOWN_IRRADIANCE490','DOWN_IRRADIANCE490_ADJUSTED','DOWN_IRRADIANCE490_ADJUSTED_ERROR','DOWN_IRRADIANCE490_ADJUSTED_QC','DOWN_IRRADIANCE490_QC','DOWN_IRRADIANCE490_dPRES','DOWN_IRRADIANCE555','DOWN_IRRADIANCE555_ADJUSTED','DOWN_IRRADIANCE555_ADJUSTED_ERROR','DOWN_IRRADIANCE555_ADJUSTED_QC','DOWN_IRRADIANCE555_QC','DOWN_IRRADIANCE555_dPRES','DOXY','DOXY2','DOXY2_ADJUSTED','DOXY2_ADJUSTED_ERROR','DOXY2_ADJUSTED_QC','DOXY2_QC','DOXY2_dPRES','DOXY_ADJUSTED','DOXY_ADJUSTED_ERROR','DOXY_ADJUSTED_QC','DOXY_QC','DOXY_dPRES','FIRMWARE_VERSION','FLOAT_SERIAL_NO','FORMAT_VERSION','HANDBOOK_VERSION','JULD','JULD_LOCATION','JULD_QC','LATITUDE','LONGITUDE','NITRATE','NITRATE_ADJUSTED','NITRATE_ADJUSTED_ERROR','NITRATE_ADJUSTED_QC','NITRATE_QC','NITRATE_dPRES','PARAMETER','PARAMETER_DATA_MODE','PH_IN_SITU_TOTAL','PH_IN_SITU_TOTAL_ADJUSTED','PH_IN_SITU_TOTAL_ADJUSTED_ERROR','PH_IN_SITU_TOTAL_ADJUSTED_QC','PH_IN_SITU_TOTAL_QC','PH_IN_SITU_TOTAL_dPRES','PI_NAME','PLATFORM_NUMBER','PLATFORM_TYPE','POSITIONING_SYSTEM','POSITION_QC','PRES','PRES_ADJUSTED','PRES_ADJUSTED_ERROR','PRES_ADJUSTED_QC','PRES_QC','PROFILE_BBP470_QC','PROFILE_BBP532_QC','PROFILE_BBP700_2_QC','PROFILE_BBP700_QC','PROFILE_BISULFIDE_QC','PROFILE_CNDC_QC','PROFILE_CDOM_QC','PROFILE_CHLA_QC','PROFILE_CP660_QC','PROFILE_DOWNWELLING_PAR_QC','PROFILE_DOWN_IRRADIANCE380_QC','PROFILE_DOWN_IRRADIANCE412_QC','PROFILE_DOWN_IRRADIANCE443_QC','PROFILE_DOWN_IRRADIANCE490_QC','PROFILE_DOWN_IRRADIANCE555_QC','PROFILE_DOXY2_QC','PROFILE_DOXY_QC','PROFILE_NITRATE_QC','PROFILE_PH_IN_SITU_TOTAL_QC','PROFILE_PRES_QC','PROFILE_PSAL_QC','PROFILE_TEMP_QC','PROFILE_TURBIDITY_QC','PROFILE_UP_RADIANCE412_QC','PROFILE_UP_RADIANCE443_QC','PROFILE_UP_RADIANCE490_QC','PROFILE_UP_RADIANCE555_QC','PROJECT_NAME','PSAL','PSAL_ADJUSTED','PSAL_ADJUSTED_ERROR','PSAL_ADJUSTED_QC','PSAL_QC','PSAL_dPRES','REFERENCE_DATE_TIME','SCIENTIFIC_CALIB_COEFFICIENT','SCIENTIFIC_CALIB_COMMENT','SCIENTIFIC_CALIB_DATE','SCIENTIFIC_CALIB_EQUATION','STATION_PARAMETERS','TEMP','TEMP_ADJUSTED','TEMP_ADJUSTED_ERROR','TEMP_ADJUSTED_QC','TEMP_QC','TEMP_dPRES','TURBIDITY','TURBIDITY_ADJUSTED','TURBIDITY_ADJUSTED_ERROR','TURBIDITY_ADJUSTED_QC','TURBIDITY_QC','TURBIDITY_dPRES','UP_RADIANCE412','UP_RADIANCE412_ADJUSTED','UP_RADIANCE412_ADJUSTED_ERROR','UP_RADIANCE412_ADJUSTED_QC','UP_RADIANCE412_QC','UP_RADIANCE412_dPRES','UP_RADIANCE443','UP_RADIANCE443_ADJUSTED','UP_RADIANCE443_ADJUSTED_ERROR','UP_RADIANCE443_ADJUSTED_QC','UP_RADIANCE443_QC','UP_RADIANCE443_dPRES','UP_RADIANCE490','UP_RADIANCE490_ADJUSTED','UP_RADIANCE490_ADJUSTED_ERROR','UP_RADIANCE490_ADJUSTED_QC','UP_RADIANCE490_QC','UP_RADIANCE490_dPRES','UP_RADIANCE555','UP_RADIANCE555_ADJUSTED','UP_RADIANCE555_ADJUSTED_ERROR','UP_RADIANCE555_ADJUSTED_QC','UP_RADIANCE555_QC','UP_RADIANCE555_dPRES','WMO_INST_TYPE'])
#             float_id,cleaned_bgc_df,bgc_metadata_df = format_split_bgc(fil, bgc_template_df)
#             cleaned_bgc_to_vault(float_id,cleaned_bgc_df,bgc_metadata_df)
#             insert_data_metadata_DB(cleaned_bgc_df,bgc_metadata_df)
#         except Exception as e:
#             print(fil)
#             print(e)
#             missed_file_list.append(fil)

#     else:
#         print("This float is already in the DB: " + fil_ID)
# pd.Series(mfl).to_csv("missed_file_list.csv",index=False)


# float_id,cleaned_bgc_df,bgc_metadata_df = format_split_bgc(fil, bgc_template_df)








# float_id = fil.split("_Sprof")[0].rsplit("/")[-1]
# xdf = xr.open_dataset(fil)
# df = xdf.to_dataframe().reset_index()
# df.to_csv("temp.csv",index=False)
# df = pd.read_csv("temp.csv")
# df_chunk = pd.read_csv(r'temp.csv', chunksize=1000000)

# def chunk_cleaning(df):
#     print(len(df))
#     print(df.dtypes)
#     pass

# chunk_list = []  # append each chunk df here 

# # Each chunk is in df format
# for df in tqdm(df_chunk):  
#     # perform data filtering 
#     print(df.dtypes)
#     print(len(df))
#     df= slow_gc_data_decode(df)
#     # df.memory_usage(index=True,deep=True).sum() / 1000000000
#     df[list(df)] = df[list(df)].apply(pd.to_numeric, errors='ignore',downcast='signed')
#     #4.5GB
#     # chunk_filter = chunk_cleaning(df)
    
    # # Once the data filtering is done, append the chunk to list
    # chunk_list.append(chunk_filter)
    # break
    
# concat the list into dataframe 
# df_concat = pd.concat(chunk_list)


# gc.collect()
# df = xdf.to_dataframe().reset_index()
# df = df.dropna(axis=1)
# gc.collect()
# df.dropna(how='all', axis=1,inplace=True)



# cleaned_bgc_to_vault(float_id,cleaned_bgc_df,bgc_metadata_df)
# insert_data_metadata_DB(cleaned_bgc_df,bgc_metadata_df)


# diff_set = get_diff_DB_flist(filelist)

# missed_file_list = []
# for fil in tqdm(filelist):
#     fil_ID = fil.rsplit("/")[-1].split("_")[0]
#     if fil_ID in diff_set:
#         try:
#             print(fil, ' ingesting...')
#             bgc_template_df = pd.DataFrame(columns=['BBP470','BBP470_ADJUSTED','BBP470_ADJUSTED_ERROR','BBP470_ADJUSTED_QC','BBP470_QC','BBP470_dPRES','BBP532','BBP532_ADJUSTED','BBP532_ADJUSTED_ERROR','BBP532_ADJUSTED_QC','BBP532_QC','BBP532_dPRES','BBP700','BBP700_2','BBP700_2_ADJUSTED','BBP700_2_ADJUSTED_ERROR','BBP700_2_ADJUSTED_QC','BBP700_2_QC','BBP700_2_dPRES','BBP700_ADJUSTED','BBP700_ADJUSTED_ERROR','BBP700_ADJUSTED_QC','BBP700_QC','BBP700_dPRES','BISULFIDE','BISULFIDE_ADJUSTED','BISULFIDE_ADJUSTED_ERROR','BISULFIDE_ADJUSTED_QC','BISULFIDE_dPRES','CNDC','CNDC_ADJUSTED','CNDC_ADJUSTED_ERROR','CNDC_ADJUSTED_QC','CNDC_dPRES','CDOM','CDOM_ADJUSTED','CDOM_ADJUSTED_ERROR','CDOM_ADJUSTED_QC','CDOM_QC','CDOM_dPRES','CHLA','CHLA_ADJUSTED','CHLA_ADJUSTED_ERROR','CHLA_ADJUSTED_QC','CHLA_QC','CHLA_dPRES','CONFIG_MISSION_NUMBER','CP660','CP660_ADJUSTED','CP660_ADJUSTED_ERROR','CP660_ADJUSTED_QC','CP660_QC','CP660_dPRES','CYCLE_NUMBER','DATA_CENTRE','DATA_TYPE','DATE_CREATION','DATE_UPDATE','DIRECTION','DOWNWELLING_PAR','DOWNWELLING_PAR_ADJUSTED','DOWNWELLING_PAR_ADJUSTED_ERROR','DOWNWELLING_PAR_ADJUSTED_QC','DOWNWELLING_PAR_QC','DOWNWELLING_PAR_dPRES','DOWN_IRRADIANCE380','DOWN_IRRADIANCE380_ADJUSTED','DOWN_IRRADIANCE380_ADJUSTED_ERROR','DOWN_IRRADIANCE380_ADJUSTED_QC','DOWN_IRRADIANCE380_QC','DOWN_IRRADIANCE380_dPRES','DOWN_IRRADIANCE412','DOWN_IRRADIANCE412_ADJUSTED','DOWN_IRRADIANCE412_ADJUSTED_ERROR','DOWN_IRRADIANCE412_ADJUSTED_QC','DOWN_IRRADIANCE412_QC','DOWN_IRRADIANCE412_dPRES','DOWN_IRRADIANCE443','DOWN_IRRADIANCE443_ADJUSTED','DOWN_IRRADIANCE443_ADJUSTED_ERROR','DOWN_IRRADIANCE443_ADJUSTED_QC','DOWN_IRRADIANCE443_QC','DOWN_IRRADIANCE443_dPRES','DOWN_IRRADIANCE490','DOWN_IRRADIANCE490_ADJUSTED','DOWN_IRRADIANCE490_ADJUSTED_ERROR','DOWN_IRRADIANCE490_ADJUSTED_QC','DOWN_IRRADIANCE490_QC','DOWN_IRRADIANCE490_dPRES','DOWN_IRRADIANCE555','DOWN_IRRADIANCE555_ADJUSTED','DOWN_IRRADIANCE555_ADJUSTED_ERROR','DOWN_IRRADIANCE555_ADJUSTED_QC','DOWN_IRRADIANCE555_QC','DOWN_IRRADIANCE555_dPRES','DOXY','DOXY2','DOXY2_ADJUSTED','DOXY2_ADJUSTED_ERROR','DOXY2_ADJUSTED_QC','DOXY2_QC','DOXY2_dPRES','DOXY_ADJUSTED','DOXY_ADJUSTED_ERROR','DOXY_ADJUSTED_QC','DOXY_QC','DOXY_dPRES','FIRMWARE_VERSION','FLOAT_SERIAL_NO','FORMAT_VERSION','HANDBOOK_VERSION','JULD','JULD_LOCATION','JULD_QC','LATITUDE','LONGITUDE','NITRATE','NITRATE_ADJUSTED','NITRATE_ADJUSTED_ERROR','NITRATE_ADJUSTED_QC','NITRATE_QC','NITRATE_dPRES','PARAMETER','PARAMETER_DATA_MODE','PH_IN_SITU_TOTAL','PH_IN_SITU_TOTAL_ADJUSTED','PH_IN_SITU_TOTAL_ADJUSTED_ERROR','PH_IN_SITU_TOTAL_ADJUSTED_QC','PH_IN_SITU_TOTAL_QC','PH_IN_SITU_TOTAL_dPRES','PI_NAME','PLATFORM_NUMBER','PLATFORM_TYPE','POSITIONING_SYSTEM','POSITION_QC','PRES','PRES_ADJUSTED','PRES_ADJUSTED_ERROR','PRES_ADJUSTED_QC','PRES_QC','PROFILE_BBP470_QC','PROFILE_BBP532_QC','PROFILE_BBP700_2_QC','PROFILE_BBP700_QC','PROFILE_BISULFIDE_QC','PROFILE_CNDC_QC','PROFILE_CDOM_QC','PROFILE_CHLA_QC','PROFILE_CP660_QC','PROFILE_DOWNWELLING_PAR_QC','PROFILE_DOWN_IRRADIANCE380_QC','PROFILE_DOWN_IRRADIANCE412_QC','PROFILE_DOWN_IRRADIANCE443_QC','PROFILE_DOWN_IRRADIANCE490_QC','PROFILE_DOWN_IRRADIANCE555_QC','PROFILE_DOXY2_QC','PROFILE_DOXY_QC','PROFILE_NITRATE_QC','PROFILE_PH_IN_SITU_TOTAL_QC','PROFILE_PRES_QC','PROFILE_PSAL_QC','PROFILE_TEMP_QC','PROFILE_TURBIDITY_QC','PROFILE_UP_RADIANCE412_QC','PROFILE_UP_RADIANCE443_QC','PROFILE_UP_RADIANCE490_QC','PROFILE_UP_RADIANCE555_QC','PROJECT_NAME','PSAL','PSAL_ADJUSTED','PSAL_ADJUSTED_ERROR','PSAL_ADJUSTED_QC','PSAL_QC','PSAL_dPRES','REFERENCE_DATE_TIME','SCIENTIFIC_CALIB_COEFFICIENT','SCIENTIFIC_CALIB_COMMENT','SCIENTIFIC_CALIB_DATE','SCIENTIFIC_CALIB_EQUATION','STATION_PARAMETERS','TEMP','TEMP_ADJUSTED','TEMP_ADJUSTED_ERROR','TEMP_ADJUSTED_QC','TEMP_QC','TEMP_dPRES','TURBIDITY','TURBIDITY_ADJUSTED','TURBIDITY_ADJUSTED_ERROR','TURBIDITY_ADJUSTED_QC','TURBIDITY_QC','TURBIDITY_dPRES','UP_RADIANCE412','UP_RADIANCE412_ADJUSTED','UP_RADIANCE412_ADJUSTED_ERROR','UP_RADIANCE412_ADJUSTED_QC','UP_RADIANCE412_QC','UP_RADIANCE412_dPRES','UP_RADIANCE443','UP_RADIANCE443_ADJUSTED','UP_RADIANCE443_ADJUSTED_ERROR','UP_RADIANCE443_ADJUSTED_QC','UP_RADIANCE443_QC','UP_RADIANCE443_dPRES','UP_RADIANCE490','UP_RADIANCE490_ADJUSTED','UP_RADIANCE490_ADJUSTED_ERROR','UP_RADIANCE490_ADJUSTED_QC','UP_RADIANCE490_QC','UP_RADIANCE490_dPRES','UP_RADIANCE555','UP_RADIANCE555_ADJUSTED','UP_RADIANCE555_ADJUSTED_ERROR','UP_RADIANCE555_ADJUSTED_QC','UP_RADIANCE555_QC','UP_RADIANCE555_dPRES','WMO_INST_TYPE'])
#             float_id,cleaned_bgc_df,bgc_metadata_df = format_split_bgc(fil, bgc_template_df)
#             cleaned_bgc_to_vault(float_id,cleaned_bgc_df,bgc_metadata_df)
#             insert_data_metadata_DB(cleaned_bgc_df,bgc_metadata_df)
#         except Exception as e:
#             print(fil)
#             print(e)
#             missed_file_list.append(fil)

#     else:
#         print("This float is already in the DB: " + fil_ID)
# pd.Series(mfl).to_csv("missed_file_list.csv",index=False)



























# bgc_template_df = pd.DataFrame(columns=['BBP470','BBP470_ADJUSTED','BBP470_ADJUSTED_ERROR','BBP470_ADJUSTED_QC','BBP470_QC','BBP470_dPRES','BBP532','BBP532_ADJUSTED','BBP532_ADJUSTED_ERROR','BBP532_ADJUSTED_QC','BBP532_QC','BBP532_dPRES','BBP700','BBP700_2','BBP700_2_ADJUSTED','BBP700_2_ADJUSTED_ERROR','BBP700_2_ADJUSTED_QC','BBP700_2_QC','BBP700_2_dPRES','BBP700_ADJUSTED','BBP700_ADJUSTED_ERROR','BBP700_ADJUSTED_QC','BBP700_QC','BBP700_dPRES','CDOM','CDOM_ADJUSTED','CDOM_ADJUSTED_ERROR','CDOM_ADJUSTED_QC','CDOM_QC','CDOM_dPRES','CHLA','CHLA_ADJUSTED','CHLA_ADJUSTED_ERROR','CHLA_ADJUSTED_QC','CHLA_QC','CHLA_dPRES','CONFIG_MISSION_NUMBER','CP660','CP660_ADJUSTED','CP660_ADJUSTED_ERROR','CP660_ADJUSTED_QC','CP660_QC','CP660_dPRES','CYCLE_NUMBER','DATA_CENTRE','DATA_TYPE','DATE_CREATION','DATE_UPDATE','DIRECTION','DOWNWELLING_PAR','DOWNWELLING_PAR_ADJUSTED','DOWNWELLING_PAR_ADJUSTED_ERROR','DOWNWELLING_PAR_ADJUSTED_QC','DOWNWELLING_PAR_QC','DOWNWELLING_PAR_dPRES','DOWN_IRRADIANCE380','DOWN_IRRADIANCE380_ADJUSTED','DOWN_IRRADIANCE380_ADJUSTED_ERROR','DOWN_IRRADIANCE380_ADJUSTED_QC','DOWN_IRRADIANCE380_QC','DOWN_IRRADIANCE380_dPRES','DOWN_IRRADIANCE412','DOWN_IRRADIANCE412_ADJUSTED','DOWN_IRRADIANCE412_ADJUSTED_ERROR','DOWN_IRRADIANCE412_ADJUSTED_QC','DOWN_IRRADIANCE412_QC','DOWN_IRRADIANCE412_dPRES','DOWN_IRRADIANCE443','DOWN_IRRADIANCE443_ADJUSTED','DOWN_IRRADIANCE443_ADJUSTED_ERROR','DOWN_IRRADIANCE443_ADJUSTED_QC','DOWN_IRRADIANCE443_QC','DOWN_IRRADIANCE443_dPRES','DOWN_IRRADIANCE490','DOWN_IRRADIANCE490_ADJUSTED','DOWN_IRRADIANCE490_ADJUSTED_ERROR','DOWN_IRRADIANCE490_ADJUSTED_QC','DOWN_IRRADIANCE490_QC','DOWN_IRRADIANCE490_dPRES','DOWN_IRRADIANCE555','DOWN_IRRADIANCE555_ADJUSTED','DOWN_IRRADIANCE555_ADJUSTED_ERROR','DOWN_IRRADIANCE555_ADJUSTED_QC','DOWN_IRRADIANCE555_QC','DOWN_IRRADIANCE555_dPRES','DOXY','DOXY2','DOXY2_ADJUSTED','DOXY2_ADJUSTED_ERROR','DOXY2_ADJUSTED_QC','DOXY2_QC','DOXY2_dPRES','DOXY_ADJUSTED','DOXY_ADJUSTED_ERROR','DOXY_ADJUSTED_QC','DOXY_QC','DOXY_dPRES','FIRMWARE_VERSION','FLOAT_SERIAL_NO','FORMAT_VERSION','HANDBOOK_VERSION','JULD','JULD_LOCATION','JULD_QC','LATITUDE','LONGITUDE','NITRATE','NITRATE_ADJUSTED','NITRATE_ADJUSTED_ERROR','NITRATE_ADJUSTED_QC','NITRATE_QC','NITRATE_dPRES','PARAMETER','PARAMETER_DATA_MODE','PH_IN_SITU_TOTAL','PH_IN_SITU_TOTAL_ADJUSTED','PH_IN_SITU_TOTAL_ADJUSTED_ERROR','PH_IN_SITU_TOTAL_ADJUSTED_QC','PH_IN_SITU_TOTAL_QC','PH_IN_SITU_TOTAL_dPRES','PI_NAME','PLATFORM_NUMBER','PLATFORM_TYPE','POSITIONING_SYSTEM','POSITION_QC','PRES','PRES_ADJUSTED','PRES_ADJUSTED_ERROR','PRES_ADJUSTED_QC','PRES_QC','PROFILE_BBP470_QC','PROFILE_BBP532_QC','PROFILE_BBP700_2_QC','PROFILE_BBP700_QC','PROFILE_CDOM_QC','PROFILE_CHLA_QC','PROFILE_CP660_QC','PROFILE_DOWNWELLING_PAR_QC','PROFILE_DOWN_IRRADIANCE380_QC','PROFILE_DOWN_IRRADIANCE412_QC','PROFILE_DOWN_IRRADIANCE443_QC','PROFILE_DOWN_IRRADIANCE490_QC','PROFILE_DOWN_IRRADIANCE555_QC','PROFILE_DOXY2_QC','PROFILE_DOXY_QC','PROFILE_NITRATE_QC','PROFILE_PH_IN_SITU_TOTAL_QC','PROFILE_PRES_QC','PROFILE_PSAL_QC','PROFILE_TEMP_QC','PROFILE_TURBIDITY_QC','PROFILE_UP_RADIANCE412_QC','PROFILE_UP_RADIANCE443_QC','PROFILE_UP_RADIANCE490_QC','PROFILE_UP_RADIANCE555_QC','PROJECT_NAME','PSAL','PSAL_ADJUSTED','PSAL_ADJUSTED_ERROR','PSAL_ADJUSTED_QC','PSAL_QC','PSAL_dPRES','REFERENCE_DATE_TIME','SCIENTIFIC_CALIB_COEFFICIENT','SCIENTIFIC_CALIB_COMMENT','SCIENTIFIC_CALIB_DATE','SCIENTIFIC_CALIB_EQUATION','STATION_PARAMETERS','TEMP','TEMP_ADJUSTED','TEMP_ADJUSTED_ERROR','TEMP_ADJUSTED_QC','TEMP_QC','TEMP_dPRES','TURBIDITY','TURBIDITY_ADJUSTED','TURBIDITY_ADJUSTED_ERROR','TURBIDITY_ADJUSTED_QC','TURBIDITY_QC','TURBIDITY_dPRES','UP_RADIANCE412','UP_RADIANCE412_ADJUSTED','UP_RADIANCE412_ADJUSTED_ERROR','UP_RADIANCE412_ADJUSTED_QC','UP_RADIANCE412_QC','UP_RADIANCE412_dPRES','UP_RADIANCE443','UP_RADIANCE443_ADJUSTED','UP_RADIANCE443_ADJUSTED_ERROR','UP_RADIANCE443_ADJUSTED_QC','UP_RADIANCE443_QC','UP_RADIANCE443_dPRES','UP_RADIANCE490','UP_RADIANCE490_ADJUSTED','UP_RADIANCE490_ADJUSTED_ERROR','UP_RADIANCE490_ADJUSTED_QC','UP_RADIANCE490_QC','UP_RADIANCE490_dPRES','UP_RADIANCE555','UP_RADIANCE555_ADJUSTED','UP_RADIANCE555_ADJUSTED_ERROR','UP_RADIANCE555_ADJUSTED_QC','UP_RADIANCE555_QC','UP_RADIANCE555_dPRES','WMO_INST_TYPE'])
# syn_float = get_argo_synthetic()[0]
# float_id,cleaned_bgc_df,bgc_metadata_df = format_split_bgc(syn_float, bgc_template_df)
# cleaned_bgc_to_vault(float_id,cleaned_bgc_df,bgc_metadata_df)
# insert_data_metadata_in_vault(cleaned_bgc_df,bgc_metadata_df)



""" for every float in directory:
    import float into memory, transfer to vault, insert into DB, insert float_metadata

"""
# base_path = cmn.vault_struct_retrieval('float') +'tblArgoBGC_REP/'
# data_ext = 'rep/{float_id}.csv'.format(float_id = float_id)
# data.data_df_to_db(cleaned_bgc_df, 'tblArgoBGC_REP', 'Rainier',clean_data_df_flag=True)






# def clean_argo_df(df,float_ID):
#     """This function cleans and renames variables in the raw argo dataframe

#     Args:
#         df (Pandas DataFrame): Input raw argo dataframe

#     Returns:
#         df (Pandas DataFrame): Output cleaned argo dataframe
#     """
#     # df = df[['direction','data_centre','data_mode','juld','juld_qc','juld_location','latitude','longitude','position_qc','pres','psal','temp','pres_qc','psal_qc','temp_qc','pres_adjusted','psal_adjusted','temp_adjusted','pres_adjusted_qc','psal_adjusted_qc','temp_adjusted_qc','pres_adjusted_error','psal_adjusted_error','temp_adjusted_error']]
#     df["float_id"] = float_ID
#     df = add_depth_columns(df,"pres")
    
#     # 1.   trim down df columns
#     # 1.5  add float_ID col, calculate depth from pressure 
#     # 2.   rename columns to match
#     # 3.   reorder columsn to match
#     # 4.   insert any missing columns
#     # df = df[['cycle_number','direction','data_centre','data_mode','juld','juld_qc','juld_location','latitude','longitude','position_qc','pres','psal','temp','pres_qc','psal_qc','temp_qc','pres_adjusted','psal_adjusted','temp_adjusted','pres_adjusted_qc','psal_adjusted_qc','temp_adjusted_qc','pres_adjusted_error','psal_adjusted_error','temp_adjusted_error']]
#     df.rename()
#     return df


# def compile_profiles(daac,float_ID):
#     float_profiles = glob.glob(argo_base_dir + daac + "/" + float_ID + "/" + "profiles/*.nc")
#     master_df = pd.DataFrame(columns = ['N_CALIB','N_HISTORY','N_LEVELS','N_PARAM','N_PROF','DATA_TYPE','FORMAT_VERSION','HANDBOOK_VERSION','REFERENCE_DATE_TIME','DATE_CREATION','DATE_UPDATE','PLATFORM_NUMBER','PROJECT_NAME','PI_NAME','STATION_PARAMETERS','CYCLE_NUMBER','DIRECTION','DATA_CENTRE','DC_REFERENCE','DATA_STATE_INDICATOR','DATA_MODE','PLATFORM_TYPE','FLOAT_SERIAL_NO','FIRMWARE_VERSION','WMO_INST_TYPE','JULD','JULD_QC','JULD_LOCATION','LATITUDE','LONGITUDE','POSITION_QC','POSITIONING_SYSTEM','VERTICAL_SAMPLING_SCHEME','CONFIG_MISSION_NUMBER','PROFILE_PRES_QC','PROFILE_PSAL_QC','PROFILE_TEMP_QC','PRES','PSAL','TEMP','PRES_QC','PSAL_QC','TEMP_QC','PRES_ADJUSTED','PSAL_ADJUSTED','TEMP_ADJUSTED','PRES_ADJUSTED_QC','PSAL_ADJUSTED_QC','TEMP_ADJUSTED_QC','PRES_ADJUSTED_ERROR','PSAL_ADJUSTED_ERROR','TEMP_ADJUSTED_ERROR','PARAMETER','SCIENTIFIC_CALIB_EQUATION','SCIENTIFIC_CALIB_COEFFICIENT','SCIENTIFIC_CALIB_COMMENT','SCIENTIFIC_CALIB_DATE','HISTORY_INSTITUTION','HISTORY_STEP','HISTORY_SOFTWARE','HISTORY_SOFTWARE_RELEASE','HISTORY_REFERENCE','HISTORY_DATE','HISTORY_ACTION','HISTORY_PARAMETER','HISTORY_START_PRES','HISTORY_STOP_PRES','HISTORY_PREVIOUS_VALUE','HISTORY_QCTEST'])
#     for argo_float in float_profiles:
#         #clean df here
#         df = xr.open_dataset(argo_float).to_dataframe().reset_index()
#         master_df = master_df.append(df)
#     return master_df



# compile_float = '2902297'
# daac = 'incois'
# # compile_df = compile_profiles(daac,compile_float)

# df = xr.open_dataset(argo_base_dir + daac + "/" + compile_float +"/" +  compile_float +"_prof.nc").to_dataframe().reset_index()
# xdf = xr.open_dataset(argo_base_dir + daac + "/" + compile_float +"/" +  compile_float +"_Sprof.nc")




# def get_unique_cols(argo_synthetic_list):
#     counter=10
#     ucol_df = pd.DataFrame(columns=['float_ID','ucols'])
#     float_id_list = []
#     ucols_list = []
#     for syn_float in tqdm(argo_synthetic_list[0:10]):
#         df = xr.open_dataset(argo_base_dir + syn_float +  syn_float.split('/')[1] +"_Sprof.nc").to_dataframe().reset_index()
#         df = data.decode_df_columns(df)
#         print(df.PI_NAME.unique())
#     #     uniqucols = []
#     #     for val in list(df):
#     #         numunique =  len(df[val].unique())
#     #         if numunique == 1:
#     #             uniqucols.append(val)
#     #     float_id_list.append(syn_float)
#     #     ucols_list.append(uniqucols)
#     #     counter += 1
#     #     if counter == 10:
#     #         break
#     # ucol_df.append(float_id_list,ucols_list)
#     return ucol_df
# ucol_df = get_unique_cols(argo_synthetic_list)

