

import urllib
import xarray as xr
import pandas as pd
import modin.pandas as pd
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


argo_base_dir = vs.collected_data + "ARGO/"
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





