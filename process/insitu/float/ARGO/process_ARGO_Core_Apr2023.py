import sys
import os
import pandas as pd
pd.set_option('max_columns', None)
pd.set_option('display.expand_frame_repr', True)
import numpy as np
import xarray as xr
import seawater as sw

from tqdm import tqdm
import glob
import shutil

sys.path.append("cmapdata/ingest")
sys.path.append("ingest")

import vault_structure as vs
import credentials as cr
import DB
import common as cmn
import data
import stats


#####################################
### File Structure and Unzipping  ###
#####################################
#####################################

tbl = 'tblArgoCore_REP_Apr2023'
argo_base_path = vs.float_dir + 'tblArgoBGC_REP_Apr2023/raw/ARGO/202304-ArgoData/dac/'
argo_rep_path = f'{vs.float_dir}{tbl}/rep/'
meta_path =  f'{vs.float_dir}{tbl}/metadata/'


# def unzip_and_organize_Core():
#     # 16189 files
#     vs.makedir(argo_base_path + "Core_subset/")
#     os.chdir(argo_base_path)
#     for daac in tqdm(daac_list):
#         os.system(
#             f"""tar -xvf {daac}_core.tar.gz -C Core_subset/ --transform='s/.*\///' --wildcards --no-anchored '*_prof*'"""
#         )

#####################################
### Cleaning and Processing Block ###
#####################################
#####################################

def rename_cols(df):
    rename_df = df.rename(
        columns={
            "JULD": "time",
            "LATITUDE": "lat",
            "LONGITUDE": "lon",
            "CYCLE_NUMBER": "cycle",
            "PLATFORM_NUMBER": "float_id",
        }
    )
    return rename_df


def core_reorder_and_add_missing_cols(df):
    missing_cols = set(core_cols) ^ set(list(df))
    df = df.reindex(columns=[*df.columns.tolist(), *missing_cols]).reindex(
        columns=core_cols
    )
    return df

def set_core_dtypes(df):
    for c in df.columns.to_list():
        if c in ['time','JULD_LOCATION']:
            df[c] = pd.to_datetime(df[c].astype(str), format="%Y-%m-%d %H:%M:%S").astype("datetime64[s]")           
        elif c in ["DIRECTION","DATA_MODE", "DATA_CENTRE"] or "_QC" in c:
            df[c] = df[c].astype(str)
        elif c in ["float_id", "cycle"]:
            df[c] = df[c].astype(int) 
        else: 
            df[c] = df[c].astype(np.float64)   
    return df


def get_Core_flist():
    Core_flist = glob.glob(argo_base_path + f"Core_subset/*.nc")
    return Core_flist

Core_flist = get_Core_flist()
len(Core_flist)


core_cols = [
    "time",
    "lat",
    "lon",
    "depth",
    "year",
    "month",
    "week",
    "dayofyear",
    "float_id",
    "cycle",
    "POSITION_QC",
    "DIRECTION",
    "DATA_MODE",
    "DATA_CENTRE",
    "JULD_QC",
    "JULD_LOCATION",
    "PROFILE_PRES_QC",
    "PROFILE_TEMP_QC",
    "PROFILE_PSAL_QC",
    "PRES",
    "PRES_QC",
    "PRES_ADJUSTED",
    "PRES_ADJUSTED_QC",
    "PRES_ADJUSTED_ERROR",
    "TEMP",
    "TEMP_QC",
    "TEMP_ADJUSTED",
    "TEMP_ADJUSTED_QC",
    "TEMP_ADJUSTED_ERROR",
    "PSAL",
    "PSAL_QC",
    "PSAL_ADJUSTED",
    "PSAL_ADJUSTED_QC",
    "PSAL_ADJUSTED_ERROR",
]


um_cols = [
            "PLATFORM_NUMBER",
            "DATA_TYPE",
            "FORMAT_VERSION",
            "HANDBOOK_VERSION",
            "REFERENCE_DATE_TIME",
            "DATE_CREATION",
            "DATE_UPDATE",
            "PROJECT_NAME",
            "PI_NAME",
            "STATION_PARAMETERS",
            #"DC_REFERENCE", ##Unique identifier of the profile in the data centre. not unique across data centres
            "DATA_STATE_INDICATOR", 
            "PLATFORM_TYPE",
            "FLOAT_SERIAL_NO",
            "FIRMWARE_VERSION",
            "WMO_INST_TYPE",
            "POSITIONING_SYSTEM",
            "VERTICAL_SAMPLING_SCHEME",
            "CONFIG_MISSION_NUMBER",
            "PARAMETER",
            "HISTORY_INSTITUTION",
            "HISTORY_STEP",
            "HISTORY_SOFTWARE",
            "HISTORY_SOFTWARE_RELEASE",
            "HISTORY_REFERENCE",
            "HISTORY_DATE",
            "HISTORY_ACTION",
            "HISTORY_PARAMETER",
            "HISTORY_START_PRES",
            "HISTORY_STOP_PRES",
            "HISTORY_PREVIOUS_VALUE",
            "HISTORY_QCTEST",
        ]


sci_cols = [
            "PARAMETER",
            "SCIENTIFIC_CALIB_EQUATION",
            "SCIENTIFIC_CALIB_COEFFICIENT",
            "SCIENTIFIC_CALIB_COMMENT",
            "SCIENTIFIC_CALIB_DATE",
]


# fname = pathlib.Path(fil)
# ctime = datetime.datetime.fromtimestamp(fname.stat().st_ctime, tz=datetime.timezone.utc)

# fil = '/data/CMAP Data Submission Dropbox/Simons CMAP/vault/observation/in-situ/float/tblArgoBGC_REP_Apr2023/raw/ARGO/202304-ArgoData/dac/Core_subset/5905229_prof.nc'
#5905229
# Core_flist_sub = Core_flist[0:2000]
# Core_flist_sub = Core_flist[2000:4000]
# Core_flist_sub = Core_flist[4000:6000]
# Core_flist_sub = Core_flist[6000:8000]
# Core_flist_sub = Core_flist[8000:10000]
# Core_flist_sub = Core_flist[10000:12000]
# Core_flist_sub = Core_flist[12000:14000]
# Core_flist_sub = Core_flist[14000:16000]
# Core_flist_sub = Core_flist[16000:]
# Core_flist_sub = ['/data/CMAP Data Submission Dropbox/Simons CMAP/vault/observation/in-situ/float/tblArgoBGC_REP_Apr2023/raw/ARGO/202304-ArgoData/dac/Core_subset/5905229_prof.nc','/data/CMAP Data Submission Dropbox/Simons CMAP/vault/observation/in-situ/float/tblArgoBGC_REP_Apr2023/raw/ARGO/202304-ArgoData/dac/Core_subset/7900344_prof.nc','/data/CMAP Data Submission Dropbox/Simons CMAP/vault/observation/in-situ/float/tblArgoBGC_REP_Apr2023/raw/ARGO/202304-ArgoData/dac/Core_subset/7900602_prof.nc','/data/CMAP Data Submission Dropbox/Simons CMAP/vault/observation/in-situ/float/tblArgoBGC_REP_Apr2023/raw/ARGO/202304-ArgoData/dac/Core_subset/5906555_prof.nc']

def process_core(Core_flist_sub):
    for fil in tqdm(Core_flist_sub):
        # open xarray
        xdf = xr.open_dataset(fil, mask_and_scale=True)
        # convert netcdf binary
        xdf = cmn.decode_xarray_bytes(xdf)
        # xum = xdf.drop([x for x in list(xdf.keys()) if x not in um_cols])
        # xum = cmn.decode_xarray_bytes(xum)
        # scium = xdf.drop([x for x in list(xdf.keys()) if x not in sci_cols])  
        # scium = cmn.decode_xarray_bytes(scium) 
        # df_sci = scium.to_dataframe().reset_index()  
        # df_sci = df_sci.drop(["N_CALIB", "N_PARAM","N_PROF"], axis=1)
        # df_um = pd.DataFrame(columns=['name','attr','value'])
        # for x in xum.data_vars:
        #     if xum.data_vars[x].size > 0:
        #         d = {'name':[x],'attr':[xum.data_vars[x].attrs],'value':[np.unique(xum.data_vars[x])]}
        #         temp= pd.DataFrame(data=d)
        #         df_um = df_um.append(temp)
        # ## Exports for unstructured metadata
        # with pd.ExcelWriter(f"{meta_path}unst_meta_{os.path.basename(fil).strip('.nc')}.xlsx") as writer:
        #     df_um.to_excel(writer, sheet_name="unst_meta",index=False)  
        #     df_sci.to_excel(writer, sheet_name="sci_calib",index=False)    
        # drop ex cols from xarray
        xdf = xdf.drop(
            [
                "DATA_TYPE",
                "FORMAT_VERSION",
                "HANDBOOK_VERSION",
                "REFERENCE_DATE_TIME",
                "DATE_CREATION",
                "DATE_UPDATE",
                "PROJECT_NAME",
                "PI_NAME",
                "STATION_PARAMETERS",
                "DC_REFERENCE",
                "DATA_STATE_INDICATOR", 
                "PLATFORM_TYPE",
                "FLOAT_SERIAL_NO",
                "FIRMWARE_VERSION",
                "WMO_INST_TYPE",
                "POSITIONING_SYSTEM",
                "VERTICAL_SAMPLING_SCHEME",
                "CONFIG_MISSION_NUMBER",
                "PARAMETER",
                "SCIENTIFIC_CALIB_EQUATION",
                "SCIENTIFIC_CALIB_COEFFICIENT",
                "SCIENTIFIC_CALIB_COMMENT",
                "SCIENTIFIC_CALIB_DATE",
                "HISTORY_INSTITUTION",
                "HISTORY_STEP",
                "HISTORY_SOFTWARE",
                "HISTORY_SOFTWARE_RELEASE",
                "HISTORY_REFERENCE",
                "HISTORY_DATE",
                "HISTORY_ACTION",
                "HISTORY_PARAMETER",
                "HISTORY_START_PRES",
                "HISTORY_STOP_PRES",
                "HISTORY_PREVIOUS_VALUE",
                "HISTORY_QCTEST",
            ],
            errors="ignore",
        )
        for c in list(xdf.keys()):
            if c not in core_cols and c not in ['PLATFORM_NUMBER','CYCLE_NUMBER','JULD','LATITUDE','LONGITUDE','MTIME','TILT']:
                print(f"New column needs to be added: {c}")
                print(f"{os.path.basename(fil).strip('.nc')}")
                sys.exit()
        # xdf to df, reset index
        df = xdf.to_dataframe().reset_index()
        # adds depth as column, calculated by seawater library
        df.reset_index(level=0, inplace=True)
        ##### CHANGE PRES TO FLOAT64
        df['PRES']=df['PRES'].astype(np.float64)
        depth_df = pd.DataFrame(sw.dpth(df['PRES'], df['LATITUDE']))
        depth_df.reset_index(level=0, inplace=True)
        depth_df.columns=['index','depth']
        df = pd.merge(df,depth_df, how='left', on='index')
        df = df.loc[df['depth'] >= 0 ]
        # df["depth"] = df["PRES"]
        # drop ex metadata cols
        df = df.drop(["N_LEVELS", "N_PROF","index"], axis=1)
        # rename ST cols
        df = rename_cols(df)
        # formats time col
        # df["time"] = pd.to_datetime(
        #     df["time"].astype(str), format="%Y-%m-%d %H:%M:%S"
        # ).astype("datetime64[s]")
        # df["JULD_LOCATION"] = pd.to_datetime(
        #     df["JULD_LOCATION"].astype(str), format="%Y-%m-%d %H:%M:%S"
        # ).astype("datetime64[s]")
        # adds any missing columns and reorders
        df = core_reorder_and_add_missing_cols(df)
        # drops any invalid ST rows"""
        df = df.dropna(subset=["time", "lat", "lon", "depth"])
        ## Mohammad requested to keep all data in lieu of dropping unique constraint
        # df = df.drop_duplicates(subset=["time", "lat", "lon", "depth"], keep="first")
        df.fillna(999999.9, inplace=True)
        df = df.drop_duplicates(keep="first")
        df = df.replace(999999.9, np.nan)
        # sort ST cols
        df = data.sort_values(df, ["time", "lat", "lon", "depth"])
        # adds climatology day,month,week,doy columns"""
        df = data.add_day_week_month_year_clim(df)
        # removes any inf vals
        df = df.replace([np.inf, -np.inf], np.nan)
        # removes any nan string. makes string floats
        df = df.replace("nan", np.nan)
        # strips any whitespace from col values"""
        df = cmn.strip_whitespace_data(df)
        # set schema
        df = set_core_dtypes(df)
        df['cycle'] = df['cycle'].astype("Int64")   
        df.columns.to_list()[:]
        #transfers data to vault/
        if len(df) >0:
            df.to_parquet(f"{argo_rep_path}{tbl}_{os.path.basename(fil).strip('.nc')}.parquet",
            index=False)


# tableName = tbl
# server = 'Rossby'
# col_list =  ["time"] + cmn.get_numeric_cols_in_table_excluding_climatology(
#     tableName, server
#     ) 

# exclude_val = f"'4'"
# qc_suffix = '_qc'

# stats_DF = pd.DataFrame(index=["count", "max", "mean", "min", "std"])
# for var in tqdm(col_list):
#     print(var)
#     stats_DF[var] = ""
#     if var == 'time':  
#         stats_qry = (
#             f"""SELECT count_big({var}),MAX({var}),MIN({var}),'','' FROM {tableName} where JULD_QC <> {exclude_val}"""
#         )         
#         var_df = DB.dbRead(stats_qry, server)    
#     elif ('PRES' in var or var == 'depth'):
#         stats_qry = f"""SELECT count_big({var}),MAX({var}),MIN({var}),AVG({var}),STDEV({var}) FROM {tableName} where PRES_QC <> {exclude_val}"""
#         var_df = DB.dbRead(stats_qry, server)
#     elif 'TEMP'   in var:
#         stats_qry = f"""SELECT count_big({var}),MAX({var}),MIN({var}),AVG({var}),STDEV({var}) FROM {tableName} where TEMP_QC <> {exclude_val}"""
#         var_df = DB.dbRead(stats_qry, server)  
#     elif 'PSAL'   in var:
#         stats_qry = f"""SELECT count_big({var}),MAX({var}),MIN({var}),AVG({var}),STDEV({var}) FROM {tableName} where PSAL_QC <> {exclude_val} and psal > -17014118346046923"""
#         var_df = DB.dbRead(stats_qry, server)   
#     elif (var == 'lat' or var == 'lon'):
#         stats_qry = f"""SELECT count_big({var}),MAX({var}),MIN({var}),AVG({var}),STDEV({var}) FROM {tableName} where POSITION_QC <> {exclude_val} and POSITION_QC <> '9'"""
#         var_df = DB.dbRead(stats_qry, server)   
#     else:    
#         stats_qry = f"""SELECT count_big({var}),MAX(cast({var} as numeric(12, 0))),MIN(cast({var} as numeric(12, 0))),AVG(cast({var} as numeric(12, 0))),STDEV(cast({var} as numeric(12, 0))) FROM {tableName} """
#         var_df = DB.dbRead(stats_qry, server)                            
#     if (var == 'lat' or var == 'lon'):
#         stats_DF.at["mean", var] = ""
#         stats_DF.at["std", var] = ""
#     else:
#         stats_DF.at["mean", var] = var_df.iloc[:, 3][0]
#         stats_DF.at["std", var] = var_df.iloc[:, 4][0]
#     stats_DF.at["count", var] = var_df.iloc[:, 0][0] 
#     stats_DF.at["max", var] = var_df.iloc[:, 1][0]
#     stats_DF.at["min", var] = var_df.iloc[:, 2][0]


# stats.update_stats_large(tableName, stats_DF, 'Opedia', 'Mariana')