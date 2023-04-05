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

tbl = 'tblArgoCore_REP'
argo_base_path = vs.float_dir + 'tblArgoBGC_REP/raw/202206-ArgoData/dac/'
argo_rep_path = f'{vs.float_dir}{tbl}/rep/'


qry = f'SELECT float_id, count(*) as cnt from tblArgoCore_REP_update group by float_id'
df_cmap = DB.dbRead(qry, 'Rossby')


# argo_base_path = vs.collected_data + "insitu/float/ARGO/202206-ArgoData/dac/"
# daac_list = [
#     "aoml",
#     "bodc",
#     "coriolis",
#     "csio",
#     "csiro",
#     "incois",
#     "jma",
#     "kma",
#     "kordi",
#     "meds",
#     "nmdis",
# ]

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
        ]


fil = Core_flist[12274]
fil = '/data/CMAP Data Submission Dropbox/Simons CMAP/vault/observation/in-situ/float/tblArgoBGC_REP/raw/202206-ArgoData/dac/Core_subset/5901098_prof.nc'
fil_pass = []
#5905229
Core_flist_sub = Core_flist[12275:]
Core_flist_sub[0]
for fil in tqdm(Core_flist_sub):
    # open xarray
    xdf = xr.open_dataset(fil)
    # convert netcdf binary
    xdf = cmn.decode_xarray_bytes(xdf)
    for v in xdf.data_vars.values():
        print(v.long_name)
    xdf.data_vars['SCIENTIFIC_CALIB_EQUATION']
    xdf.FIRMWARE_VERSION
    xum = xdf.drop([x for x in list(xdf.keys()) if x not in um_cols])
    xum.data_vars.values()
    for x in xum.data_vars:
        if xum.data_vars[x].size > 0:
            print(x)
            print(xum.data_vars[x].attrs)
            print(xum.data_vars[x].max().item())
        
    xum.data_vars['PI_NAME'].max().item()
    df_um = xum.to_dataframe().reset_index()
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
    df["time"] = pd.to_datetime(
        df["time"].astype(str), format="%Y-%m-%d %H:%M:%S"
    ).astype("datetime64[s]")
    df["JULD_LOCATION"] = pd.to_datetime(
        df["JULD_LOCATION"].astype(str), format="%Y-%m-%d %H:%M:%S"
    ).astype("datetime64[s]")

    # adds any missing columns and reorders
    df = core_reorder_and_add_missing_cols(df)
    #### Check that number of rows dropped is only nulls
    # len_df = len(df)
    # # len_null = len(df.loc[(df['PRES'].isna() ) & (df['PSAL'].isna() )  & (df['TEMP'].isna() )])
    # len_drop =  len(df.loc[df['depth'].isna() ])
    # len_pos =  len(df.loc[(df['lat'].isna() ) | (df['lon'].isna() )  | (df['time'].isna() )])
    # len_pos =  len(df.loc[(df['time'].isna() & df['JULD_LOCATION'].notnull() )])
    # # len_pres = len(df.loc[( (df['PRES_QC'] == '4') | (df['TEMP_QC'] == '4') | (df['PSAL_QC'] == '4')) & (df['POSITION_QC'] != '9') & (df['lat'].notnull() ) & (df['lon'].notnull() )])
    
    # df_notnull = df.loc[(df['PRES']==df['PRES'] ) & (df['PSAL']==df['PSAL'] )  & (df['TEMP']==df['TEMP'] )]
    # len_half_dup = len(df_notnull.loc[df_notnull[["time", "lat", "lon", "depth"]].duplicated(keep=False)])/2
    # #df.to_csv('argo.csv',index=False)

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
    
    DB.toSQLbcp_wrapper(df, "tblArgoCore_REP_update", "Rossby")

    # # fill in gaps of partial ingestion
    # if len(df) > 0:
    #     float_id = int(df['float_id'].iloc[0])
    #     if len( df_cmap.loc[df_cmap['float_id']==float_id]) == 1:
    #         cmap_size = df_cmap.loc[df_cmap['float_id']==float_id]['cnt'].iloc[0]
    #         if len(df) == cmap_size:
    #             print(f'{float_id} already ingested')
    #             fil_pass.append(fil)
    #             continue
    #         else:
    #             print(f'{float_id} partially ingested')
    #             del_qry = f'delete from tblArgoCore_REP_update where float_id = {float_id}'
    #             DB.DB_modify(del_qry,'Rossby')
    #             DB.toSQLbcp_wrapper(df, "tblArgoCore_REP_update", "Rossby")
    #             fil_pass.append(fil)
    #     else:
    #         DB.toSQLbcp_wrapper(df, "tblArgoCore_REP_update", "Rossby")
    #         fil_pass.append(fil)
    # else:
    #     fil_pass.append(fil)
    #     continue

    # # downcasts data
    # df.loc[:, df.columns != "time"] = df.loc[:, df.columns != "time"].apply(
    # pd.to_numeric, errors="ignore", downcast="signed")
    # df['cycle'] = df['cycle'].astype("Int64")

        #     # # builds summary stats for aggregation
        #     # stats.buildLarge_Stats(
        #     #     df, df["float_id"].iloc[0], "tblArgoCore_REP", "float", transfer_flag="na"
        #     # )
    # ingests data

    # DB.toSQLbcp_wrapper(df, "tblArgoCore_REP_update", "Rossby")
    
    # err = subprocess.check_call([DB.toSQLbcp_wrapper(df, "tblArgoCore_REP_update", "Rainier")])
    #transfers data to vault/
    # df.to_parquet(
    #     vs.float_dir + "tblArgoCore_REP/rep/" + os.path.basename(fil).strip(".nc") + ".parquet",
    #     index=False)


len(fil_pass) == len(Core_flist)    

qry = "select distinct year from dbo.tblArgoCore_REP_update order by year"
df_yr = DB.dbRead(qry, 'Rossby')


yr_list = df_yr['year'].to_list()

for yr in tqdm(yr_list):
    qry = f"select * from dbo.tblArgoCore_REP_update where year = {yr}"
    df_ing = DB.dbRead(qry, 'Rossby')
    DB.toSQLbcp_wrapper(df_ing, "tblArgoCore_REP_update", "Mariana")
    # DB.toSQLbcp_wrapper(df_ing, "tblArgoCore_REP_update", "Rainier")
    del df_ing

## Export previous version before retiring / deleting
qry = "select distinct year from dbo.tblArgoCore_REP order by year"
df_yr = DB.dbRead(qry, 'Rossby')
yr_list = df_yr['year'].to_list()
for yr in tqdm(yr_list):
    qry = f"select * from dbo.tblArgoCore_REP where year = {yr}"
    df_exp = DB.dbRead(qry, 'Rossby')
    df_exp.to_parquet(argo_rep_path+f'tblArgoCore_REP_2021-06-10_{yr}.parquet')

## Export previous version before retiring / deleting
tbl = 'tblArgoMerge_REP'
vs.leafStruc(vs.float_dir+tbl)
rep_path = vs.float_dir+tbl+'/rep/'
qry = "select distinct year from dbo.tblArgoMerge_REP order by year"
df_yr = DB.dbRead(qry, 'Rainier')
yr_list = df_yr['year'].to_list()
for yr in tqdm(yr_list):
    qry = f"select * from dbo.tblArgoMerge_REP where year = {yr}"
    df_exp = DB.dbRead(qry, 'Rainier')
    df_exp.to_parquet(rep_path+f'tblArgoMerge_REP_{yr}.parquet')

tableName = tbl
server = 'Rossby'
col_list =  ["time"] + cmn.get_numeric_cols_in_table_excluding_climatology(
    tableName, server
    ) 

exclude_val = f"'4'"
qc_suffix = '_qc'

stats_DF = pd.DataFrame(index=["count", "max", "mean", "min", "std"])
for var in tqdm(col_list):
    print(var)
    stats_DF[var] = ""
    if var == 'time':  
        stats_qry = (
            f"""SELECT count_big({var}),MAX({var}),MIN({var}),'','' FROM {tableName} where JULD_QC <> {exclude_val}"""
        )         
        var_df = DB.dbRead(stats_qry, server)    
    elif ('PRES' in var or var == 'depth'):
        stats_qry = f"""SELECT count_big({var}),MAX({var}),MIN({var}),AVG({var}),STDEV({var}) FROM {tableName} where PRES_QC <> {exclude_val}"""
        var_df = DB.dbRead(stats_qry, server)
    elif 'TEMP'   in var:
        stats_qry = f"""SELECT count_big({var}),MAX({var}),MIN({var}),AVG({var}),STDEV({var}) FROM {tableName} where TEMP_QC <> {exclude_val}"""
        var_df = DB.dbRead(stats_qry, server)  
    elif 'PSAL'   in var:
        stats_qry = f"""SELECT count_big({var}),MAX({var}),MIN({var}),AVG({var}),STDEV({var}) FROM {tableName} where PSAL_QC <> {exclude_val} and psal > -17014118346046923"""
        var_df = DB.dbRead(stats_qry, server)   
    elif (var == 'lat' or var == 'lon'):
        stats_qry = f"""SELECT count_big({var}),MAX({var}),MIN({var}),AVG({var}),STDEV({var}) FROM {tableName} where POSITION_QC <> {exclude_val} and POSITION_QC <> '9'"""
        var_df = DB.dbRead(stats_qry, server)   
    else:    
        stats_qry = f"""SELECT count_big({var}),MAX(cast({var} as numeric(12, 0))),MIN(cast({var} as numeric(12, 0))),AVG(cast({var} as numeric(12, 0))),STDEV(cast({var} as numeric(12, 0))) FROM {tableName} """
        var_df = DB.dbRead(stats_qry, server)                            
    if (var == 'lat' or var == 'lon'):
        stats_DF.at["mean", var] = ""
        stats_DF.at["std", var] = ""
    else:
        stats_DF.at["mean", var] = var_df.iloc[:, 3][0]
        stats_DF.at["std", var] = var_df.iloc[:, 4][0]
    stats_DF.at["count", var] = var_df.iloc[:, 0][0] 
    stats_DF.at["max", var] = var_df.iloc[:, 1][0]
    stats_DF.at["min", var] = var_df.iloc[:, 2][0]


stats.update_stats_large(tableName, stats_DF, 'Opedia', 'Mariana')