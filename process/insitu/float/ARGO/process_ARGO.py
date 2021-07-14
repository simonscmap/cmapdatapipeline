from cmapingest import DB
from cmapingest import common as cmn
from cmapingest import vault_structure as vs
from cmapingest import data
import stats
import pandas as pd
import xarray as xr
import os
import numpy as np
from tqdm import tqdm
import glob

import multiprocessing as mp


#####################################
### File Structure and Unzipping  ###
#####################################
#####################################


argo_base_path = vs.collected_data + "insitu/float/ARGO/202106-ArgoData/dac/"
daac_list = [
    "aoml",
    "bodc",
    "coriolis",
    "csio",
    "csiro",
    "incois",
    "jma",
    "kma",
    "kordi",
    "meds",
    "nmdis",
]


def unzip_and_organize_BGC():
    # 1559 files
    vs.makedir(argo_base_path + "BGC/")
    os.chdir(argo_base_path)
    for daac in tqdm(daac_list):
        os.system(
            f"""tar -xvf {daac}_bgc.tar.gz -C BGC/ --transform='s/.*\///' --wildcards --no-anchored '*_Sprof*'"""
        )


def unzip_and_organize_Core():
    # 16189 files
    vs.makedir(argo_base_path + "Core/")
    os.chdir(argo_base_path)
    for daac in tqdm(daac_list):
        os.system(
            f"""tar -xvf {daac}_core.tar.gz -C Core/ --transform='s/.*\///' --wildcards --no-anchored '*_prof*'"""
        )


#####################################
### Cleaning and Processing Block ###
#####################################
#####################################


def reorder_bgc_data(df):
    """Reordered a BGC dataframe to move ST coodinates first and sci equations last

    Args:
        df (Pandas DataFrame): Input ARGO BGC DataFrame

    Returns:
        df (Pandas DataFrame): Reordered DataFrame
    """
    st_col_list = [
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
    ]
    st_cols = df[st_col_list]
    non_st_cols = df.drop(st_col_list, axis=1)
    reorder_df = pd.concat([st_cols, non_st_cols], axis=1, sort=False)

    sci_col_list = [
        "SCIENTIFIC_CALIB_COEFFICIENT",
        "SCIENTIFIC_CALIB_COMMENT",
        "SCIENTIFIC_CALIB_DATE",
        "SCIENTIFIC_CALIB_EQUATION",
    ]
    sci_cols = reorder_df[sci_col_list]
    non_sci_cols = reorder_df.drop(sci_col_list, axis=1)
    neworder_df = pd.concat([non_sci_cols, sci_cols], axis=1, sort=False)
    return neworder_df


def replace_comm_delimiter(df):
    sci_cols = [
        "SCIENTIFIC_CALIB_COEFFICIENT",
        "SCIENTIFIC_CALIB_COMMENT",
        "SCIENTIFIC_CALIB_EQUATION",
    ]
    for col in sci_cols:
        if len(df[col]) > 0:
            df[col] = df[col].astype(str).str.replace(",", ";")
    return df


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


def bgc_reorder_and_add_missing_cols(df):
    missing_cols = set(bgc_cols) ^ set(list(df))
    df = df.reindex(columns=[*df.columns.tolist(), *missing_cols]).reindex(
        columns=bgc_cols
    )
    return df


def core_reorder_and_add_missing_cols(df):
    missing_cols = set(core_cols) ^ set(list(df))
    df = df.reindex(columns=[*df.columns.tolist(), *missing_cols]).reindex(
        columns=core_cols
    )
    return df


def get_BGC_flist():
    BGC_flist = glob.glob(argo_base_path + "BGC/*.nc")
    return BGC_flist


def get_Core_flist():
    Core_flist = glob.glob(argo_base_path + "Core/*.nc")
    return Core_flist


BGC_flist = get_BGC_flist()
Core_flist = get_Core_flist()


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

bgc_cols = [
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
    "BBP470",
    "BBP470_ADJUSTED",
    "BBP470_ADJUSTED_ERROR",
    "BBP470_ADJUSTED_QC",
    "BBP470_QC",
    "BBP470_dPRES",
    "BBP532",
    "BBP532_ADJUSTED",
    "BBP532_ADJUSTED_ERROR",
    "BBP532_ADJUSTED_QC",
    "BBP532_QC",
    "BBP532_dPRES",
    "BBP700",
    "BBP700_2",
    "BBP700_2_ADJUSTED",
    "BBP700_2_ADJUSTED_ERROR",
    "BBP700_2_ADJUSTED_QC",
    "BBP700_2_QC",
    "BBP700_2_dPRES",
    "BBP700_ADJUSTED",
    "BBP700_ADJUSTED_ERROR",
    "BBP700_ADJUSTED_QC",
    "BBP700_QC",
    "BBP700_dPRES",
    "BISULFIDE",
    "BISULFIDE_ADJUSTED",
    "BISULFIDE_ADJUSTED_ERROR",
    "BISULFIDE_ADJUSTED_QC",
    "BISULFIDE_dPRES",
    "CNDC",
    "CNDC_ADJUSTED",
    "CNDC_ADJUSTED_ERROR",
    "CNDC_ADJUSTED_QC",
    "CNDC_dPRES",
    "CDOM",
    "CDOM_ADJUSTED",
    "CDOM_ADJUSTED_ERROR",
    "CDOM_ADJUSTED_QC",
    "CDOM_QC",
    "CDOM_dPRES",
    "CHLA",
    "CHLA_ADJUSTED",
    "CHLA_ADJUSTED_ERROR",
    "CHLA_ADJUSTED_QC",
    "CHLA_QC",
    "CHLA_dPRES",
    "CP660",
    "CP660_ADJUSTED",
    "CP660_ADJUSTED_ERROR",
    "CP660_ADJUSTED_QC",
    "CP660_QC",
    "CP660_dPRES",
    "DOWNWELLING_PAR",
    "DOWNWELLING_PAR_ADJUSTED",
    "DOWNWELLING_PAR_ADJUSTED_ERROR",
    "DOWNWELLING_PAR_ADJUSTED_QC",
    "DOWNWELLING_PAR_QC",
    "DOWNWELLING_PAR_dPRES",
    "DOWN_IRRADIANCE380",
    "DOWN_IRRADIANCE380_ADJUSTED",
    "DOWN_IRRADIANCE380_ADJUSTED_ERROR",
    "DOWN_IRRADIANCE380_ADJUSTED_QC",
    "DOWN_IRRADIANCE380_QC",
    "DOWN_IRRADIANCE380_dPRES",
    "DOWN_IRRADIANCE412",
    "DOWN_IRRADIANCE412_ADJUSTED",
    "DOWN_IRRADIANCE412_ADJUSTED_ERROR",
    "DOWN_IRRADIANCE412_ADJUSTED_QC",
    "DOWN_IRRADIANCE412_QC",
    "DOWN_IRRADIANCE412_dPRES",
    "DOWN_IRRADIANCE443",
    "DOWN_IRRADIANCE443_ADJUSTED",
    "DOWN_IRRADIANCE443_ADJUSTED_ERROR",
    "DOWN_IRRADIANCE443_ADJUSTED_QC",
    "DOWN_IRRADIANCE443_QC",
    "DOWN_IRRADIANCE443_dPRES",
    "DOWN_IRRADIANCE490",
    "DOWN_IRRADIANCE490_ADJUSTED",
    "DOWN_IRRADIANCE490_ADJUSTED_ERROR",
    "DOWN_IRRADIANCE490_ADJUSTED_QC",
    "DOWN_IRRADIANCE490_QC",
    "DOWN_IRRADIANCE490_dPRES",
    "DOWN_IRRADIANCE555",
    "DOWN_IRRADIANCE555_ADJUSTED",
    "DOWN_IRRADIANCE555_ADJUSTED_ERROR",
    "DOWN_IRRADIANCE555_ADJUSTED_QC",
    "DOWN_IRRADIANCE555_QC",
    "DOWN_IRRADIANCE555_dPRES",
    "DOXY",
    "DOXY2",
    "DOXY2_ADJUSTED",
    "DOXY2_ADJUSTED_ERROR",
    "DOXY2_ADJUSTED_QC",
    "DOXY2_QC",
    "DOXY2_dPRES",
    "DOXY_ADJUSTED",
    "DOXY_ADJUSTED_ERROR",
    "DOXY_ADJUSTED_QC",
    "DOXY_QC",
    "DOXY_dPRES",
    "JULD_QC",
    "NITRATE",
    "NITRATE_ADJUSTED",
    "NITRATE_ADJUSTED_ERROR",
    "NITRATE_ADJUSTED_QC",
    "NITRATE_QC",
    "NITRATE_dPRES",
    "PH_IN_SITU_TOTAL",
    "PH_IN_SITU_TOTAL_ADJUSTED",
    "PH_IN_SITU_TOTAL_ADJUSTED_ERROR",
    "PH_IN_SITU_TOTAL_ADJUSTED_QC",
    "PH_IN_SITU_TOTAL_QC",
    "PH_IN_SITU_TOTAL_dPRES",
    "POSITION_QC",
    "PRES",
    "PRES_ADJUSTED",
    "PRES_ADJUSTED_ERROR",
    "PRES_ADJUSTED_QC",
    "PRES_QC",
    "PROFILE_BBP470_QC",
    "PROFILE_BBP532_QC",
    "PROFILE_BBP700_2_QC",
    "PROFILE_BBP700_QC",
    "PROFILE_BISULFIDE_QC",
    "PROFILE_CNDC_QC",
    "PROFILE_CDOM_QC",
    "PROFILE_CHLA_QC",
    "PROFILE_CP660_QC",
    "PROFILE_DOWNWELLING_PAR_QC",
    "PROFILE_DOWN_IRRADIANCE380_QC",
    "PROFILE_DOWN_IRRADIANCE412_QC",
    "PROFILE_DOWN_IRRADIANCE443_QC",
    "PROFILE_DOWN_IRRADIANCE490_QC",
    "PROFILE_DOWN_IRRADIANCE555_QC",
    "PROFILE_DOXY2_QC",
    "PROFILE_DOXY_QC",
    "PROFILE_NITRATE_QC",
    "PROFILE_PH_IN_SITU_TOTAL_QC",
    "PROFILE_PRES_QC",
    "PROFILE_PSAL_QC",
    "PROFILE_TEMP_QC",
    "PROFILE_TURBIDITY_QC",
    "PROFILE_UP_RADIANCE412_QC",
    "PROFILE_UP_RADIANCE443_QC",
    "PROFILE_UP_RADIANCE490_QC",
    "PROFILE_UP_RADIANCE555_QC",
    "PSAL",
    "PSAL_ADJUSTED",
    "PSAL_ADJUSTED_ERROR",
    "PSAL_ADJUSTED_QC",
    "PSAL_QC",
    "PSAL_dPRES",
    "TEMP",
    "TEMP_ADJUSTED",
    "TEMP_ADJUSTED_ERROR",
    "TEMP_ADJUSTED_QC",
    "TEMP_QC",
    "TEMP_dPRES",
    "TURBIDITY",
    "TURBIDITY_ADJUSTED",
    "TURBIDITY_ADJUSTED_ERROR",
    "TURBIDITY_ADJUSTED_QC",
    "TURBIDITY_QC",
    "TURBIDITY_dPRES",
    "UP_RADIANCE412",
    "UP_RADIANCE412_ADJUSTED",
    "UP_RADIANCE412_ADJUSTED_ERROR",
    "UP_RADIANCE412_ADJUSTED_QC",
    "UP_RADIANCE412_QC",
    "UP_RADIANCE412_dPRES",
    "UP_RADIANCE443",
    "UP_RADIANCE443_ADJUSTED",
    "UP_RADIANCE443_ADJUSTED_ERROR",
    "UP_RADIANCE443_ADJUSTED_QC",
    "UP_RADIANCE443_QC",
    "UP_RADIANCE443_dPRES",
    "UP_RADIANCE490",
    "UP_RADIANCE490_ADJUSTED",
    "UP_RADIANCE490_ADJUSTED_ERROR",
    "UP_RADIANCE490_ADJUSTED_QC",
    "UP_RADIANCE490_QC",
    "UP_RADIANCE490_dPRES",
    "UP_RADIANCE555",
    "UP_RADIANCE555_ADJUSTED",
    "UP_RADIANCE555_ADJUSTED_ERROR",
    "UP_RADIANCE555_ADJUSTED_QC",
    "UP_RADIANCE555_QC",
    "UP_RADIANCE555_dPRES",
]


def clean_Core(fil):
    # open xarray
    xdf = xr.open_dataset(fil)
    # convert netcdf binary
    xdf = cmn.decode_xarray_bytes(xdf)
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
    # drop ex metadata cols
    df = df.drop(["N_LEVELS", "N_PROF"], axis=1)
    # adds depth as column
    df["depth"] = df["PRES"]
    # rename ST cols
    df = rename_cols(df)
    # formats time col
    df["time"] = pd.to_datetime(
        df["time"].astype(str), format="%Y-%m-%d %H:%M:%S"
    ).astype("datetime64[s]")
    # drops duplicates created by netcdf multilevel index being flattened to pandas dataframe
    df = df.drop_duplicates(subset=["time", "lat", "lon", "depth"], keep="first")
    # drops any invalid ST rows"""
    df = df.dropna(subset=["time", "lat", "lon", "depth"])
    # sort ST cols
    df = data.sort_values(df, ["time", "lat", "lon", "depth"])
    # adds climatology day,month,week,doy columns"""
    df = data.add_day_week_month_year_clim(df)
    # adds any missing columns and reorders
    df = core_reorder_and_add_missing_cols(df)
    # removes any inf vals
    df = df.replace([np.inf, -np.inf], np.nan)
    # removes any nan string
    df = df.replace("nan", np.nan)
    # strips any whitespace from col values"""
    df = cmn.strip_whitespace_data(df)
    # downcasts data
    df.loc[:, df.columns != "time"] = df.loc[:, df.columns != "time"].apply(
        pd.to_numeric, errors="ignore", downcast="signed"
    )
    # builds summary stats for aggregation
    stats.buildLarge_Stats(
        df, df["float_id"].iloc[0], "tblArgoCore_REP", "float", transfer_flag="na"
    )
    # ingests data
    DB.toSQLbcp_wrapper(df, "tblArgoCore_REP", "Rossby")


def clean_bgc(fil, dtype):
    # open xarray
    xdf = xr.open_dataset(fil)
    # convert netcdf binary
    xdf = cmn.decode_xarray_bytes(xdf)
    # xdf to df, reset index
    df = xdf.to_dataframe().reset_index()
    # drop ex metadata cols
    df = df.drop(
        [
            "N_CALIB",
            "N_LEVELS",
            "N_PARAM",
            "N_PROF",
            "DATA_TYPE",
            "FORMAT_VERSION",
            "HANDBOOK_VERSION",
            "REFERENCE_DATE_TIME",
            "DATE_CREATION",
            "DATE_UPDATE",
            "PROJECT_NAME",
            "PI_NAME",
            "STATION_PARAMETERS",
            "PARAMETER_DATA_MODE",
            "PLATFORM_TYPE",
            "FLOAT_SERIAL_NO",
            "FIRMWARE_VERSION",
            "WMO_INST_TYPE",
            "JULD_LOCATION",
            "POSITIONING_SYSTEM",
            "CONFIG_MISSION_NUMBER",
            "PARAMETER",
            "SCIENTIFIC_CALIB_COEFFICIENT",
            "SCIENTIFIC_CALIB_COMMENT",
            "SCIENTIFIC_CALIB_DATE",
            "SCIENTIFIC_CALIB_EQUATION",
        ],
        axis=1,
        errors="ignore",
    )
    # adds depth as column
    df["depth"] = df["PRES"]
    # rename ST cols
    df = rename_cols(df)
    # formats time col
    df["time"] = pd.to_datetime(
        df["time"].astype(str), format="%Y-%m-%d %H:%M:%S"
    ).astype("datetime64[s]")
    # drops duplicates created by netcdf multilevel index being flattened to pandas dataframe
    df = df.drop_duplicates(subset=["time", "lat", "lon", "depth"], keep="first")
    # drops any invalid ST rows"""
    df = df.dropna(subset=["time", "lat", "lon", "depth"])
    # sort ST cols
    df = data.sort_values(df, ["time", "lat", "lon", "depth"])
    # adds climatology day,month,week,doy columns"""
    df = data.add_day_week_month_year_clim(df)
    # adds any missing columns and reorders
    df = bgc_reorder_and_add_missing_cols(df)
    # removes any inf vals
    df = df.replace([np.inf, -np.inf], np.nan)
    # removes any nan string
    df = df.replace("nan", np.nan)
    # strips any whitespace from col values"""
    df = cmn.strip_whitespace_data(df)
    # downcasts data
    df.loc[:, df.columns != "time"] = df.loc[:, df.columns != "time"].apply(
        pd.to_numeric, errors="ignore", downcast="signed"
    )
    # builds summary stats for aggregation
    stats.buildLarge_Stats(
        df, df["float_id"].iloc[0], "tblArgoBGC_REP", "float", transfer_flag="na"
    )
    # #ingests data
    # DB.toSQLbcp_wrapper(df, 'tblArgoBGC_REP', "Rossby")


# missed = []
# for fil in tqdm(BGC_flist):
#     try:
#         clean_bgc(fil)
#     except:
#         missed.append(fil)

# missed = []
# for fil in tqdm(Core_flist):
#     try:
#         clean_Core(fil)
#     except:
#         missed.append(fil)


fil = BGC_flist[0]
