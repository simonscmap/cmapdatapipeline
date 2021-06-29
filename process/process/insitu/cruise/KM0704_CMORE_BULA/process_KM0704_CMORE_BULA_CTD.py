import pandas as pd
import geopandas as gpd
import numpy as np

from scipy.spatial import cKDTree
from shapely.geometry import Point

import glob
import os
import re

from cmapingest import vault_structure as vs
import geopandas as gpd
import numpy as np


CMORE_BULA_path = vs.collected_data + "insitu/cruise/misc_cruise/KM0704_CMORE_BULA/"


def ckdnearest(df1, df2, cols_2_drop):

    gdA = gpd.GeoDataFrame(df1, geometry=gpd.points_from_xy(df1.lon, df1.lat))
    gdB = gpd.GeoDataFrame(df2, geometry=gpd.points_from_xy(df2.lon, df2.lat))

    nA = np.array(list(gdA.geometry.apply(lambda x: (x.x, x.y))))
    nB = np.array(list(gdB.geometry.apply(lambda x: (x.x, x.y))))
    btree = cKDTree(nB)
    dist, idx = btree.query(nA, k=1)
    gdB_nearest = (
        gdB.iloc[idx].drop(columns=["geometry"] + cols_2_drop).reset_index(drop=True)
    )
    gdf = pd.concat(
        [gdA.reset_index(drop=True), gdB_nearest, pd.Series(dist, name="dist")], axis=1
    )

    return gdf


def process_ctd():
    """concat CTD, build station/cast/direction out of filename, zero padd number"""
    ctd_concat_list = []
    ctd_flist = glob.glob(CMORE_BULA_path + "CTD/" + "*.ctd")
    for ctd_cast in ctd_flist:
        df = pd.read_csv(
            ctd_cast,
            delim_whitespace=True,
            skiprows=3,
            names=[
                "CTDPRS",
                "CTDTMP",
                "CTDSAL",
                "CTDOXY",
                "PAR",
                "LS6000",
                "CHLPIG",
                "NITRATE",
                "num_observations",
                "QUALT1",
            ],
        )
        df.replace(-9.0, np.nan, inplace=True)
        station_cast_meta = os.path.basename(ctd_cast).split(".")[0].split("bu1")[1]
        df["station"] = station_cast_meta.split("s")[1].split("c")[0].zfill(3)
        df["cast"] = (
            station_cast_meta.split("c")[1].split("up")[0].split("dn")[0].zfill(3)
        )
        df["cast_direction"] = station_cast_meta[-2:]
        df["num_observations"] = df["num_observations"].astype(str).str.zfill(5)
        df[
            [
                "CTDPRS_flag",
                "CTDTMP_flag",
                "CTDSAL_flag",
                "CTDOXY_flag",
                "PAR_flag",
                "LS6000_flag",
                "CHLPIG_flag",
                "NITRATE_flag",
            ]
        ] = (
            df["QUALT1"].astype(str).str.extractall("(.)")[0].unstack()
        )
        df.drop(
            ["NITRATE", "NITRATE", "LS6000", "LS6000_flag", "QUALT1"],
            axis=1,
            inplace=True,
        )

        ctd_concat_list.append(df)
    concat_df = pd.concat(ctd_concat_list, axis=0, ignore_index=True)
    return concat_df


def process_underway():
    """create header, split flags, create time"""
    uway_path = CMORE_BULA_path + "underway/bu1uw2.dat"
    df = pd.read_csv(
        uway_path,
        delim_whitespace=True,
        names=[
            "year",
            "doy",
            "hour",
            "minute",
            "lat",
            "lon",
            "salinity",
            "temperature",
            "chloropigment",
            "quality_flags",
        ],
    )
    # builds time out of year, doy,hour,min
    df["time"] = pd.to_datetime(
        (df["year"]).astype(str)
        + (df["doy"]).astype(str)
        + (df["hour"].astype(str))
        + (df["minute"].astype(str)),
        format="%Y%j%H%M",
    )
    # split flags into variable flag columns
    df["salinity_flag"] = (
        df["quality_flags"].astype(str).str.split("", expand=True).iloc[:, [1]]
    )
    df["temperature_flag"] = (
        df["quality_flags"].astype(str).str.split("", expand=True).iloc[:, [2]]
    )
    df["chloropigment_flag"] = (
        df["quality_flags"].astype(str).str.split("", expand=True).iloc[:, [3]]
    )
    # removes old cols and reorders
    df.drop(["year", "doy", "hour", "minute", "quality_flags"], axis=1, inplace=True)
    df = df[
        [
            "time",
            "lat",
            "lon",
            "salinity",
            "temperature",
            "chloropigment",
            "salinity_flag",
            "temperature_flag",
            "chloropigment_flag",
        ]
    ]
    return df


def process_underway_sample():
    uway_sample_path = CMORE_BULA_path + "underway/bu1uwsamp.txt"
    df = pd.read_csv(
        uway_sample_path,
        delim_whitespace=True,
        skiprows=3,
        names=[
            "sample",
            "lat",
            "lon",
            "doy",
            "GMT",
            "sigma",
            "oxygen",
            "DIC",
            "alkalin",
            "phspht",
            "NO2_NO3",
            "silcat",
            "DOP",
            "DON",
            "DOC",
            "LLN",
            "LLP",
            "PC",
            "PN",
            "PP",
            "PSi",
            "chl_a",
            "pheo",
            "chlda",
            "chl_plus",
            "PERID",
            "19_but",
            "FUCO",
            "19_Hex",
            "Prasino",
            "Viol",
            "Diadino",
            "Allox",
            "Lutein",
            "Zeaxan",
            "Chl_b",
            "alpha_carotene",
            "beta_carotene",
            "divinyl_chl",
            "monovinyl_chl",
            "HPLC_chla",
            "hetero_bact",
            "prochloro_bact",
            "synecho_bact",
            "eukaryotes",
            "ATP",
            "CH4",
            "N2O",
        ],
    )
    df.replace(-9.0, np.nan, inplace=True)
    df["sample"] = df["sample"].astype(str).str.zfill(3)
    # lons need to be negative
    df["lon"] = df["lon"] * -1.0
    # convert datetime ## !!!ASSUMING YEAR IS 2007 BASED ON OTHER UNDERWAY!!!!
    df["year"] = "2007"
    df["hour"] = df["GMT"].astype(str).str.zfill(4)
    df["time"] = pd.to_datetime(
        (df["year"]).astype(str) + (df["doy"]).astype(str) + (df["hour"].astype(str)),
        format="%Y%j%H%M",
    )
    # drop old cols and reorder
    df.drop(["year", "doy", "hour"], axis=1, inplace=True)
    df = df[
        [
            "time",
            "lat",
            "lon",
            "sample",
            "sigma",
            "oxygen",
            "DIC",
            "alkalin",
            "phspht",
            "NO2_NO3",
            "silcat",
            "LLN",
            "LLP",
            "PC",
            "PN",
            "PP",
            "PSi",
            "chl_a",
            "pheo",
            "chl_plus",
            "PERID",
            "19_but",
            "FUCO",
            "19_Hex",
            "Prasino",
            "Viol",
            "Diadino",
            "Allox",
            "Lutein",
            "Zeaxan",
            "Chl_b",
            "alpha_carotene",
            "beta_carotene",
            "divinyl_chl",
            "monovinyl_chl",
            "HPLC_chla",
            "hetero_bact",
            "prochloro_bact",
            "synecho_bact",
            "eukaryotes",
            "ATP",
        ]
    ]
    return df


def process_bottle():
    bottle_path = CMORE_BULA_path + "bottle/bula1.gof"
    df = pd.read_csv(
        bottle_path,
        skiprows=5,
        index_col=False,
        delim_whitespace=True,
        names=[
            "station",
            "cast",
            "rosette",
            "lat",
            "lon",
            "CTD_pres",
            "CTD_temp",
            "CTD_sal",
            "CTD_doxy",
            "CTD_chl",
            "theta",
            "sigma",
            "bottle_oxygen",
            "DIC",
            "alkalinity",
            "phosphate",
            "NO2_NO3",
            "silcate",
            "DOP",
            "DON",
            "DOC",
            "TDP",
            "TDN",
            "LLN",
            "LLP",
            "PC",
            "PN",
            "PP",
            "PSi",
            "chl_a",
            "pheo",
            "chlda",
            "chl_plus",
            "PERID",
            "19_but",
            "FUCO",
            "19_Hex",
            "Prasino",
            "Viol",
            "Diadino",
            "Allox",
            "Lutein",
            "Zeaxan",
            "Chl_b",
            "alpha_carotene",
            "beta_carotene",
            "divinyl_chl",
            "monovinyl_chl",
            "HPLC_chla",
            "hetero_bact",
            "prochloro_bact",
            "synecho_bact",
            "eukaryotes",
            "ATP",
            "CH4",
            "N2O",
            "qual_1",
            "qual_2",
            "qual_3",
            "qual_4",
            "qual_5",
            "qual_6",
            "qual_7",
            "qual_8",
        ],
    )
    df.replace(-9.0, np.nan, inplace=True)

    df[
        [
            "CTD_temp_flag",
            "CTD_sal_flag",
            "CTD_doxy_flag",
            "CTD_chl_flag",
            "theta_flag",
            "sigma_flag",
            "bottle_oxygen_flag",
        ]
    ] = (
        df["qual_1"].astype(str).str.extractall("(.)")[0].unstack()
    )
    df[
        [
            "CTD_temp_flag",
            "CTD_sal_flag",
            "CTD_doxy_flag",
            "CTD_chl_flag",
            "theta_flag",
            "sigma_flag",
            "bottle_oxygen_flag",
        ]
    ] = (
        df["qual_1"].astype(str).str.extractall("(.)")[0].unstack()
    )
    df[
        [
            "DIC_flag",
            "alkalinity_flag",
            "phosphate_flag",
            "NO2_NO3_flag",
            "silcate_flag",
            "DOP_flag",
            "DON_flag",
        ]
    ] = (
        df["qual_2"].astype(str).str.extractall("(.)")[0].unstack()
    )
    df[["DOC_flag", "TDP_flag", "TDN_flag", "LLN_flag", "LLP_flag", "PC_flag"]] = (
        df["qual_3"].astype(str).str.extractall("(.)")[0].unstack()
    )
    df[["PN_flag", "PP_flag", "PSi_flag", "chl_a_flag", "pheo_flag", "chlda_flag"]] = (
        df["qual_4"].astype(str).str.extractall("(.)")[0].unstack()
    )
    df[
        [
            "chl_plus_flag",
            "PERID_flag",
            "19_but_flag",
            "FUCO_flag",
            "19_Hex_flag",
            "Prasino_flag",
        ]
    ] = (
        df["qual_5"].astype(str).str.extractall("(.)")[0].unstack()
    )
    df[
        [
            "Viol_flag",
            "Diadino_flag",
            "Allox_flag",
            "Lutein_flag",
            "Zeaxan_flag",
            "Chl_b_flag",
        ]
    ] = (
        df["qual_6"].astype(str).str.extractall("(.)")[0].unstack()
    )
    df[
        [
            "alpha_carotene_flag",
            "beta_carotene_flag",
            "divinyl_chl_flag",
            "monovinyl_chl_flag",
            "HPLC_chla_flag",
            "hetero_bact_flag",
        ]
    ] = (
        df["qual_7"].astype(str).str.extractall("(.)")[0].unstack()
    )
    df[
        [
            "prochloro_bact_flag",
            "synecho_bact_flag",
            "eukaryotes_flag",
            "ATP_flag",
            "CH4_flag",
            "N2O_flag",
        ]
    ] = (
        df["qual_8"].astype(str).str.extractall("(.)")[0].unstack()
    )
    df["station"] = df["station"].astype(str).str.zfill(3)
    # lons need to be negative
    df["lon"] = df["lon"] * -1.0
    df.drop(
        [
            "DOP",
            "DOP_flag",
            "DON",
            "DON_flag",
            "DOC",
            "DOC_flag",
            "chlda",
            "chlda_flag",
            "CH4",
            "CH4_flag",
            "N2O",
            "N2O_flag",
            "qual_1",
            "qual_2",
            "qual_3",
            "qual_4",
            "qual_5",
            "qual_6",
            "qual_7",
            "qual_8",
        ],
        axis=1,
        inplace=True,
    )
    return df


def process_wind():
    wind_concat_list = []
    wind_fpath = CMORE_BULA_path + "wind/"
    wind_flist = glob.glob(wind_fpath + "*.gz*")
    names = [
        "year",
        "jday",
        "hr",
        "min",
        "sec",
        "msec",
        "code",
        "port_wind_relative_speed",
        "port_wind_relative_heading",
        "SOG",
        "COG",
        "POSMV_HDG",
        "wind_true_speed",
        "wind_true_heading",
        "starboard_wind_relative_speed",
        "starboard_wind_relative_heading",
    ]
    for windf in wind_flist:
        df = pd.read_csv(windf, names=names, delim_whitespace=True)
        df["time"] = pd.to_datetime(
            (df["year"]).astype(str)
            + (df["jday"].astype(str))
            + (df["hr"].astype(str))
            + (df["min"].astype(str))
            + (df["sec"].astype(str).str.zfill(2))
            + (df["msec"].astype(str)),
            format="%Y%j%H%M%S%f",
        )
        df.drop(["year", "jday", "hr", "min", "sec", "msec"], axis=1, inplace=True)
        df = df[
            [
                "time",
                "port_wind_relative_speed",
                "port_wind_relative_heading",
                "SOG",
                "COG",
                "POSMV_HDG",
                "wind_true_speed",
                "wind_true_heading",
                "starboard_wind_relative_speed",
                "starboard_wind_relative_heading",
            ]
        ]
        wind_concat_list.append(df)

    concat_df = pd.concat(wind_concat_list, axis=0, ignore_index=True)
    concat_df["time"] = concat_df["time"].astype("datetime64[s]")

    return concat_df


def add_missing_ST_columns(underway, CTD, bottle, wind):
    # Adds time to bottle dataset
    bottle["depth"] = bottle["CTD_pres"]
    underway_geo = underway.copy()
    bottle_merge = ckdnearest(bottle, underway_geo, ["lat", "lon"])
    bottle_ST = bottle_merge[
        [
            "time",
            "lat",
            "lon",
            "depth",
            "station",
            "cast",
            "rosette",
            "CTD_pres",
            "CTD_temp",
            "CTD_sal",
            "CTD_doxy",
            "CTD_chl",
            "theta",
            "sigma",
            "bottle_oxygen",
            "DIC",
            "alkalinity",
            "phosphate",
            "NO2_NO3",
            "silcate",
            "TDP",
            "TDN",
            "LLN",
            "LLP",
            "PC",
            "PN",
            "PP",
            "PSi",
            "chl_a",
            "pheo",
            "chl_plus",
            "PERID",
            "19_but",
            "FUCO",
            "19_Hex",
            "Prasino",
            "Viol",
            "Diadino",
            "Allox",
            "Lutein",
            "Zeaxan",
            "Chl_b",
            "alpha_carotene",
            "beta_carotene",
            "divinyl_chl",
            "monovinyl_chl",
            "HPLC_chla",
            "hetero_bact",
            "prochloro_bact",
            "synecho_bact",
            "eukaryotes",
            "ATP",
            "CTD_temp_flag",
            "CTD_sal_flag",
            "CTD_doxy_flag",
            "CTD_chl_flag",
            "theta_flag",
            "sigma_flag",
            "bottle_oxygen_flag",
            "DIC_flag",
            "alkalinity_flag",
            "phosphate_flag",
            "NO2_NO3_flag",
            "silcate_flag",
            "TDP_flag",
            "TDN_flag",
            "LLN_flag",
            "LLP_flag",
            "PC_flag",
            "PN_flag",
            "PP_flag",
            "PSi_flag",
            "chl_a_flag",
            "pheo_flag",
            "chl_plus_flag",
            "PERID_flag",
            "19_but_flag",
            "FUCO_flag",
            "19_Hex_flag",
            "Prasino_flag",
            "Viol_flag",
            "Diadino_flag",
            "Allox_flag",
            "Lutein_flag",
            "Zeaxan_flag",
            "Chl_b_flag",
            "alpha_carotene_flag",
            "beta_carotene_flag",
            "divinyl_chl_flag",
            "monovinyl_chl_flag",
            "HPLC_chla_flag",
            "hetero_bact_flag",
            "prochloro_bact_flag",
            "synecho_bact_flag",
            "eukaryotes_flag",
            "ATP_flag"
        ]
    ]

    # multiple station ST values in bottle_ST, so only first station ST used for merge
    CTD_merge = pd.merge(
        CTD, bottle_ST.drop_duplicates("station"), how="left", on="station"
    )
    CTD_merge["depth"] = CTD_merge["CTDPRS"]
    CTD_merge.rename(columns={"cast_x": "cast"}, inplace=True)
    CTD_ST = CTD_merge[
        [
            "time",
            "lat",
            "lon",
            "depth",
            "num_observations",
            "station",
            "cast",
            "cast_direction",
            "CTDPRS",
            "CTDTMP",
            "CTDSAL",
            "CTDOXY",
            "PAR",
            "CHLPIG",
            "CTDPRS_flag",
            "CTDTMP_flag",
            "CTDSAL_flag",
            "CTDOXY_flag",
            "PAR_flag",
            "CHLPIG_flag",
        ]
    ]

    # wind ST merge - had to downsample to minute res to merge
    wind["time"] = wind["time"].astype("datetime64[m]")
    underway_merge = underway.copy()
    underway_merge["time"] = underway_merge["time"].astype("datetime64[m]")
    wind_merge = pd.merge(wind, underway_merge, how="left", on="time")
    wind_ST = wind_merge[
        [
            "time",
            "lat",
            "lon",
            "port_wind_relative_speed",
            "port_wind_relative_heading",
            "SOG",
            "COG",
            "POSMV_HDG",
            "wind_true_speed",
            "wind_true_heading",
            "starboard_wind_relative_speed",
            "starboard_wind_relative_heading",
        ]
    ]
    wind_ST = wind_ST[~wind_ST["lat"].isnull()]
    return bottle_ST, CTD_ST, wind_ST


def export_to_staging(dataset_df, dataset_fname):
    outpath = vs.combined
    writer = pd.ExcelWriter(outpath + f"""{dataset_fname}.xlsx""", engine="xlsxwriter")
    dataset_df.to_excel(writer, sheet_name="data", index=False)
    writer.save()


""" 
CTD: (21874)                [station,cast] #added time from bottle merge
underway: (16332)           [time,lat,lon]   
underway sample (7):        [time,lat,lon,sample]
bottle (552)                ['station','cast','rosette','lat','lon']
wind (507850)               ['time']
"""


CTD = process_ctd()
underway = process_underway()
underway_sample = process_underway_sample()
bottle = process_bottle()
wind = process_wind()

# add ST coords from merge
bottle_ST, CTD_ST, wind_ST = add_missing_ST_columns(underway, CTD, bottle, wind)


# export to staging
export_to_staging(underway_sample, "KM0704_CMORE_BULA_Underway_Sample")
export_to_staging(underway, "KM0704_CMORE_BULA_Underway")
export_to_staging(bottle_ST, "KM0704_CMORE_BULA_Bottle")
export_to_staging(CTD_ST, "KM0704_CMORE_BULA_CTD")
export_to_staging(wind_ST, "KM0704_CMORE_BULA_wind")
