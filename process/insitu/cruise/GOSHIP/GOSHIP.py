# dev note on feb 16th:
# add cruise name to the data file as column

import pandas as pd
import numpy as np
import xarray as xr
import glob
import os
from tqdm import tqdm
import vaex
from cmapingest import vault_structure as vs
from cmapingest import data
from cmapingest import stats

# goship_raw_path = vs.collected_data + "GOSHIP/go_ship_clean_ctd/"
goship_raw_path = "/home/nrhagen/Downloads/go_ship_clean_ctd/"

gridded_prefix = "gridded/"
reported_prefix = "reported/"


def ingest_processed(df, tablename):
    data.data_df_to_db(df, tablename, "Rainier")
    data.data_df_to_db(df, tablename, "Mariana")


def process_gridded():
    gridded_filelist = glob.glob(
        goship_raw_path + gridded_prefix + "**/*.nc", recursive=True
    )
    failed_list = []
    for fil in tqdm(gridded_filelist):
        try:
            region = fil.split("gridded/")[1].split("/")[0]
            cruise = fil.split("gridded/")[1].split("/")[1]
            df = xr.open_dataset(fil).to_dataframe().reset_index()
            df["region"] = region
            df["goship_woce_line_id"] = cruise
            df = df[
                [
                    "time",
                    "latitude",
                    "longitude",
                    "depth",
                    "pressure",
                    "temperature",
                    "practical_salinity",
                    "oxygen",
                    "conservative_temperature",
                    "absolute_salinity",
                    "region",
                    "gridded_section",
                    "goship_woce_line_id",
                ]
            ]
            df.rename(columns={"latitude": "lat", "longitude": "lon"}, inplace=True)
            df.dropna(subset=["time", "lat", "lon", "depth"], inplace=True)
            df.to_csv(
                goship_raw_path
                + "processed_gridded/"
                + "cleaned_"
                + fil.rsplit("/")[-1].split(".nc")[0]
                + ".csv",
                index=False,
            )
            stats.buildLarge_Stats(
                df, cruise, "tblGOSHIP_Gridded", "cruise", transfer_flag="na"
            )

            ingest_processed(df, "tblGOSHIP_Gridded")
        except:
            print(fil, " failed")
            failed_list.append(fil)
    print("failed: ")
    print(failed_list)


def process_sparse():
    if not os.path.exists(goship_raw_path + "processed_reported/"):
        os.makedirs(goship_raw_path + "processed_reported/")
    reported_filelist = glob.glob(
        goship_raw_path + reported_prefix + "**/*.csv", recursive=True
    )
    missed_file_list = []

    for fil in tqdm(reported_filelist):
        try:
            sample = fil.split(".csv")[0].split("/")[-1]
            df = pd.read_csv(
                fil,
                skiprows=13,
                names=[
                    "pressure",
                    "temperature",
                    "salinity",
                    "doxy",
                    "conservative_temperature",
                    "absolute_salinity",
                ],
            )
            meta = pd.read_csv(fil, nrows=10)
            meta_df = meta["CTD"].str.split("=", expand=True).T
            meta_df.columns = meta_df.iloc[0].to_list()
            meta_df = meta_df.iloc[1::]
            df = df[df.pressure != "END_DATA"]
            df["time"] = pd.to_datetime(
                (meta_df["DATE "].iloc[0] + meta_df["TIME "].iloc[0]).strip()
            )
            df["lat"] = meta_df["LATITUDE "].iloc[0].strip()
            df["lon"] = meta_df["LONGITUDE "].iloc[0].strip()
            df["lat"] = pd.to_numeric(df["lat"])
            df["depth"] = pd.to_numeric(df["pressure"])
            df["region"] = fil.rsplit("/")[-3]
            df["goship_woce_line_id"] = fil.rsplit("/")[-2]
            df = data.mapTo180180(df)
            df.replace(-999, np.nan, inplace=True)
            df.loc[df.doxy < 0, "doxy"] = np.nan
            df = df[
                [
                    "time",
                    "lat",
                    "lon",
                    "depth",
                    "pressure",
                    "temperature",
                    "salinity",
                    "doxy",
                    "conservative_temperature",
                    "absolute_salinity",
                    "region",
                    "goship_woce_line_id",
                ]
            ]
            # df.to_csv(goship_raw_path + 'processed_reported/'+"cleaned_"+ fil.rsplit("/")[-1].split(".nc")[0] + ".csv",index=False)
            stats.buildLarge_Stats(
                df, sample, "tblGOSHIP_Reported", "cruise", transfer_flag="na"
            )
            # ingest_processed(df,'tblGOSHIP_Reported')
        except Exception as e:
            print(e)
            missed_file_list.append(fil)
    pd.Series(missed_file_list).to_csv("goship_sparse_missed_flist.csv", index=False)
