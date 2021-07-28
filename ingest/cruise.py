"""
Author: Norland Raphael Hagen <norlandrhagen@gmail.com>
Date: 07-23-2021

cmapdata - cruise - cruise metadata and trajectory functionallity. 
"""


import sys
import os
import shutil
import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import time

import vault_structure as vs
import common as cmn
import DB
import transfer
import region_classification as rc

##############################################
########## Cruise Helper Funcs ###############
##############################################


def build_cruise_metadata_from_user_input(df):
    """Attempts to build cruise_metadata dataframe from input trajectory metadata + user input. Recomended to ingest via cruise template instead.

    Args:
        df (Pandas DataFrame): Input dataset dataframe with time/lat/lon.

    Returns:
        Pandas Dataframe: Pandas dataframe for tblCruise ingestion.
        string: cruise_name

    """
    cruise_name = input("Please enter the cruise name. ie. KM1906: ")
    cruise_nickname = input("Please enter the cruise nickname. ie. Gradients 3: ")
    cruise_shipname = input("Please enter the cruise ship name. ie. Kilo Moana: ")
    chief_sci = input(
        "Please enter the name of the Chief Scientist. ie. Ginger Armbrust: "
    )
    time_min, time_max, lat_min, lat_max, lon_min, lon_max = ST_bounds_from_df(df)
    tblCruise_df = pd.DataFrame(
        {
            "Nickname": [cruise_nickname],
            "Name": [cruise_name],
            "Ship_Name": [cruise_shipname],
            "Start_Time": time_min,
            "End_Time": time_max,
            "Lat_Min": lat_min,
            "Lat_Max": lat_max,
            "Lon_Min": lon_min,
            "Lon_Max": lon_max,
            "Chief_Name": [chief_sci],
        }
    )
    return tblCruise_df, cruise_name


def return_cruise_trajectory_from_df(df, Cruise_ID):
    """Returns a Pandas DataFrame of time/lat/lon from input dataframe and cruise_ID"""
    cdf = df[["time", "lat", "lon"]]
    cdf.insert(loc=0, column="Cruise_ID", value=Cruise_ID[0])
    return cdf


def resample_trajectory(df, interval="1min"):
    """Resamples a cruise trajectory dataframe based on given interval. Default is one minute. Use cases are downsampling super high temporal resolution. ex. GPS.


    Args:
        df (Pandas DataFrame): Input dataset dataframe with time/lat/lon.
        interval (str, optional): valid pandas resample() interval. Defaults to "1min".

    Returns:
        Pandas DataFrame: Resampled Pandas DataFrame
    """
    df.index = pd.to_datetime(df.time)
    rs_df = df.resample(interval).mean()
    rs_df = rs_df.dropna()
    rs_df.reset_index(inplace=True)
    rs_df = rs_df[["Cruise_ID", "time", "lat", "lon"]]
    return rs_df


def vault_cruises():
    """Returns cruise dirs from r2r_cruise dir in vault_structure"""
    cruise_dirs = os.listdir(vs.r2r_cruise)
    return cruise_dirs


def retrieve_id_search(cmdf, id_col_str):
    id_return = cmdf[cmdf["id_col"].str.contains(id_col_str)]["info_col"].to_list()
    return id_return


def trim_returned_link(link_str):
    """webscraping helper func for triming strings"""
    if isinstance(link_str, str):
        link_str = [link_str]
    trimmed_link = [link.replace("<", "").replace(">", "") for link in link_str]
    return trimmed_link


def download_cruise_data_from_url(cruise_name, download_url_str, dataset_category):
    """webscraping helper function for downloading from url"""
    cruise_base_path = vs.r2r_cruise
    vs.makedir(cruise_base_path + cruise_name + "/")
    transfer.requests_Download(
        download_url_str,
        cruise_name + "_" + dataset_category + ".csv",
        cruise_base_path + cruise_name + "/",
    )


def add_ID_trajectory_df(trajectory_df, cruise_name, server):
    """Adds Cruise_ID column to a trajectory dataframe

    Args:
        trajectory_df (Pandas DataFrame): Input dataframe containing time/lat/lon cols.
        cruise_name (str): Valid CMAP cruise name (UNOLS ex. KM1906)
        Server (string): Valid CMAP server name

    Returns:
        Pandas DataFrame: Trajectory df with Cruise_ID column added
    """
    cruise_ID = cmn.get_cruise_IDS([cruise_name], server)
    trajectory_df["Cruise_ID"] = cruise_ID[0]
    trajectory_df = trajectory_df[["Cruise_ID", "time", "lat", "lon"]]
    return trajectory_df


def add_ST_cols_to_metadata_df(metadata_df, trajectory_df):
    """Adds Space-Time columns to metadata df from trajectory df

    Args:
        metadata_df (Pandas DataFrame): cruise specific metadata df for tblCruise
        trajectory_df (Pandas DataFrame): cruise specific trajectory df for tblCruise_Trajectory

    Returns:
        Pandas DataFrame: cruise specific metadata df for tblCruise with ST columns added.
    """
    time_min, time_max, lat_min, lat_max, lon_min, lon_max = ST_bounds_from_df(
        trajectory_df
    )
    metadata_df["Start_Time"] = time_min
    metadata_df["End_Time"] = time_max
    metadata_df.at[0, "Lat_Min"] = lat_min
    metadata_df.at[0, "Lat_Max"] = lat_max
    metadata_df.at[0, "Lon_Min"] = lon_min
    metadata_df.at[0, "Lon_Max"] = lon_max

    metadata_df = metadata_df[
        [
            "Nickname",
            "Name",
            "Ship_Name",
            "Start_Time",
            "End_Time",
            "Lat_Min",
            "Lat_Max",
            "Lon_Min",
            "Lon_Max",
            "Chief_Name",
            "Cruise_Series",
        ]
    ]
    return metadata_df


def ST_bounds_from_df(df):
    """Retrieve ST bounds from input dataframe with time/lat/lon"""
    time_min = np.min(df["time"])
    time_max = np.max(df["time"])
    lat_min = round(np.min(df["lat"]), 4)
    lat_max = round(np.max(df["lat"]), 4)
    lon_min = round(np.min(df["lon"]), 4)
    lon_max = round(np.max(df["lon"]), 4)
    return time_min, time_max, lat_min, lat_max, lon_min, lon_max


def fill_ST_bounds_metadata(cruise_name):
    """ST filler func for webscraping - old"""
    traj_path = vs.r2r_cruise + cruise_name + "/" + cruise_name + "_trajectory.csv"
    meta_path = vs.r2r_cruise + cruise_name + "/" + cruise_name + "_cruise_metadata.csv"
    meta_df = pd.read_csv(meta_path, sep=",")
    try:
        traj_df = pd.read_csv(traj_path, sep=",")
        traj_df["time"] = pd.to_datetime(traj_df["time"], errors="coerce").dt.strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    except:
        pass

    time_min, time_max, lat_min, lat_max, lon_min, lon_max = ST_bounds_from_df(traj_df)
    meta_df["Start_Time"] = time_min
    meta_df["End_Time"] = time_max
    meta_df.at[0, "Lat_Min"] = lat_min
    meta_df.at[0, "Lat_Max"] = lat_max
    meta_df.at[0, "Lon_Min"] = lon_min
    meta_df.at[0, "Lon_Max"] = lon_max
    meta_df.to_csv(meta_path, sep=",", index=False)


def update_tblCruises(server):
    """old webscraping updating func"""
    cruises_in_vault = cmn.lowercase_List(vault_cruises())
    DB_cruises = set(cmn.lowercase_List(cmn.getListCruises()["Name"].to_list()))
    new_cruises = sorted(list(set(cruises_in_vault) - set(DB_cruises)))
    for cruise in new_cruises:
        try:
            meta_df = cmn.nanToNA(
                pd.read_csv(
                    vs.r2r_cruise
                    + cruise.upper()
                    + "/"
                    + cruise.upper()
                    + "_cruise_metadata.csv"
                )
            )
            DB.lineInsert(
                server,
                "tblCruise",
                "(Nickname,Name,Ship_Name,Start_Time,End_Time,Lat_Min,Lat_Max,Lon_Min,Lon_Max,Chief_Name)",
                tuple(meta_df.iloc[0].astype(str).to_list()),
            )
            print(cruise, " Ingested into DB")

        except Exception as ex:
            print(ex, cruise, " not ingested...")


def get_cruise_data(cmdf, cruise_name):
    """old webscraping script"""
    try:
        cruise_data_links = retrieve_id_search(cmdf, "isr2r:hasCruiseof")
        trim_data_links = trim_returned_link(cruise_data_links)

        for data_link in trim_data_links:
            print(data_link)

            data = parse_r2r_page(data_link)
            return data
    except:
        pass


##############################################
########### Cruise Trajectory ################
##############################################
def get_cruise_traj(cmdf, cruise_name):
    """old webscraping trajectory downloading from r2r"""
    cruise_traj_best_str = """http://get.rvdata.us/cruise/{cruise_name}/products/r2rnav/{cruise_name}_bestres.r2rnav""".format(
        cruise_name=cruise_name
    )
    cruise_traj_1min_str = """http://get.rvdata.us/cruise/{cruise_name}/products/r2rnav/{cruise_name}_1min.r2rnav""".format(
        cruise_name=cruise_name
    )
    cruise_traj_control_points_str = """http://get.rvdata.us/cruise/{cruise_name}/products/r2rnav/{cruise_name}_control.r2rnav""".format(
        cruise_name=cruise_name
    )
    try:
        download_cruise_data_from_url(cruise_name, cruise_traj_1min_str, "trajectory")
        clean_cruise_traj(cruise_name)
    except:
        download_cruise_data_from_url(cruise_name, cruise_traj_best_str, "trajectory")
        clean_cruise_traj(cruise_name)
    else:
        download_cruise_data_from_url(
            cruise_name, cruise_traj_control_points_str, "trajectory"
        )
        clean_cruise_traj(cruise_name)


def clean_cruise_traj(cruise_name):
    """cleans cruise trajectory from r2r"""
    fpath = vs.r2r_cruise + cruise_name + "/" + cruise_name + "_trajectory.csv"
    try:
        df = pd.read_csv(
            fpath,
            skiprows=3,
            names=[
                "time",
                "lon",
                "lat",
                "Instantaneous Speed-over-ground",
                "Instantaneous Course-over-ground",
            ],
            sep="\t",
        )
    except:
        print(
            "Trajectory CSV download invalid or corrupted. Please manually check download link. Removing ship directory"
        )
        shutil.rmtree(vs.r2r_cruise + cruise_name + "/")

    df = df[["time", "lat", "lon"]]
    df["time"] = pd.to_datetime(df["time"], errors="coerce").dt.strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    dfr = resample_trajectory(df, interval="1min")
    dfr.to_csv(fpath, sep=",", index=False)


##############################################
#######  Cruise General Metadata   ###########
##############################################
def get_chief_sci(cmdf):
    """webscraping chief sci"""
    try:
        chief_sci_link = (
            retrieve_id_search(cmdf, "r2r:hasParticipant")[0]
            .replace("<", "")
            .replace(">", "")
        )
        chief_sci_df = parse_r2r_page(chief_sci_link)
        chief_sci = retrieve_id_search(chief_sci_df, "rdfs:label")
        chief_sci = chief_sci[0].split(" on")[0]
    except:
        chief_sci = ""
    return chief_sci


def get_cruise_metadata(cmdf, cruise_name):
    """webscraping r2r metadata"""
    try:
        cruise_name = retrieve_id_search(cmdf, "gl:hasCruiseID")[0]
    except:
        cruise_name = ""
    try:
        cruise_nickname = retrieve_id_search(cmdf, "dcterms:title")[0]
    except:
        cruise_nickname = ""
    try:
        cruise_shipname = retrieve_id_search(cmdf, "r2r:VesselName")[0]
    except:
        cruise_shipname = ""
    chief_sci = get_chief_sci(cmdf)
    format_cruise_metadata(cruise_name, cruise_nickname, cruise_shipname, chief_sci)


def format_cruise_metadata(cruise_name, cruise_nickname, cruise_shipname, chief_sci):
    """webscraping formatting"""
    cruise_name = cmn.empty_list_2_empty_str(cruise_name)
    cruise_nickname = cmn.empty_list_2_empty_str(cruise_nickname)
    cruise_shipname = cmn.empty_list_2_empty_str(cruise_shipname)

    fpath = vs.r2r_cruise + cruise_name + "/" + cruise_name + "_cruise_metadata.csv"
    tblCruise_df = pd.DataFrame(
        {
            "Nickname": [cruise_nickname],
            "Name": [cruise_name],
            "Ship_Name": [cruise_shipname],
            "Start_Time": "",
            "End_Time": "",
            "Lat_Min": "",
            "Lat_Max": "",
            "Lon_Min": "",
            "Lon_Max": "",
            "Chief_Name": [chief_sci],
        }
    )
    vs.makedir(vs.r2r_cruise + cruise_name + "/")
    tblCruise_df.to_csv(fpath, sep=",", index=False)


def gather_cruise_links():
    """webscraping"""
    all_cruise_url = "http://data.rvdata.us/directory/Cruise"
    page = requests.get(all_cruise_url)
    soup = BeautifulSoup(page.content, "html.parser")
    table_rows = soup.findAll("a")
    all_cruise_df = pd.DataFrame(columns=["cruise_name", "cruise_link"])
    for row in table_rows:
        if "/cruise/" in str(row):
            cruise_link = str(row).split("""href=\"""")[1].split('">')[0]
            cruise_name = cruise_link.split("cruise/")[1]
            all_cruise_df.loc[len(all_cruise_df)] = [cruise_name, cruise_link]
    return all_cruise_df


def parse_r2r_page(url):
    """webscraping"""
    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")
    table_rows = soup.findAll("tr")
    cmdf = pd.DataFrame(columns=["id_col", "info_col"])
    for tr in table_rows:
        td = tr.findAll("td")
        row = [tr.text for tr in td]
        rowlen = row + [""] * (2 - len(row))
        rowlen = [row.replace("\n", "").strip() for row in rowlen]
        cmdf.loc[len(cmdf)] = rowlen
    return cmdf


def parse_cruise_metadata(cruise_name="", cruise_url=""):
    """webscraping"""
    if cruise_name != "":
        cruise_url = "http://data.rvdata.us/page/cruise/" + cruise_name.upper()
    try:
        cmdf = parse_r2r_page(cruise_url)
        return cmdf

    except Exception as e:
        print(e)


def download_hot_cruises():
    """webscraping"""
    cruise_links = gather_cruise_links()
    for cruise_name, cruise_link in zip(
        cruise_links["cruise_name"], cruise_links["cruise_link"]
    ):

        try:
            cmdf = parse_cruise_metadata(cruise_name)
            cruise_name_str = (
                cmdf[cmdf["id_col"] == "dcterms:title"]["info_col"].iloc[0].lower()
            )
            if "hot" in cruise_name_str:
                get_cruise_metadata(cmdf, cruise_name)

            else:
                print(cruise_name_str, " NOT HOT")
        except:
            print("##########################")
            print(cruise_name, " No applicable cruise data -- cmdf empty")
            print("##########################")


def download_all_cruises():
    """webscraping"""
    cruise_links = gather_cruise_links()
    for cruise_name, cruise_link in zip(
        cruise_links["cruise_name"], cruise_links["cruise_link"]
    ):

        try:
            cmdf = parse_cruise_metadata(cruise_name)
            if not cmdf.empty:
                try:
                    get_cruise_traj(cmdf, cruise_name)
                    get_cruise_metadata(cmdf, cruise_name)
                    fill_ST_bounds_metadata(cruise_name)
                    print(cruise_name, " Downloaded")

                except:
                    print(
                        cruise_name,
                        " cruise data not downloaded b/c trajectory or metadata mising...",
                    )
        except:
            print("##########################")
            print(cruise_name, " No applicable cruise data -- cmdf empty")
            print("##########################")


def fill_ST_meta(cruise_meta_df, cruise_traj_df):
    """webscraping"""
    for cruise_name in cruise_meta_df["Name"].to_list():
        traj_df = cruise_traj_df[cruise_traj_df["cruise"] == cruise_name]
        time_min = np.min(traj_df["time"])
        time_max = np.max(traj_df["time"])
        lat_min = np.min(traj_df["lat"])
        lat_max = np.max(traj_df["lat"])
        lon_min = np.min(traj_df["lon"])
        lon_max = np.max(traj_df["lon"])
        cruise_meta_df.at[
            cruise_meta_df["Name"] == cruise_name, "Start_Time"
        ] = time_min
        cruise_meta_df.at[cruise_meta_df["Name"] == cruise_name, "End_Time"] = time_max
        cruise_meta_df.at[cruise_meta_df["Name"] == cruise_name, "Lat_Min"] = lat_min
        cruise_meta_df.at[cruise_meta_df["Name"] == cruise_name, "Lat_Max"] = lat_max
        cruise_meta_df.at[cruise_meta_df["Name"] == cruise_name, "Lon_Min"] = lon_min
        cruise_meta_df.at[cruise_meta_df["Name"] == cruise_name, "Lon_Max"] = lon_max
    return cruise_meta_df
