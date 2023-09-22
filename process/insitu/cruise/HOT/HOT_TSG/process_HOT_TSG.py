"""              Thermosalinograph Data Format Document

                        March 16, 2015


Thermosalinograph data are distributed in a format specified by this
document.

The thermosalinograph data for each cruise are stored together with the 
navigation data in an ASCII file.  The file names are determined by cruise 
name and number.  For example, the thermosalinograph data for HOT-63 can 
be found in hot63ts.dat.

The thermosalinograph data files do not contain any header
information.  Only the data for each cruise are presented in the
files.  The order of variables in a thermosalinograph record are as
follows:  time (year, decimal year day), longitude, latitude,
temperature, salinity and quality.  Note, negative longitude
corresponds to West longitude.

   Data Record Format:

Column		     Variable
-------              -------
  1                  Year
  2                  Decimal Year Day (January 1 = Year Day 0)
  3                  Longitude (decimal degrees)
  4                  Latitude (decimal degrees)
  5                  Temperature (Degrees Celsius,
                     International Temperature Scale of 1990)
  6                  Salinity (1978 International Practical Salinity Scale)
  7                  Quality (defined by investigator) **

FORTRAN FORMAT  (i4, f10.5, f12.6, f11.6, f7.3, f7.3, i3)

**  The quality word is the left-to-right concatenation of required
quality bytes for temperature and salinity; the first byte represents
temperature, the second represents salinity.  Quality information is
only available for cruises after HOT-71.

note : The temperature from cruises up to HOT-268 on the R/V Kilo Moana 
      are flagged as uncalibrated (1) because the remote temperatures 
      were affected by heating from the nearby system's pump. These 
      temperatures are only corrected by a constant mean offset from the
      cruise's CTD casts.
      
       

The byte values are defined as follows:

 byte value        Definition
       1           Uncalibrated
       2           Acceptable measurement.
       3           Questionable measurement.
       4           Bad measurement.
 
Sample File: (First few records)

1996 142.40750 -157.994382  22.749950 25.313 34.877 23
1996 142.40762 -157.994536  22.749976 25.310 34.873 23
1996 142.40773 -157.994677  22.750001 25.309 34.876 23
1996 142.40785 -157.994831  22.750029 25.308 34.882 23"""

import sys
import pandas as pd
import glob
from tqdm import tqdm
sys.path.append("cmapdata/ingest")
sys.path.append("ingest")
import vault_structure as vs
import data
import cruise
import common as cmn
import DB

""" need to grab hot cruise ID from fname
1. combine TSG files in a dataframe

"""

tbl = 'tblHOT_TSG_Time_Series'
rep_folder = vs.cruise+f'{tbl}/rep/'
hot_tsg_flist = glob.glob(vs.cruise + f"{tbl}/raw/*.dat")
len(hot_tsg_flist)
# hot_tsg_flist.remove('/home/nrhagen/Vault/collected_data/HOT_TSG/tsg/hot100bts.dat')

## HOT071 and HOT063 split nans for flags
hot_tsg_flist[0]
fil = '/data/CMAP Data Submission Dropbox/Simons CMAP/vault/observation/in-situ/cruise/tblHOT_TSG_Time_Series/raw/hot71ts.dat'
fil = '/data/CMAP Data Submission Dropbox/Simons CMAP/vault/observation/in-situ/cruise/tblHOT_TSG_Time_Series/raw/hot280ts.dat'

df = pd.read_csv(fil, delim_whitespace=True)

combined_df_list = []
hot_cruise_list = []
for fil in tqdm(hot_tsg_flist):
    df = pd.read_csv(
        fil,
        delim_whitespace=True,
        names=[
            "year",
            "decimal_year_day",
            "lon",
            "lat",
            "temperature",
            "salinity",
            "quality_flag",
        ],
    )
    df['quality_flag'].fillna("", inplace=True)
    df["temperature_flag"] = df.quality_flag.astype(str).str[0]
    df["salinity_flag"] = df.quality_flag.astype(str).str[1:]
    df['temperature_flag'].fillna("", inplace=True)
    df['salinity_flag'].fillna("", inplace=True)
    hot_cruise = fil.split("raw/")[1].split(".dat")[0].split("ts")[0]
    if "hot" in hot_cruise:
        cruise_id = "HOT" + hot_cruise.split("hot")[1].zfill(3)
    elif "ac" in hot_cruise:
        cruise_id = "AC" + hot_cruise.split("ac")[1].zfill(3)
    elif "ha" in hot_cruise:
        cruise_id = "HA" + hot_cruise.split("ha")[1].zfill(3)
    elif "bts" in hot_cruise:
        cruise_id = "HOT" + hot_cruise.split("hot")[1].zfill(3) + "b"
    hot_cruise_list.append(cruise_id)
    df["cruise"] = cruise_id
    df["time"] = (
        pd.to_datetime(df.year, format="%Y")
        + pd.to_timedelta(df["decimal_year_day"], unit="d")
    )
    df = df[
        [
            "time",
            "lat",
            "lon",
            "temperature",
            "salinity",
            "temperature_flag",
            "salinity_flag",
            "cruise",
        ]
    ]
    df = data.clean_data_df(df)
    df = data.removeMissings(df, data.ST_columns(df))
    combined_df_list.append(df)
combined_df = pd.concat(combined_df_list, axis=0, ignore_index=True)
combined_df.to_csv(rep_folder + "HOT_TSG_data.csv", index=False)
# return hot_cruise_list


## Check cruises
df_test = combined_df.loc[combined_df['cruise']=='HOT280']

def insert_cruise_into_db(df):
    DB.toSQLbcp_wrapper(df, "tblCruise_Trajectory", "Rainier")
    DB.toSQLbcp_wrapper(df, "tblCruise_Trajectory", "Mariana")


"""hot cruise trajectory"""


def remove_hot_tsg_cruise_trajectories_from_db():
    unique_cruises = list(df["cruise"].unique())
    for ucruise in unique_cruises:
        Cruise_ID = cmn.get_cruise_IDS([ucruise])
        if Cruise_ID:
            print(f"removing cruise_ID {Cruise_ID}, and {ucruise}")
            Cruise_ID = Cruise_ID[0]
            cmnd = (
                f"""DELETE FROM tblCruise_Trajectory WHERE [Cruise_ID] = {Cruise_ID}"""
            )
            DB.DB_modify(cmnd, "Rainier")
            DB.DB_modify(cmnd, "Mariana")


def ingest_hot_cruise_trajectories():
    df = pd.read_csv(rep_folder + "HOT_TSG_data.csv")
    unique_cruises = list(df["cruise"].unique())
    for ucruise in tqdm(unique_cruises):
        Cruise_ID = cmn.get_cruise_IDS([ucruise])
        if Cruise_ID:
            print(ucruise)
            Cruise_ID = Cruise_ID[0]
            cdf = df[df["cruise"] == ucruise]
            cdf["Cruise_ID"] = int(Cruise_ID)
            cdf = cdf[["Cruise_ID", "time", "lat", "lon"]]
            cdf_resample = cruise.resample_trajectory(cdf)
            cdf_resample = cdf_resample.sort_values(by=["time"])
            insert_cruise_into_db(cdf_resample)

import numpy as np
df.sort_values('time', inplace=True)
server = 'rossby'
tbl = 'tblHOT_TSG_Time_Series'
qry = f"truncate table {tbl}"
DB.DB_modify(qry, server)
DB.toSQLbcp_wrapper(df, tbl, server)

import stats
import credentials as cr
stats_df = stats.build_stats_df_from_db_calls(tbl, server, server)
for server in cr.server_alias_list:
    stats.update_stats_large(tbl, stats_df, 'opedia', server)

# cruise_min_max = df.groupby(["cruise"]).agg({'time': [np.min,np.max]})
# cruise_min_max = df.groupby("cruise")['time'].agg(['min','max','count'])
# cruise_min_max.to_excel(rep_folder+'cruise_min_max.xlsx')

# hot_310_11 = df[df['cruise'].isin(['HOT310','HOT311'])]
# hot_210 = df[df['cruise'].isin(['HOT269'])]
# hot_210['n'] = np.where(hot_210[['time','lat','lon']].isnull().any(1), 1, 0) 
# hot_210.loc[hot_210['n']==1]
# hot_210.dropna(subset=['time','lat','lon'])

# df.dropna(subset=['time','lat','lon']) # 35,355,187, 35,355,244 original