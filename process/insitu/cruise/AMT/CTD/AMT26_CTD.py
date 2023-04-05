import sys
import os
import pandas as pd
pd.options.mode.chained_assignment = None  # default="warn"
import glob 
import numpy as np
import shutil
from tqdm import tqdm 
import zipfile
import bs4 as bs

sys.path.append("cmapdata/ingest")

import vault_structure as vs
import DB
import data_checks as dc
import common as cmn
import stats

amt = "AMT26"
data_type = 'CTD'


def fill_depth_if_missing(df, depth_replace_col):
    """This function fills the depth column with depth_below_surface if it is missing"""
    if depth_replace_col in list(df):
        if "depth" not in list(df):
            all_depth_neg = all(
                df[depth_replace_col] < 0
            )  # all values are above sea surface..
            if (
                all_depth_neg == False
            ):  # some vals may be outliers, but will be used for depth
                df = df[df[depth_replace_col] >= 0]
                df["depth"] = df[depth_replace_col]
            if df['depth'].equals(df[depth_replace_col]):
                df.drop(columns=[depth_replace_col], inplace=True)
                print("No updates made to depth, original column dropped")
    return df

def soup_header(header_str):
    if header_str in t.find('th').stripped_strings:
                    data = [[td.a['href'] if td.find('a') else 
                        ''.join(td.stripped_strings)
                        for td in row.find_all('td')]
                        for row in t.find_all('tr')]
                    cols = [[th.a['href'] if th.find('a') else 
                        ''.join(th.stripped_strings)
                        for th in row.find_all('th')]
                        for row in t.find_all('tr')]
                    df = pd.DataFrame(data[1:], columns=cols[0])
    return df

tbl = f"tbl{amt}_{data_type}"
vs.leafStruc(vs.cruise+tbl)

base_folder = f"{vs.cruise}{tbl}/raw/"
rep_folder = f"{vs.cruise}{tbl}/rep/"
code_folder= f"{vs.cruise}{tbl}/code/"

collected = f"{vs.collected_data}/insitu/cruise/AMT/{amt}/{data_type}"

zip_list = glob.glob(os.path.join(collected, "*.zip"))

with zipfile.ZipFile(zip_list[0], "r") as zip_ref:
    zip_ref.extractall(base_folder)

all_files =glob.glob(os.path.join(base_folder, "*")) 

html_list = glob.glob(os.path.join(base_folder, "*.htm*"))

doi_list = glob.glob(os.path.join(base_folder, "*.ris"))


txt_list = glob.glob(os.path.join(base_folder, "*.csv"))

df = pd.read_csv(txt_list[0], skiprows=skip, engine='python')

df_meta = pd.DataFrame()

df.columns
df.dtypes
df.shape
df.head

df_data_meta = pd.read_csv(doi_list[0], sep='  - ', engine='python', index_col=0)

for c in df.columns.tolist():
    if "yyyy-mm-dd" in c:
        df.rename(columns={c:"time"}, inplace = True)
        try:
            df["time"] = pd.to_datetime(df["time"], format="%d/%m/%Y %H:%M")
        except:
            df["time"] = pd.to_datetime(df["time"], format="%Y-%m-%d %H:%M:%S")
        print("Time column renamed")
        continue
    if "[" in c:
        new_column = c.split(" [",1)[0]   
        df.rename(columns={c:new_column}, inplace = True) 
        print(f"Renamed {c} to {new_column}")
        continue
    if len(df[c].unique().tolist())==1 and ('ODV' in c or 'Site' in c or 'EDMO_code' in c or 'Instrument' in c):
        df.drop(columns={c}, inplace=True)
        print(f"Dropped {c}")

df.rename(columns={"Latitude":"lat", "Longitude":"lon"}, inplace = True)
fill_cols = ['lat', 'lon', 'time', 'Station', 'Cruise']
for f in fill_cols:
    df[f] = df[f].fillna(method='ffill')

dataset_meta, vars_meta = cmn.built_meta_DataFrame()
cruise_name = df["Cruise"].unique()[0]
dataset_meta["dataset_short_name"] = [f"{cruise_name}_{amt}_{data_type}"]
dataset_meta["dataset_version"] = ["Final"]
dataset_meta["dataset_make"] = ["observation"]
dataset_meta["dataset_source"] = ["Atlantic Meridional Transect (AMT)"]
dataset_meta["dataset_distributor"] = ["British Oceanographic Data Centre (BODC)"]
dataset_meta["dataset_description"] = [df_data_meta.loc['AB','DATA']]
dataset_meta["dataset_release_date"] = [df_data_meta.loc['DA','DATA']]
dataset_meta["dataset_long_name"] = [df_data_meta.loc['TI','DATA']]
dataset_meta["dataset_references"] = [df_data_meta.loc['DO','DATA']]
dataset_meta["dataset_acknowledgement"] = [df_data_meta.loc['AU','DATA']+'. Contains data supplied by the Natural Environment Research Council.']

dataset_meta["cruise_names"] = [cruise_name]
dataset_meta["dataset_source"] = [cruise_link]


df = dc.sort_values(df,dc.ST_columns(df))
col_list = [i for i in df.columns.tolist() if i not in ["time", "lat", "lon", "depth"]]
df = df[dc.ST_columns(df)+col_list]

c_list = ['Cruise','Type']
for c in c_list:
    # if len(df[c].unique().tolist())==1:
    df.drop(columns={c}, inplace=True)
    print(f"Dropped {c}")

var_df = df.drop(columns = dc.ST_columns(df))

vars_meta["var_short_name"] = list(var_df)
vars_meta["var_sensor"] = 'CTD'
vars_meta["var_spatial_res"] = 'Irregular'
vars_meta["var_temporal_res"] = 'Irregular'
vars_meta["var_discipline"] = 'Biogeochemistry'


vars_meta["var_comment"][
            vars_meta["var_short_name"].str.contains("QV:")
        ] = data_flag


cmn.combine_df_to_excel(
        f'{base_folder}tbl{amt}_{data_type}_qa.xlsx',
        df,
        dataset_meta,
        vars_meta,
    )