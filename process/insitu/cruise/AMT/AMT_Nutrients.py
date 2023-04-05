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

sys.path.append("ingest")

import vault_structure as vs
import DB
import data_checks as dc
import common as cmn
import stats

amt_list = ["AMT17", "AMT06"]
amt=amt_list[1]

def built_meta_DataFrame():
    dataset_meta = pd.DataFrame(
        columns=[
            "dataset_short_name",
            "dataset_long_name",
            "dataset_version",
            "dataset_release_date",
            "dataset_make",
            "dataset_source",
            "dataset_distributor",
            "dataset_acknowledgement",
            "dataset_history",
            "dataset_description",
            "dataset_references",
            "climatology",
            "cruise_names",
        ]
    )
    vars_meta = pd.DataFrame(
        columns=[
            "var_short_name",
            "var_long_name",
            "var_sensor",
            "var_unit",
            "var_spatial_res",
            "var_temporal_res",
            "var_discipline",
            "visualize",
            "var_keywords",
            "var_comment",
        ]
    )
    return dataset_meta, vars_meta

bottleflag = "Flag Description -- 0: No problem reported; 1: Filter burst; 2: Leakage contamination; 3: Bottle misfire; 4: Bottles fired in incorrect order; 5: Bottle leak; 6: Partial sample loss; 7: No sample; 8: Questionable depth; 9: Vent left open"

dataflag = "Flag Description -- <: Below detection limit; >: In excess of quoted value; A: Taxonomic flag for affinis (aff.); B: Beginning of CTD Down/Up Cast; C: Taxonomic flag for confer (cf.); D: Thermometric depth; E: End of CTD Down/Up Cast; G: Non-taxonomic biological characteristic uncertainty; H: Extrapolated value; I: Taxonomic flag for single; species (sp.); K: Improbable value - unknown quality control source; L: Improbable value - originator's quality control; M: Improbable value - BODC quality control; N: Null value; O: Improbable value - user quality control; P: Trace/calm; Q: Indeterminate; R: Replacement value; S: Estimated value; T: Interpolated value; U:; Uncalibrated; W: Control value; X: Excessive difference"

def combine_df_to_excel(filename, df, dataset_metadata, vars_metadata):
    writer = pd.ExcelWriter(filename, engine="xlsxwriter")
    df.to_excel(writer, sheet_name="data", index=False)
    dataset_metadata.to_excel(writer, sheet_name="dataset_meta_data", index=False)
    vars_metadata.to_excel(writer, sheet_name="vars_meta_data", index=False)
    writer.save()

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

tbl = f"tbl{amt}_Nutrients"
vs.leafStruc(vs.cruise+tbl)

base_folder = f"{vs.cruise}{tbl}/raw/"
rep_folder = f"{vs.cruise}{tbl}/rep/"
code_folder= f"{vs.cruise}{tbl}/code/"

collected = f"{vs.collected_data}/insitu/cruise/AMT/{amt}/Nutrients"

zip_list = glob.glob(os.path.join(collected, "*.zip"))

with zipfile.ZipFile(zip_list[0], "r") as zip_ref:
    zip_ref.extractall(base_folder)

csv_list = glob.glob(os.path.join(base_folder, "*.csv"))
bot_list = glob.glob(os.path.join(base_folder, "*.out"))
html_list = glob.glob(os.path.join(base_folder, "*.htm*"))
txt_list = glob.glob(os.path.join(base_folder, "*.htm*"))
html_var = [i for i in html_list if 'metadata' in i or 'documentation' in i]
doi_list = glob.glob(os.path.join(base_folder, "*.ris"))
df = pd.read_csv(bot_list[0])
df = pd.read_csv(csv_list[0])

## Two table headers read as multi index, skip first one
# df_meta = pd.read_html(html_list[0]), header=1)
# ## Pandas returns a list of dfs
# df_meta[0].columns
df_data_meta = pd.read_csv(doi_list[0], sep='  - ', engine='python', index_col=0)
# df_dmeta = pd.read_csv(doi_list[0], sep=' - ', engine='python')
# df_dmeta.columns=df_dmeta.columns.str.replace(' ','')
# df_dmeta['TY'] = df_dmeta['TY'].str.strip()
# df_dmeta.set_index('TY', drop=True, inplace=True)

with open(html_var[0],"r") as f:
    soup = bs.BeautifulSoup(f, 'lxml')
    parsed_table = soup.find_all('table')[0] 
    cruise_link = [row.a['href'] if row.find('a') else 
             ''.join(row.stripped_strings)
            for row in parsed_table.find_all('th')][0]
    data = [[td.a['href'] if td.find('a') else 
             ''.join(td.stripped_strings)
             for td in row.find_all('td')]
            for row in parsed_table.find_all('tr')]
    df_meta = pd.DataFrame(data[2:])


for c in df.columns.tolist():
    if c == "yyyy-mm-ddThh24:mi:ss[GMT]":
        df.rename(columns={c:"time"}, inplace = True)
        try:
            df["time"] = pd.to_datetime(df["time"], format="%d/%m/%Y %H:%M")
        except:
            df["time"] = pd.to_datetime(df["time"], format="%Y-%m-%d %H:%M:%S")
        print("Time column renamed")
        continue
    if "[" in c:
        new_column = c.split("[",1)[0]   
        df.rename(columns={c:new_column}, inplace = True) 
        print(f"Renamed {c} to {new_column}")
        continue
    if len(df[c].unique().tolist())==1 and ('ODV' in c or 'Site' in c):
        df.drop(columns={c}, inplace=True)
        print(f"Dropped {c}")

cruise_name = df["Cruise"].unique()[0]
dataset_meta, vars_meta = built_meta_DataFrame()
dataset_meta["dataset_short_name"] = [f"{cruise_name}_{amt}_Nutrients"]
dataset_meta["dataset_version"] = ["Final"]
dataset_meta["dataset_make"] = ["observation"]
dataset_meta["dataset_source"] = ["Atlantic Meridional Transect (AMT)"]
dataset_meta["dataset_distributor"] = ["British Oceanographic Data Centre (BODC)"]
dataset_meta["dataset_description"] = [df_data_meta.loc['AB','DATA']]
dataset_meta["dataset_release_date"] = [df_data_meta.loc['DA','DATA']]
dataset_meta["dataset_long_name"] = [df_data_meta.loc['TI','DATA']]
dataset_meta["dataset_references"] = [df_data_meta.loc['DO','DATA']]
dataset_meta["dataset_acknowledgement"] = [df_meta.loc[0,6]+'. Contains data supplied by the Natural Environment Research Council.']
dataset_meta["cruise_names"] = [cruise_name]
dataset_meta["dataset_source"] = [cruise_link]


df = fill_depth_if_missing(df, 'Bot_depth')
df.rename(columns={"Latitude":"lat", "Longitude":"lon"}, inplace = True)

df = dc.sort_values(df,dc.ST_columns(df))
col_list = [i for i in df.columns.tolist() if i not in ["time", "lat", "lon", "depth"]]
df = df[dc.ST_columns(df)+col_list]

##Add if condition
c_list = ['Cruise', 'Gear', 'Bot_Ref']
for c in c_list:
    if len(df[c].unique().tolist())==1:
        df.drop(columns={c}, inplace=True)
        print(f"Dropped {c}")

var_df = df.drop(columns = dc.ST_columns(df))

vars_meta["var_short_name"] = list(var_df)
vars_meta["var_sensor"] = 'CTD'
vars_meta["var_spatial_res"] = 'Irregular'
vars_meta["var_temporal_res"] = 'Irregular'
vars_meta["var_discipline"] = 'Biogeochemistry'

var = 'NTRIAATX'

for var in df_meta[0].values.tolist():
# for var in df_meta[2].values.tolist():
    if var in vars_meta.var_short_name.values.tolist():
        u = df_meta.loc[df_meta[0]==var,[3]].values
        # u = df_meta.loc[df_meta[2]==var,[3]].values
        vars_meta.loc[vars_meta['var_short_name']==var,['var_unit']] = u
        vars_meta.loc[vars_meta['var_short_name']==var,['visualize']] = 1
        l =  df_meta.loc[df_meta[0]==var,[4]].values
        # l =  df_meta.loc[df_meta[2]==var,[4]].values
        vars_meta.loc[vars_meta['var_short_name']==var,['var_long_name']] = l

vars_meta["var_comment"][
            vars_meta["var_short_name"].str.contains("QV:")
        ] = dataflag
vars_meta["var_comment"][
            vars_meta["var_short_name"].str.contains("_Flag")
        ] = bottleflag

combine_df_to_excel(
        f'{base_folder}tbl{amt}_Nutrients_qa.xlsx',
        df,
        dataset_meta,
        vars_meta,
    )