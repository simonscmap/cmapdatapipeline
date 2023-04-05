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

amt_list = ["AMT26", "AMT22","AMT21","AMT20","AMT19"]
amt = amt_list[1]
data_type = 'CTD'


bottleflag = "Flag Description -- 0: No problem reported; 1: Filter burst; 2: Leakage contamination; 3: Bottle misfire; 4: Bottles fired in incorrect order; 5: Bottle leak; 6: Partial sample loss; 7: No sample; 8: Questionable depth; 9: Vent left open"

dataflag = "Flag Description -- <: Below detection limit; >: In excess of quoted value; A: Taxonomic flag for affinis (aff.); B: Beginning of CTD Down/Up Cast; C: Taxonomic flag for confer (cf.); D: Thermometric depth; E: End of CTD Down/Up Cast; G: Non-taxonomic biological characteristic uncertainty; H: Extrapolated value; I: Taxonomic flag for single; species (sp.); K: Improbable value - unknown quality control source; L: Improbable value - originator's quality control; M: Improbable value - BODC quality control; N: Null value; O: Improbable value - user quality control; P: Trace/calm; Q: Indeterminate; R: Replacement value; S: Estimated value; T: Interpolated value; U:; Uncalibrated; W: Control value; X: Excessive difference"


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
csv_list = glob.glob(os.path.join(base_folder, "*.csv"))

html_list = glob.glob(os.path.join(base_folder, "*.htm*"))

html_var = [i for i in html_list if 'metadata' in i or 'documentation' in i]
doi_list = glob.glob(os.path.join(base_folder, "*.ris"))

if 'AMT22' in amt:
    txt_list = glob.glob(os.path.join(base_folder, "*.txt"))
    with open(txt_list[0], encoding='utf-8') as readfile:
            ls_readfile = readfile.readlines()
            #Find the skiprows number with ID as the startswith
            skip = next(filter(lambda x: x[1].startswith('Cruise'), enumerate(ls_readfile)))[0]
            meta = []
            for row in ls_readfile[:skip]:
                if 'DataVariable' in row:
                    meta.append(row)
            print(skip)

    df = pd.read_csv(txt_list[0], sep='\t', skiprows=skip, engine='python')

    df_meta = pd.DataFrame()
    for m in meta:
        full_var = m.split('label="',1)[1].rsplit('" value_type',1)[0]
        if '[' in full_var:
            var_name = full_var.split(' [')[0]
            var_unit = full_var.split(' [')[1].replace(']','')
        else:
            var_name = ''
            var_unit = ''
        codes = m.split('Codes: ',1)[1].rsplit('"</DataVariable>',1)[0]
        bodc_code = codes.split('::',1)[1].split(' ',1)[0]
        d = {'full_var':[full_var],'var_name':[var_name],'var_unit':[var_unit], 'bodc_code':[bodc_code]}    
        temp_df = pd.DataFrame(data=d)
        df_meta = df_meta.append(temp_df, ignore_index = True)
    
    with open(html_list[0],"r") as f:
        soup = bs.BeautifulSoup(f, 'lxml')
        parsed_table = soup.find_all('table')
        parsed_pg = soup.find_all('p')
        for p in parsed_pg:
            if 'Cruise Summary Report' in str(p):
                cruise_link = p.find_all('a', href=True)[0]['href']
        for t in parsed_table:
            if not t.find('th'):
                continue
            else: 
                if "BODC Code" in t.find('th').stripped_strings:
                    df_bodc = soup_header("BODC Code")  
                if "Originator's Parameter Name" in t.find('th').stripped_strings:
                    df_orig_name = soup_header("Originator's Parameter Name")  
                if "Flag" in t.find('th').stripped_strings:    
                    df_flag = soup_header("Flag")                                     
                


# df = pd.read_csv(csv_list[0], engine='python')

df.columns
df.dtypes
df.shape
df.head

## Two table headers read as multi index, skip first one
# df_meta = pd.read_html(html_list[0]), header=1)
len(df_html)
df_metavar_1 = df_html[7]
df_metavar_2 = df_html[372]
df_flag = df_html[373]
# ## Pandas returns a list of dfs
# df_meta[0].columns
df_data_meta = pd.read_csv(doi_list[0], sep='  - ', engine='python', index_col=0)
# df_dmeta = pd.read_csv(doi_list[0], sep=' - ', engine='python')
# df_dmeta.columns=df_dmeta.columns.str.replace(' ','')
# df_dmeta['TY'] = df_dmeta['TY'].str.strip()
# df_dmeta.set_index('TY', drop=True, inplace=True)
df_dmeta.columns
df.columns
df.head
df.shape

with open(html_list[0],"r") as f:
    soup = bs.BeautifulSoup(f, 'lxml')
    parsed_table = soup.find_all('table')[0] 
    cruise_link = [row.a['href'] if row.find('a') else 
             ''.join(row.stripped_strings)
            for row in parsed_table.find_all('th')][0]
    data = [[td.a['href'] if td.find('a') else 
             ''.join(td.stripped_strings)
             for td in row.find_all('td')]
            for row in parsed_table.find_all('tr')]
    df_html = pd.DataFrame(data[2:])


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
    if len(df[c].unique().tolist())==1 and ('ODV' in c or 'Site' in c):
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


df = fill_depth_if_missing(df, 'Bot. Depth')


df = dc.sort_values(df,dc.ST_columns(df))
col_list = [i for i in df.columns.tolist() if i not in ["time", "lat", "lon", "depth"]]
df = df[dc.ST_columns(df)+col_list]

##Add if condition
c_list = ['Cruise','TYPE']
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
        f'{base_folder}tbl{amt}_{data_type}_qa.xlsx',
        df,
        dataset_meta,
        vars_meta,
    )