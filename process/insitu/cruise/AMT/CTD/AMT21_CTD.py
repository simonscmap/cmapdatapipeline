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

amt = "AMT21"
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
        var_name = full_var
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
                cols = ['Flag', 'Description']
                df_flag['combined'] = df_flag[cols].apply(lambda row: ': '.join(row.values.astype(str)), axis=1)  
                data_flag = 'Flag Description --' + '; '.join(df_flag['combined'])                           
            

df_orig_name.columns
df_orig_name['Description']
df.columns
df.dtypes
df.shape
df.head

df_data_meta = pd.read_csv(doi_list[0], sep='  - ', engine='python', index_col=0)

##Lingering rows of metadata at the start of new day's data
df = df[~df.Cruise.str.contains("History", na=False)]

for c in df.columns.tolist():
    if "yyyy-mm-dd" in c:
        df.rename(columns={c:"time"}, inplace = True)
        try:
            df["time"] = pd.to_datetime(df["time"], format="%d/%m/%Y %H:%M")
        except:
            try:
                df["time"] = pd.to_datetime(df["time"], format="%Y-%m-%d %H:%M:%S")
            except:
                df["time"] = pd.to_datetime(df["time"], format="%Y-%m-%dT%H:%M:%S.%f")
        print("Time column renamed")
        continue
    if "[" in c:
        new_column = c.split(" [",1)[0]   
        df.rename(columns={c:new_column}, inplace = True) 
        print(f"Renamed {c} to {new_column}")
        continue


df.rename(columns={"Latitude":"lat", "Longitude":"lon"}, inplace = True)
fill_cols = ['lat', 'lon', 'time', 'Station', 'Cruise', 'Bot. Depth','LOCAL_CDI_ID', 'EDMO_code', 'Type']
for f in fill_cols:
    df[f] = df[f].fillna(method='ffill')

df = fill_depth_if_missing(df, 'Bot. Depth')

dataset_meta, vars_meta = cmn.built_meta_DataFrame()
cruise_name = df["Cruise"].unique()[0]
dataset_meta["dataset_short_name"] = [f"{cruise_name}_{amt}_{data_type}"]
dataset_meta["dataset_version"] = ["Final"]
dataset_meta["climatology"] = [0]
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

## Data columns dropped contain no data or a single data flag
c_list = ['Cruise','Type', 'EDMO_code', 'Atten_red', 'QV:SEADATANET.2', 'chl-a_water_ISfluor_manufctrcal_sensor1', 'QV:SEADATANET.3','Trans_Red_25cm', 'QV:SEADATANET.7', 'WC_Potemp', 'QV:SEADATANET.8','SigTheta', 'QV:SEADATANET.11', 'Uncal_CTD_Temp', 'QV:SEADATANET.12', 'Uncal_CTD_Temp2', 'QV:SEADATANET.13','Attn_Red_25cm', 'QV:SEADATANET.16', 'WC_temp_CTD', 'QV:SEADATANET.17', 'ShortRed', 'QV:SEADATANET.18', 'AqVolt', 'QV:SEADATANET.19', 'Trans_Red_10cm', 'QV:SEADATANET.20','Atten', 'QV:SEADATANET.23', 'SubsurVPAR', 'QV:SEADATANET.24', 'Trans_Unspec', 'QV:SEADATANET.25', 'QV:SEADATANET:SAMPLE']
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
vars_meta["var_keywords"] ='Atlantic Meridional Transect, cruise, insitu, in-situ, in situ, observation, NERC'

var_join = df_meta.merge(df_bodc, how='left', left_on='bodc_code', right_on='BODC Code')
var_join.columns
for var in var_join['var_name'].values.tolist():
# for var in df_meta[2].values.tolist():
    if var in vars_meta.var_short_name.values.tolist():
        u = var_join.loc[var_join['var_name']==var,['var_unit']].values
        # u = df_meta.loc[df_meta[2]==var,[3]].values
        vars_meta.loc[vars_meta['var_short_name']==var,['var_unit']] = u
        vars_meta.loc[vars_meta['var_short_name']==var,['visualize']] = 1
        l =  var_join.loc[var_join['var_name']==var,['Description']].values
        # l =  df_meta.loc[df_meta[2]==var,[4]].values
        vars_meta.loc[vars_meta['var_short_name']==var,['var_long_name']] = l

vars_meta["var_comment"][
            vars_meta["var_short_name"].str.contains("QV:")
        ] = data_flag


cmn.combine_df_to_excel(
        f'{base_folder}tbl{amt}_{data_type}_qa.xlsx',
        df,
        dataset_meta,
        vars_meta,
    )