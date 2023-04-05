import os
import sys
import glob
import pandas as pd
import numpy as np
import seawater as sw
import re

sys.path.append("ingest")

from ingest import vault_structure as vs
from ingest import data_checks as dc
from ingest import transfer
from ingest import common as cmn

tbl = 'tblMGL1704_Gradients2'
raw_folder = f'{vs.cruise}{tbl}/raw/'

## Get dates from CTD summary file
slist = glob.glob(raw_folder +'*.sum')
df_sum = pd.read_csv(slist[0], skipinitialspace = True, header=None, skiprows=4, delim_whitespace = True, engine = 'python')

def dms2dd(s):
    # example: s = """26.16 N"""
    s = s.strip()
    hours, minutes, seconds, direction = re.split('[." "]+', s)
    dd = float(hours) + float(minutes)/60 + float(seconds)/(60*60)
    if direction in ('S','W'):
        dd*= -1
    return dd

df_sum.columns = ['stnnbr','cast','mnth','day','yr','time_only','lat_deg','lat_min','lat_dir','lon_deg','lon_min','lon_dir','depth','bottles']
df_sum['time']=pd.to_datetime(df_sum['mnth']+' '+df_sum['day'].astype(str)+' '+df_sum['yr'].astype(str)+df_sum['time_only'], format="%b %d %Y%H:%M:%S")
df_sum['lat'] = (df_sum['lat_deg'].astype(str) +" "+ df_sum['lat_min'].astype(str)+" "+ df_sum['lat_dir']).apply(dms2dd)
df_sum['lon'] = (df_sum['lon_deg'].astype(str) +" "+ df_sum['lon_min'].astype(str) +" "+ df_sum['lon_dir']).apply(dms2dd)

## Get CTD file
wc = glob.glob(raw_folder +'*.gof')
df_raw = pd.read_csv(wc[0], skipinitialspace = True, skiprows=1, delim_whitespace = True,skip_blank_lines=True, dtype=str, engine = 'python')
df_raw = df_raw.iloc[2:]
df_raw = df_raw.replace('-9',np.nan, regex=True)
## Fix header off due to space in CHL A.
df_raw.columns = ['STNNBR', 'CASTNO', 'ROSETTE', 'LAT', 'LON', 'CTDPRS', 'CTDTMP', 'CTDSAL', 'CTDOXY', 'CTDCHL', 'THETA', 'SIGMA', 'OXYGEN', 'DIC', 'ALKALIN', 'PH', 'PHSPHT', 'NO2+NO3', 'SILCAT', 'LLN', 'LLP', 'PC', 'PN', 'PP', 'PSi', 'CHLA',  'PHEO.', 'H.BACT', 'P.BACT', 'S.BACT', 'E.BACT', 'QUALT1', 'QUALT2', 'QUALT3', 'QUALT4','blank']


df_raw.reset_index(drop=True,level=0, inplace=True)
df_raw.reset_index(level=0, inplace=True)
df_raw['CTDPRS']=df_raw['CTDPRS'].astype(np.float64)
depth_df = pd.DataFrame(sw.dpth(df_raw['CTDPRS'], df_raw['LAT'].astype(np.float)))
depth_df.reset_index(level=0, inplace=True)
depth_df.columns=['index','depth']
df_raw = pd.merge(df_raw,depth_df, how='left', on='index')
df_raw = df_raw.loc[df_raw['depth'] >= 0 ]
qc_list = ['QUALT1','QUALT2','QUALT3','QUALT4']
for qc in qc_list:
       if len(df_raw[qc].unique()) == 1:
              df_raw = df_raw.drop([qc], axis=1)
              print(f'Dropped {qc}')

qc_col1 = df_raw.columns[7:14].to_list()
qc = df_raw['QUALT1'].astype(str).str.split('', expand=True)
qc.drop(columns=[0,8], inplace = True)
qc.columns = [c+'_Flag' for c in qc_col1]
qc.reset_index(level=0, inplace=True)

qc2 = df_raw['QUALT4'].astype(str).str.split('', expand=True)
qc2[2].drop_duplicates()
qc2 = qc2[[1,2]]
qc2.columns = ['CHLA_Flag', 'PHEO_Flag']
qc2.reset_index(level=0, inplace=True)

df_join = pd.merge(df_raw, qc, how='left', on='index')
df = pd.merge(df_join, qc2, how='left', on='index')
df = df.drop(["index"], axis=1)

df = dc.remove_blank_columns(df)
df.columns = df.columns.str.lower()
df.rename(columns={'pheo.':'pheo'}, inplace=True)

df_join = pd.merge(df, df_sum[['time','stnnbr','cast']].astype(str), how='left', left_on = ['stnnbr','castno'], right_on = ['stnnbr','cast'])

df_join = df_join[['time', 'lat', 'lon', 'depth', 'stnnbr', 'castno', 'rosette', 'ctdprs', 'ctdtmp',
       'ctdsal', 'ctdoxy', 'ctdchl', 'theta', 'sigma', 'oxygen', 'chla', 'pheo','ctdtmp_flag', 'ctdsal_flag',
       'ctdoxy_flag', 'ctdchl_flag', 'theta_flag', 'sigma_flag', 'oxygen_flag',
       'chla_flag','pheo_flag']]

df_join['lon'] = df_join['lon'].astype(np.float) * -1
df_join.to_excel(raw_folder+'tblMGL1704_Gradients2_data.xlsx', index = False)
df_join.to_csv('tblMGL1704_Gradients2_data.csv', index = False)

