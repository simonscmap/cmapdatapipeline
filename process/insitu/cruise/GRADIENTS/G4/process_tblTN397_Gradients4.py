import os
import sys
import glob
import pandas as pd
import numpy as np
import seawater as sw

sys.path.append("ingest")

from ingest import vault_structure as vs
from ingest import data


tbl = 'tblTN397_Gradients4'
raw_folder = f'{vs.cruise}{tbl}/raw/'

wc = glob.glob(raw_folder + '*xlsx')

df_raw = pd.read_excel(wc[0])
df_raw = df_raw.replace(-9, np.nan)

df_raw.reset_index(level=0, inplace=True)
df_raw.columns
##### CHANGE PRES TO FLOAT64
df_raw['CTDPRS']=df_raw['CTDPRS'].astype(np.float64)
depth_df = pd.DataFrame(sw.dpth(df_raw['CTDPRS'], df_raw['LAT']))
depth_df.reset_index(level=0, inplace=True)
depth_df.columns=['index','depth']
df_raw = pd.merge(df_raw,depth_df, how='left', on='index')
df_raw = df_raw.loc[df_raw['depth'] >= 0 ]
qc_list = ['QUALT1','QUALT2','QUALT3','QUALT4']
for qc in qc_list:
       if len(df_raw[qc].unique()) == 1:
              df_raw = df_raw.drop([qc], axis=1)
              print(f'Dropped {qc}')

qc_col1 = ['ctdtmp', 'ctdsal', 'ctdoxy', 'ctdflr', 'theta', 'sigma', 'oxygen']
qc = df_raw['QUALT1'].astype(str).str.split('', expand=True)
qc.drop(columns=[0,8], inplace = True)
qc.columns = [c+'_Flag' for c in qc_col1]
qc.reset_index(level=0, inplace=True)

qc2 = df_raw['QUALT4'].astype(str).str.split('', expand=True)
qc2 = qc2[[1]]
qc2.columns = ['Chla_Flag']
qc2.reset_index(level=0, inplace=True)

df_join = pd.merge(df_raw, qc, how='left', on='index')
df = pd.merge(df_join, qc2, how='left', on='index')
df = df.drop(["index"], axis=1)

df = data.remove_blank_columns(df)
df.columns = df.columns.str.lower()
df = df[['time', 'lat', 'lon', 'depth', 'stnnbr', 'castno', 'rosette', 'ctdprs', 'ctdtmp',
       'ctdsal', 'ctdoxy', 'ctdflr', 'theta', 'sigma', 'oxygen', 'chla', 'ctdtmp_flag', 'ctdsal_flag',
       'ctdoxy_flag', 'ctdflr_flag', 'theta_flag', 'sigma_flag', 'oxygen_flag',
       'chla_flag']]

df.to_excel(raw_folder+'tblTN397_Gradients4_data.xlsx')
df.to_csv('tblTN397_Gradients4_data.csv')

