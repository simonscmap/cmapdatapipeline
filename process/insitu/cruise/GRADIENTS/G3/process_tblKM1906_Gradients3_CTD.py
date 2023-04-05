
import os
import sys
import glob
import pandas as pd
import numpy as np
import seawater as sw
pd.set_option('max_columns', None)
pd.set_option('display.expand_frame_repr', True)
from io import StringIO
import re

sys.path.append("ingest")

from ingest import vault_structure as vs
from ingest import data_checks as dc
from ingest import common as cmn

tbl = 'tblKM1906_Gradients3_CTD'
raw_folder = f'{vs.cruise}{tbl}/raw/'

## Only process down casts
flist = glob.glob(raw_folder +'*dn.ctd')
len(flist)

def dms2dd(s):
    # example: s = """26.16 N"""
    s = s.strip()
    hours, minutes, seconds, direction = re.split('[." "]+', s)
    dd = float(hours) + float(minutes)/60 + float(seconds)/(60*60)
    if direction in ('S','W'):
        dd*= -1
    return dd

## Get dates from CTD summary file
slist = glob.glob(raw_folder +'*.sum')

df_sum = pd.read_csv(slist[0], skipinitialspace = True, header=None, skiprows=4, delim_whitespace = True, engine = 'python')


df_sum.columns = ['stnnbr','cast','mnth','day','yr','time_only','lat_deg','lat_min','lat_dir','lon_deg','lon_min','lon_dir','depth','bottles']
df_sum['time']=pd.to_datetime(df_sum['mnth']+df_sum['day'].astype(str)+df_sum['yr'].astype(str)+df_sum['time_only'], format="%b%d%Y%H:%M:%S")
df_sum['lat'] = (df_sum['lat_deg'].astype(str) +" "+ df_sum['lat_min'].astype(str)+" "+ df_sum['lat_dir']).apply(dms2dd)
df_sum['lon'] = (df_sum['lon_deg'].astype(str) +" "+ df_sum['lon_min'].astype(str) +" "+ df_sum['lon_dir']).apply(dms2dd)


# cols = ['CTDPRS', 'CTDTMP', 'CTDSAL', 'CTDOXY', 'PAR', 'FLUOR', 'BEAM.AT', 'XMISS', 'NUMBER', 'QUALT1']

cols = ['Pressure', 'Temperature', 'Salinity', 'Oxygen', 'PAR', 'Fluor', 'Beam_Attenuation', 'Beam_Transmission', 'Observations', 'QUALT1']
df = pd.DataFrame(columns={'Pressure', 'Temperature', 'Salinity', 'Oxygen', 'PAR', 'Fluor', 'Beam_Attenuation', 'Beam_Transmission', 'Observations', 'QUALT1','Pressure_Flag', 'Temperature_Flag', 'Salinity_Flag', 'Oxygen_Flag', 'PAR_Flag', 'Fluor_Flag', 'Beam_Attenuation_Flag', 'Beam_Transmission_Flag','station','cast'})



for fil in flist:
    temp = pd.read_csv(fil, skipinitialspace = True, header=None, skiprows=3, delim_whitespace = True, engine = 'python')
    temp.columns = cols
    qc = temp['QUALT1'].astype(str).str.split('', expand=True)
    qc.drop(columns=[9,0], inplace = True)
    qc.columns = [c+'_Flag' for c in cols[:-2]]
    combo = pd.concat([temp, qc], axis=1)
    combo['station'] = int(fil.rsplit('/',1)[1].split('s')[1].split('c')[0])
    combo['cast'] = int(fil.rsplit('/',1)[1].split('s')[1].split('c')[1].split('d')[0])
    df = df.append(combo, ignore_index=True)


df_join = pd.merge(df, df_sum[['time','lat','lon','stnnbr','cast']], how='left', left_on = ['station','cast'], right_on = ['stnnbr','cast'])
df_join.columns

df_join.reset_index(level=0, inplace=True)
##### CHANGE PRES TO FLOAT64
df_join['Pressure']=df_join['Pressure'].astype(np.float64)
depth_df = pd.DataFrame(sw.dpth(df_join['Pressure'], df_join['lat']))
depth_df.reset_index(level=0, inplace=True)
depth_df.columns=['index','depth']
df_join = pd.merge(df_join,depth_df, how='left', on='index')
df_join = df_join.loc[df_join['depth'] >= 0 ]

df_join = df_join[['time', 'lat', 'lon', 'depth', 'station', 'cast','Pressure', 'Temperature', 'Salinity', 'Oxygen', 'PAR', 'Fluor', 'Beam_Attenuation', 'Beam_Transmission', 'Observations', 'Pressure_Flag', 'Temperature_Flag', 'Salinity_Flag', 'Oxygen_Flag', 'PAR_Flag', 'Fluor_Flag', 'Beam_Attenuation_Flag', 'Beam_Transmission_Flag']]
df_join.to_excel(raw_folder+'tbllKM1906_Gradients3_ctd_data.xlsx', index = False)
df_join.to_csv('tbllKM1906_Gradients3_ctd_data.csv', index = False)

df_join = pd.read_excel(raw_folder+'tbllKM1906_Gradients3_ctd_data.xlsx')