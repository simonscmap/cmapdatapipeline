import sys
import os
import pandas as pd
import numpy as np
import glob

sys.path.append("cmapdata/ingest")
from ingest import vault_structure as vs
from ingest import DB 
from ingest import common as cmn 

tbl = 'tblGeotraces_Sensor'
cruise_list = ['JC150','JC156'] 
cruise_list = ['JC156', 'ISSS-08', 'PS70', 'AU0703', 'MD166', 'PS71', 'AU0806', '2012-13', '2014-19', 'GEOVIDE', 'HLY1502', 'RR1815', 'RR1814 | RR1815', 'IN2016_V01', 'IN2016_V02', 'JC057', 'KH09-05', 'KH11-07', '2013-18', 'KH12-04', 'KH15-03', 'KN199', 'KN204', 'M81_1', 'MD188', 'MedSeA2013', 'PE319', 'PE321', 'PE370', 'PE373', 'PE374', 'PEACETIME', 'PS100', 'PS94', 'SK324', 'SS01_10', 'SS2011', 'TAN1109', 'RR1814']
cruise_list = ['NBP1409', 'SO245']

raw_folder = f'{vs.cruise}{tbl}/raw/'
meta_xls = f'{vs.cruise}{tbl}/metadata/GEOTRACES_cruise_meta_data.xlsx'
txt_list = glob.glob(raw_folder+"*txt")

with open(txt_list[1], encoding='utf-8') as readfile:
        ls_readfile = readfile.readlines()
        #Find the skiprows number with ID as the startswith
        skip = next(filter(lambda x: x[1].startswith('Cruise'), enumerate(ls_readfile)))[0]
        meta = []
        for row in ls_readfile[:skip]:
            if 'DataVariable' in row:
                meta.append(row)
        print(skip)

df = pd.read_csv(txt_list[1], sep='\t', skiprows=skip, engine='python')
df.columns

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

df.rename(columns={"Latitude":"lat", "Longitude":"lon"}, inplace = True)
df_traj = df[['time', 'lat', 'lon']]
df_traj.shape
df_ingest = df_traj.drop_duplicates()
df_ingest.sort_values(['time','lat','lon'],ascending=[True, True,True], inplace=True)
df_ingest.to_excel(raw_folder+'JC156_trajectory.xlsx',index=False)

flist = glob.glob(raw_folder+"*locations.xlsx")
df_xl = pd.read_excel(meta_xls)

for fil in flist:
    cruise = fil.rsplit('/',1)[1].split('station',1)[0]
    if cruise in cruise_list:
        df_raw = pd.read_excel(fil)
        df_raw['time']=df_raw['start_date'].str.replace('Z','')
        df_raw.rename(columns={'latitude':'lat','longitude':'lon'}, inplace=True)
        df_cruise = df_raw[['time','lat','lon']]
        df_meta = df_xl.loc[df_xl['Name']==cruise]
        export_file = f'{vs.combined}{cruise}_cruise_meta_nav_data.xlsx'
        cmn.combine_df_to_excel(export_file, '', df_cruise, df_meta, True)
        print(f'meta for {cruise} saved')




