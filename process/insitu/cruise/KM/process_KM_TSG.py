import sys
import os
import pandas as pd
import numpy as np
import glob
from zipfile import ZipFile
import tarfile

from requests import head

import vault_structure as vs

cruise_staging = vs.collected_data + 'insitu/cruise/misc_cruise/'

tar_list = glob.glob(cruise_staging+'*.tar')
gz_list = glob.glob(cruise_staging+'*.gz')

df = pd.read_csv(gz_list[0], sep = ' ', header=None)

for tar in tar_list:
    with tarfile.open(tar) as tf:
        tf.extractall(cruise_staging)
for tar in tar_list:        
    os.remove(tar)

cruise_list = ['KM1923', 'KM2002', 'KM2014', 'KM2009', 'KM2013', 'KM2101', 'KM2010', 'KM2011']

cruise = cruise_list[5]
flist = []
for root, dirs, files in os.walk(cruise_staging+cruise):
    for f in files:
        if '1min.geoCSV' in f:
            flist.append(os.path.join(root, f))

fil = flist[0]
df = pd.read_csv(fil, skiprows=16)
df['time'] = pd.to_datetime(df['iso_time']).dt.tz_localize(None)
df.rename(columns={'ship_longitude':'lon', 'ship_latitude':'lat'},inplace=True)
df = df[['time','lat','lon']]


## Build cruise_metadata sheet
data = dict(Nickname='km1923_751', Name=cruise, Ship_Name='Kilo Moana', Chief_Name='Swalwell, Jarred',Cruise_Series='')
data = dict(Nickname='Abyssal Food Web', Name=cruise, Ship_Name='Kilo Moana', Chief_Name='Drazen, Jeffrey',Cruise_Series='')
data = dict(Nickname='HOT325', Name=cruise, Ship_Name='Kilo Moana', Chief_Name='Rohrer, Tully',Cruise_Series='')
data = dict(Nickname='HOT321', Name=cruise, Ship_Name='Kilo Moana', Chief_Name='White, Angelicque',Cruise_Series='')
data = dict(Nickname='HOT324', Name=cruise, Ship_Name='Kilo Moana', Chief_Name='Santiago-Mandujano, Fernando',Cruise_Series='')
data = dict(Nickname='HOT326', Name=cruise, Ship_Name='Kilo Moana', Chief_Name='Sadler, Daniel',Cruise_Series='')
data = dict(Nickname='HOT323', Name=cruise, Ship_Name='Kilo Moana', Chief_Name='Santiago-Mandujano, Fernando',Cruise_Series='')
data = dict(Nickname='HOT 2018-2023', Name=cruise, Ship_Name='Kilo Moana', Chief_Name='White, Angelicque',Cruise_Series='')


df_cruise_meta = pd.DataFrame(data, index=[0])

df = df[~df['lat'].isnull()]
## Export cruise metadata and trajectory template
with pd.ExcelWriter(f"{vs.combined}{cruise}_cruise_meta_nav_data.xlsx") as writer:  
    df_cruise_meta.to_excel(writer, sheet_name='cruise_metadata', index=False)
    df.to_excel(writer, sheet_name='cruise_trajectory', index=False)

    