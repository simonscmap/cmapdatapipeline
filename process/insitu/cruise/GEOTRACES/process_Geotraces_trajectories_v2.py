import sys
import os
import pandas as pd
import numpy as np
import glob

sys.path.append("cmapdata/ingest")
from ingest import vault_structure as vs
from ingest import DB 
from ingest import common as cmn 

tbl = 'tblGeotraces_Seawater_IDP2021v2'
cruise_list =['SAG25', 'AU1602', 'REDMAST']

for cruise in cruise_list:
    raw_folder = f'{vs.r2r_cruise+cruise}/raw/'
    df = pd.read_excel(raw_folder+cruise+'station_locations.xlsx')
    df['time']=df['start_date'].str.replace('Z','')
    df.rename(columns={"latitude":"lat", "longitude":"lon"}, inplace = True)
    df_traj = df[['time', 'lat', 'lon']]
    df_traj.shape
    df_ingest = df_traj.drop_duplicates()
    df_ingest = df_ingest[~df_ingest['lat'].isnull()]
    df_ingest.sort_values(['time','lat','lon'],ascending=[True, True,True], inplace=True)
    if cruise == 'AU1602':
        data = dict(Nickname='GSc03', Name='AU1602', Ship_Name='Aurora Australis', Chief_Name='Taryn, Noble',Cruise_Series=4)
        df_cruise_meta = pd.DataFrame(data, index=[0])
    elif cruise == 'SAG25':
        data = dict(Nickname='GIpr07', Name='SAG25', Ship_Name='S. A. Agulhas II', Chief_Name='Roychoudhury, Alakendra',Cruise_Series=4)
        df_cruise_meta = pd.DataFrame(data, index=[0])    
    elif cruise == 'REDMAST':
        data = dict(Nickname='GIpr09', Name='REDMAST', Ship_Name='Sam Rothberg', Chief_Name='Torfstein, Adi',Cruise_Series=4)
        df_cruise_meta = pd.DataFrame(data, index=[0])  
    with pd.ExcelWriter(f"{raw_folder}{cruise}_cruise_meta_nav_data.xlsx") as writer:  
        df_cruise_meta.to_excel(writer, sheet_name='cruise_metadata', index=False)
        df_ingest.to_excel(writer, sheet_name='cruise_trajectory', index=False)          
    print(f'meta for {cruise} saved')

        




