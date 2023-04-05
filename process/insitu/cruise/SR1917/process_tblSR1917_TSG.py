import os
import sys
import glob
import pandas as pd
import numpy as np


sys.path.append("cmapdata/ingest")

from ingest import vault_structure as vs
from ingest import credentials as cr
from ingest import cruise
from ingest import SQL
from ingest import DB
from ingest import data
from ingest import common as cmn


tbl = 'tblSR1917_TSG'
raw_folder = f'{vs.cruise}{tbl}/raw/'
rep_folder = f'{vs.cruise}{tbl}/rep/'
meta_folder = f'{vs.cruise}{tbl}/metadata/'

tsg_flist = glob.glob(raw_folder+'*.raw')

df_nav = pd.read_csv( glob.glob(raw_folder+'*.geoCSV')[0], skiprows=16)
df_nav['time'] = pd.to_datetime(df_nav['iso_time'])
df_nav.rename(columns={'ship_longitude':'lon','ship_latitude':'lat'}, inplace=True)
df_nav=df_nav[['time','lat','lon']]

server = 'Rossby'
cruise_name = 'SR1917'
cruise_id = cmn.get_cruise_IDS([cruise_name], server)
trajectory_df = cruise.add_ID_trajectory_df(df_nav, cruise_name, server)
trajectory_df = data.mapTo180180(trajectory_df)
trajectory_df = data.removeMissings(trajectory_df, ['lat','lon'])
trajectory_df.drop_duplicates(keep='first', inplace=True)
trajectory_df['time'] = trajectory_df['time'].dt.tz_localize(None)

qry = f"SELECT top(1) * FROM tblSeaflow where cruise = '{cruise_name}'"

df = DB.dbRead(qry,server)

qry = f"DELETE FROM tblCruise_Trajectory where cruise_ID = {cruise_id[0]}"

DB.DB_modify(qry, server)
data.data_df_to_db(
    trajectory_df, "tblCruise_Trajectory", server, clean_data_df_flag=False
)

