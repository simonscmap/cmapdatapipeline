import os
import sys
import glob
import pandas as pd
import numpy as np


sys.path.append("cmapdata/ingest")

from ingest import vault_structure as vs
from ingest import credentials as cr
from ingest import cruise

nav_file = '/data/CMAP/SCOPE-PARAGON_740.sfl'
nav_file = '/data/CMAP Data Submission Dropbox/Simons CMAP/collected_data/insitu/cruise/SCOPE/paragon_seaflow.csv'

df2 = pd.read_csv(nav_file, sep='\t', engine='python')
df2.columns

df_traj = df2[['DATE','LAT','LON']]
df_traj.rename(columns={'DATE':'time','LAT':'lat','LON':'lon'}, inplace=True)

df_traj.to_csv('paragon_traj.csv')

