import sys
import os
import glob
import pandas as pd

sys.path.append("ingest")
import vault_structure as vs

base_folder = vs.collected_data+'insitu/cruise/BATS/uw'

flist = []
for root, dirs, files in os.walk(str(base_folder)):
    for f in files:
        if f.endswith('.uw'):
            full_path = os.path.join(root, f)
            flist.append(full_path)

len(flist)
fil = flist[0]

# % Underway data for cruise 10235 
# % DecYr       YYYYMMDD HHMMSS Lat     Long    TSG-T1  TSG-T2  TSG-C   TSG-Sal   Sigma-T Fluro  AirTemp %Hum   BaroP   Precip RWindSP RWindDP   RWindSS RWindDS   TWindSP TWindDP   TWindSS TWindDS   SOG   COG    AD100  ADU5   Pitch  Roll 
# 2008.40360460 20080527 171546 32.3700 64.6959 -9.9900 -9.9900 -9.9900 -9.9900   -9.9900 -9.990 21.514  76.784 1019.22  1.30  10.1500  69.0100   9.9200  85.9400  10.1000 175.0000   9.9000 192.0000   0.10 188.50 254.50 255.48   0.11   0.14 

bats_cols = ['DecYr', 'YYYYMMDD', 'HHMMSS', 'Lat', 'Long', 'TSG-T1', 'TSG-T2', 'TSG-C', 'TSG-Sal', 'Sigma-T', 'Fluro', 'AirTemp', '%Hum', 'BaroP', 'Precip', 'RWindSP', 'RWindDP', 'RWindSS', 'RWindDS', 'TWindSP', 'TWindDP', 'TWindSS', 'TWindDS', 'SOG', 'COG', 'AD100', 'ADU5', 'Pitch', 'Roll']
len(bats_cols)
df_header = pd.read_csv(fil, skiprows=1, nrows=1, sep='\s+')
cols = df_header.columns.to_list()
cols.remove('%')
if cols == bats_cols:
    df = pd.read_csv(fil, skiprows=2, sep='\s+',names=bats_cols)

df.columns.to_list()