import os
import pandas as pd


import vault_structure as vs
import common as cmn
import metadata

tbl = 'tblTara_nutrients_flow_cytometry'
base_folder = os.path.join(vs.cruise,tbl,'raw')

## Variable short names in CMAP template
xl = os.path.join(vs.cruise,tbl,'metadata','tblTara_nutrients_flow_cytometry.xlsx')
vars_metadata = pd.read_excel(xl, sheet_name='vars_meta_data')
vars = vars_metadata['var_short_name'].to_list()
dataset_metadata = pd.read_excel(xl, sheet_name='dataset_meta_data')

xl_w8 = os.path.join(vs.cruise,tbl,'raw','OM.CompanionTables.xlsx')
df = pd.read_excel(xl_w8, sheet_name='Table W8')

cols = df.columns.to_list()
df.dtypes

df.rename(columns={i:j for i,j in zip(cols,vars)}, inplace=True)
# *values indicate mean values based on CTD casts that matched closest in location and depth of the actual sampling locations (table S1)
# ** note that some values are equal or below the detection limit of 0,02umol/L and should be interpretated as <=0,02umol/L

## drop notes at bottom of sheed
df = df.loc[~df['time'].isna()]


with pd.ExcelWriter(os.path.join(vs.cruise,tbl,'raw','tblTara_nutrients_flow_cytometry.xlsx')) as writer:
    df.to_excel(writer, sheet_name='data',index=False)
    dataset_metadata.to_excel(writer, sheet_name='dataset_meta_data',index=False)
    vars_metadata.to_excel(writer, sheet_name='vars_meta_data',index=False)


metadata.export_script_to_vault(tbl,'cruise','process/insitu/cruise/TARA/process_Tara_eukaryote.py','process.txt')