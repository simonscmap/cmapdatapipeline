import sys
import pandas as pd
import glob

sys.path.append("ingest")

import vault_structure as vs
import SQL
import DB


tbl = 'tblGO_SHIP_P16NS_2005_2006_515Y_926R'

# vs.leafStruc(vs.cruise+tbl)

raw_folder = vs.cruise+tbl+'/raw/'

flist = glob.glob(raw_folder+'*')

df = pd.read_csv(flist[0], sep='\t')
df.columns.to_list()
df.rename(columns={'Eukaryotic_Fraction_from_Trimmed_Sequences_18Sdiv16S+18S':'Eukaryotic_Fraction_from_Trimmed_Sequences_18Sdiv16S_18S'}, inplace=True)

df.describe()
df.dtypes

# SQL.full_SQL_suggestion_build(df, tbl, 'cruise', 'Rainier', 'Opedia')
# DB.toSQLbcp_wrapper(df, tbl, 'Rainier')