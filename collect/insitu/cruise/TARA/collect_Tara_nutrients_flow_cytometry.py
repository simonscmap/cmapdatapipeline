import os

import vault_structure as vs
import metadata

tbl = 'tblTara_nutrients_flow_cytometry'
vs.leafStruc(vs.cruise+tbl)

base_folder = os.path.join(vs.cruise,tbl,'raw')
output_dir = base_folder.replace(" ", "\\ ")


meta_link = "http://ocean-microbiome.embl.de/data/OM.CompanionTables.xlsx"
os.system(f"wget {meta_link} -P {output_dir}")

metadata.export_script_to_vault(tbl,'cruise','collect/insitu/cruise/TARA/collect_Tara_nutrients.py','collect.txt')
