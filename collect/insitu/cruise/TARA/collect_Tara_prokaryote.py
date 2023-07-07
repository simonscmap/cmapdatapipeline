import os

import vault_structure as vs
import metadata

tbl = 'tblTara_prokaryote_otu'
vs.leafStruc(vs.cruise+tbl)

base_folder = os.path.join(vs.cruise,tbl,'raw')
output_dir = base_folder.replace(" ", "\\ ")

tara_link = "http://ocean-microbiome.embl.de/data/miTAG.taxonomic.profiles.release.tsv.gz"
os.system(f"wget {tara_link} -P {output_dir}")

### Behind paywall
# meta_link = "https://www-science-org.offcampus.lib.washington.edu/doi/suppl/10.1126/science.1261359/suppl_file/sunagawa_tables1.xlsx"

meta_link = "http://ocean-microbiome.embl.de/data/OM.CompanionTables.xlsx"
os.system(f"wget {meta_link} -P {output_dir}")

metadata.export_script_to_vault(tbl,'cruise','collect/insitu/cruise/TARA/collect_Tara_prokaryote.py','collect.txt')
