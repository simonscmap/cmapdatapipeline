import os

import vault_structure as vs
import metadata

tbl = 'tblTara_eukaryote_otu'
vs.leafStruc(vs.cruise+tbl)

base_folder = os.path.join(vs.cruise,tbl,'raw')
output_dir = base_folder.replace(" ", "\\ ")

tara_link = "http://taraoceans.sb-roscoff.fr/EukDiv/data/Database_W1_Sample_parameters.xls"
tara_link = "http://taraoceans.sb-roscoff.fr/EukDiv/data/Database_W8_Cosmopolitan_OTUS_distribution.tsv"
os.system(f"wget {tara_link} -P {output_dir}")

### Behind paywall
# meta_link = "https://www-science-org.offcampus.lib.washington.edu/doi/suppl/10.1126/science.1261359/suppl_file/sunagawa_tables1.xlsx"

meta_link = "http://ocean-microbiome.embl.de/data/OM.CompanionTables.xlsx"
meta_link = "https://store.pangaea.de/Projects/TARA-OCEANS/Samples_Registry/TARA_SAMPLES_CONTEXT_METHODS.zip"
os.system(f"wget {meta_link} -P {output_dir}")

metadata.export_script_to_vault(tbl,'cruise','collect/insitu/cruise/TARA/collect_Tara_eukaryote.py','collect.txt')
