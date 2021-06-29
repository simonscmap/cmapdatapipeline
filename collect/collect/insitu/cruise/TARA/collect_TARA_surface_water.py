import os
from cmapingest import vault_structure as vs

tara_surface_link = "https://doi.pangaea.de/10.1594/PANGAEA.873592?format=zip"
output_dir = vs.collected_data + "tara_surface_underway/"
os.system(f"wget {tara_surface_link} -P {output_dir}")
os.system(
    "unzip" + vs.collected_data + "tara_surface_underway/PANGAEA.873592?format=zip"
)
