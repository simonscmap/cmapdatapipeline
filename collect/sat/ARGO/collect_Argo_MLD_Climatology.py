import os
from ingest import vault_structure as vs



file_link = "http://mixedlayer.ucsd.edu/data/Argo_mixedlayers_monthlyclim_03172021.nc"

output_dir = (vs.collected_data + "model/ARGO_MLD_Climatology/").replace(
    " ", "\\ "
)

def wget_file(file_link, output_dir):
    os.system(
        f"""wget --no-check-certificate {file_link} -P {output_dir}"""
    )

wget_file(file_link, output_dir)