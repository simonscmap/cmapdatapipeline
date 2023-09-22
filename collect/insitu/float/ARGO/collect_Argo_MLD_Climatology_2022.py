import os
import sys

sys.path.append("ingest")
import vault_structure as vs
import metadata

tbl = 'tblArgo_MLD_2022_Climatology'
vs.leafStruc(vs.float_dir+tbl)
raw_dir = vs.float_dir+tbl+"/raw/"

#http://mixedlayer.ucsd.edu/data/Argo_mixedlayers_monthlyclim_03172021.nc
#http://mixedlayer.ucsd.edu/data/Argo_mixedlayers_monthlyclim_04142022.nc
file_link = "http://mixedlayer.ucsd.edu/data/Argo_mixedlayers_monthlyclim_04142022.nc"

output_dir = raw_dir.replace(" ", "\\ ")

def wget_file(file_link, output_dir):
    os.system(
        f"""wget --no-check-certificate {file_link} -P {output_dir}"""
    )

wget_file(file_link, output_dir)
metadata.export_script_to_vault(tbl,'satellite','collect/sat/MODIS/GetMODIS_CHL_8day_NRT.py','collect.txt')   