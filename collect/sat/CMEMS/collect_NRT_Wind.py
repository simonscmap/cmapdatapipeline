from cmapingest import vault_structure as vs
import os

"""

FINISHED --2021-06-16 16:45:22--
Total wall clock time: 5h 33m 30s
Downloaded: 5089 files, 51G in 4h 47m 28s (3.01 MB/s)
"""


output_dir = (vs.collected_data + "sat/CMEMS_NRT_Wind/").replace(" ", "\\ ")

ftp_link = "ftp://nrt.cmems-du.eu/Core/WIND_GLO_WIND_L4_NRT_OBSERVATIONS_012_004/CERSAT-GLO-BLENDED_WIND_L4-V6-OBS_FULL_TIME_SERIE/*"


def wget_file(fpath, output_dir, usr, psw):
    os.system(
        f"""wget --no-parent -nd -r -m --ftp-user={usr} --ftp-password={psw} {fpath}  -P  {output_dir}"""
    )


wget_file(ftp_link, output_dir, {usr}, {psw})
