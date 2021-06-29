import os
import sys

sys.path.append("../../config")
import config_vault as cfgv

cfgv.rep_bats_raw

wget_str_bats_bottle = (
    "wget -O "
    + cfgv.rep_bats_raw
    + "Bottle/bats_bottle.txt "
    + "http://batsftp.bios.edu/BATS/bottle/bats_bottle.txt"
)
wget_str_bats_bottle_validation = (
    "wget -O "
    + cfgv.rep_bats_raw
    + "Bottle/bval_bottle.txt "
    + "http://batsftp.bios.edu/BATS/bottle/bval_bottle.txt"
)

wget_str_bats_pigment = (
    "wget -O "
    + cfgv.rep_bats_raw
    + "Pigments/bats_pigments.txt "
    + "http://batsftp.bios.edu/BATS/bottle/bats_pigments.txt "
)
wget_str_bats_pigment_validation = (
    "wget -O"
    + cfgv.rep_bats_raw
    + "Pigments/bval_pigments.txt "
    + "http://batsftp.bios.edu/BATS/bottle/bval_pigments.txt "
)


def download_BATS_Bottle(wget_str):
    os.system(wget_str)


download_BATS_Bottle(wget_str_bats_bottle)
download_BATS_Bottle(wget_str_bats_bottle_validation)
download_BATS_Bottle(wget_str_bats_pigment)
download_BATS_Bottle(wget_str_bats_pigment_validation)
