import os
import sys

import vault_structure as vs


wget_str_bats_bottle = (
    "wget -O '"
    + vs.cruise
    + "tblBATS_Bottle/raw/bats_bottle.txt' "
    + "https://www.dropbox.com/s/4sdk2tewx46cla8/bats_bottle.txt?dl=1"
)
wget_str_bats_bottle_validation = (
    "wget -O '"
    + vs.cruise
    + "tblBATS_Bottle_Validation/raw/bval_bottle.txt' "
    + "https://www.dropbox.com/s/2vutjrt5ce9t5qx/bval_bottle.txt?dl=1"
)

wget_str_bats_pigment = (
    "wget -O '"
    + vs.cruise
    + "tblBATS_Pigment/raw/bats_pigments.txt' "
    + "https://www.dropbox.com/s/8kk760972lpj5sa/bats_pigments.txt?dl=1"
)
wget_str_bats_pigment_validation = (
    "wget -O '"
    + vs.cruise
    + "tblBATS_Pigment_Validation/raw/bval_pigments.txt' "
    + "https://www.dropbox.com/s/6ajl545hyua8ot8/bval_pigments.txt?dl=1"
)

wget_str_bats_pp = (
    "wget -O '"
    + vs.cruise
    + "tblBATS_Primary_Production/raw/bats_primary_production_v002.txt' "
    + "https://www.dropbox.com/sh/8dbumf8tx3uidjv/AADlDuNqqGVOQ08TLyjQ7nxAa/bats_primary_production_v002.txt?dl=1"
)

wget_str_bats_pp = (
    "wget -O '"
    + vs.cruise
    + "tblBATS_Primary_Production/raw/bats_primary_production_v002.txt' "
    + "https://www.dropbox.com/sh/8dbumf8tx3uidjv/AADlDuNqqGVOQ08TLyjQ7nxAa/bats_primary_production_v002.txt?dl=1"
)

wget_str_bats_sediment = (
    "wget -O '"
    + vs.cruise
    + "tblBATS_Sediment_Trap/raw/bats_flux_v002.txt' "
    + "https://www.dropbox.com/sh/3jl8pq7fvy7iejl/AAC8c91nuNa5aanxLtNxau2aa?dl=1&preview=bats_flux_v002.txt"
)

wget_str_bats_zoo = (
    "wget -O '"
    + vs.cruise
    + "tblBATS_Zooplankton_Biomass/raw/BATS_zooplankton.txt' "
    + "https://www.dropbox.com/sh/xo6c72qaeznyv05/AACCiijqHcd2chjbwrqNcxica?dl=1&preview=BATS_zooplankton.txt"
)




def download_BATS_Bottle(wget_str):
    os.system(wget_str)


download_BATS_Bottle(wget_str_bats_bottle)
download_BATS_Bottle(wget_str_bats_bottle_validation)
download_BATS_Bottle(wget_str_bats_pigment)
download_BATS_Bottle(wget_str_bats_pigment_validation)
download_BATS_Bottle(wget_str_bats_pp)
download_BATS_Bottle(wget_str_bats_sediment)
download_BATS_Bottle(wget_str_bats_zoo)