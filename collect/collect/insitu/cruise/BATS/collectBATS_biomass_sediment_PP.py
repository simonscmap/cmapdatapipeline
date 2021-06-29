import os
import sys

sys.path.append("../../config")
import config_vault as cfgv

cfgv.rep_bats_raw

wget_str_bats_zooplankton_biomass = (
    "wget -O "
    + cfgv.rep_bats_raw
    + "Zooplankton_Biomass/BATS_zooplankton.xlsx "
    + "http://batsftp.bios.edu/BATS/zooplankton/BATS_zooplankton.xlsx"
)
wget_str_bats_sediment_trap = (
    "wget -O "
    + cfgv.rep_bats_raw
    + "Sediment_Trap/bats_flux.xlsx "
    + "http://batsftp.bios.edu/BATS/flux/bats_flux.xlsx"
)
wget_str_bats_bacteria_production = (
    "wget -O "
    + cfgv.rep_bats_raw
    + "Bacteria_Production/BATS_bacteria_production.txt "
    + "http://batsftp.bios.edu/BATS/production/bats_bacteria_production.txt"
)
wget_str_bats_primary_production = (
    "wget -O "
    + cfgv.rep_bats_raw
    + "Primary_Production/BATS_primary_production.txt "
    + "http://batsftp.bios.edu/BATS/production/bats_primary_production.txt"
)


def download_BATS_Bottle(wget_str):
    os.system(wget_str)


# download_BATS_Bottle(wget_str_bats_zooplankton_biomass)
# download_BATS_Bottle(wget_str_bats_sediment_trap)
# download_BATS_Bottle(wget_str_bats_bacteria_production)
# download_BATS_Bottle(wget_str_bats_primary_production)
