import pandas as pd
import numpy as np
from cmapingest import vault_structure as vs
from cmapingest import common as cmn
from cmapingest import data

df = pd.read_parquet(
    vs.collected_data
    + "RR1604_Phytoplankton/merged_ctd_station_ST_phytoplankton.parquet"
)
df = df[
    [
        "time",
        "latitude",
        "longitude",
        "depth",
        "station",
        "urea",
        "DOP",
        "chlorophyll_a",
        "LNA_bacteria",
        "HNA_bacteria",
        "prochlorococcus",
        "synechococcus",
        "picoeukaryotes",
        "nanoeukaryotes",
        "bacteria_biomass",
        "prochlorococcus_biomass",
        "synechococcus_biomass",
        "small_eukaryote_biomass",
        "diatom_biomass",
        "dinoflagellate_biomass",
        "ciliate_biomass",
        "other_large_phyto_biomass",
        "NO3_V",
        "NO3_V_std_dev",
        "NO3_rho",
        "NO3_rho_std_dev",
        "NH4_V",
        "NH4_V_std_dev",
        "NH4_rho",
        "NH4_rho_std_dev",
        "urea_V",
        "urea_V_std_dev",
        "urea_rho",
        "urea_rho_std_dev",
        "C_V",
        "C_V_std_dev",
        "C_rho",
        "C_rho_std_dev",
    ]
]
for col in list(df):
    try:
        df[col] = df[col].replace("BD", "")
    except:
        pass

df.rename(columns={"latitude": "lat", "longitude": "lon"}, inplace=True)
df = data.clean_data_df(df)

df.to_excel(
    vs.collected_data
    + "RR1604_Phytoplankton/no_duplicates_RR1604_phytoplankton_timestamped.xlsx",
    index=False,
)
