"""script to process tara surface water dataset downloaded from: https://doi.pangaea.de/10.1594/PANGAEA.873592 """

##Header: 2009-2012
"""
Optional event label (Event 2) * PI: Pesant, Stephane (https://orcid.org/0000-0002-4936-5209, spesant@marum.de)
Method/Device of event (Method/Device) * PI: Pesant, Stephane (https://orcid.org/0000-0002-4936-5209, spesant@marum.de)
Comment of event (Comment) * PI: Pesant, Stephane (https://orcid.org/0000-0002-4936-5209, spesant@marum.de)
Basis of event (Basis) * PI: Pesant, Stephane (https://orcid.org/0000-0002-4936-5209, spesant@marum.de)
Campaign of event (Campaign) * PI: Pesant, Stephane (https://orcid.org/0000-0002-4936-5209, spesant@marum.de)
Station label (Station) * PI: Pesant, Stephane (https://orcid.org/0000-0002-4936-5209, spesant@marum.de) * METHOD/DEVICE: registered at PANGAEA, Data Publisher for Earth and Environmental Science (URI: http://www.pangaea.de/) * COMMENT: TARA_station#
DATE/TIME (Date/Time) * GEOCODE * PI: Le Goff, Hervé (Herve.Legoff@locean-ipsl.upmc.fr) * METHOD/DEVICE: registered at the European Nucleotides Archive (ENA) (URI: http://www.ebi.ac.uk/ena)
LATITUDE (Latitude) * GEOCODE * PI: Le Goff, Hervé (Herve.Legoff@locean-ipsl.upmc.fr)
LONGITUDE (Longitude) * GEOCODE * PI: Le Goff, Hervé (Herve.Legoff@locean-ipsl.upmc.fr)
ALTITUDE [m] (Altitude) * GEOCODE * PI: Le Goff, Hervé (Herve.Legoff@locean-ipsl.upmc.fr)
Temperature, water [°C] (Temp) * PI: Le Goff, Hervé (Herve.Legoff@locean-ipsl.upmc.fr) * METHOD/DEVICE: Thermosalinograph (TSG)
Salinity (Sal) * PI: Le Goff, Hervé (Herve.Legoff@locean-ipsl.upmc.fr) * METHOD/DEVICE: Thermosalinograph (TSG)
Chlorophyll a [mg/m**3] (Chl a) * PI: Leeuw, Thomas * METHOD/DEVICE: Spectrophotometer, WET Labs AC-S (URI: http://www.seabird.com/wetlabs)
Particulate organic carbon [µg/kg] (POC) * PI: Leeuw, Thomas * METHOD/DEVICE: Spectrophotometer, WET Labs AC-S (URI: http://www.seabird.com/wetlabs)
Particle size distribution slope (PSD slope) * PI: Leeuw, Thomas * METHOD/DEVICE: Spectrophotometer, WET Labs AC-S (URI: http://www.seabird.com/wetlabs) * COMMENT: [gamma]
Fluorescence, minimum (Fo) * PI: Kolber, Zbigniew S (zkolber@gmail.com) * METHOD/DEVICE: Fast repetition rate fluorometry (FRRF) (Kolber & Falkowski, 1993) (URI: http://aslo.org/lo/toc/vol_38/issue_8/1646.pdf) * COMMENT: measured at 445 nm excitation [Fo_445]
Fluorescence, maximum (Fm) * PI: Kolber, Zbigniew S (zkolber@gmail.com) * METHOD/DEVICE: Fast repetition rate fluorometry (FRRF) (Kolber & Falkowski, 1993) (URI: http://aslo.org/lo/toc/vol_38/issue_8/1646.pdf) * COMMENT: measured at 445 nm excitation [Fm_445]
Maximum photochemical quantum yield of photosystem II (Fv/Fm) * PI: Kolber, Zbigniew S (zkolber@gmail.com) * METHOD/DEVICE: Fast repetition rate fluorometry (FRRF) (Kolber & Falkowski, 1993) (URI: http://aslo.org/lo/toc/vol_38/issue_8/1646.pdf) * COMMENT: ratio of variable fluorescence over maximal fluorescence (Fv/Fm=(Fm-F0)/Fm), measured at 445 nm excitation
Functional absorption cross section (Sig) * PI: Kolber, Zbigniew S (zkolber@gmail.com) * METHOD/DEVICE: Fast repetition rate fluorometry (FRRF) (Kolber & Falkowski, 1993) (URI: http://aslo.org/lo/toc/vol_38/issue_8/1646.pdf) * COMMENT: measured at 445 nm excitation [Sig_445]
Plastoquinone pool size (PQPool) * PI: Kolber, Zbigniew S (zkolber@gmail.com) * METHOD/DEVICE: Fast repetition rate fluorometry (FRRF) (Kolber & Falkowski, 1993) (URI: http://aslo.org/lo/toc/vol_38/issue_8/1646.pdf) * COMMENT: measured at 445 nm excitation [PQPool_445]
Time constant, electron transport from primary electron acceptor to plastoquinone pool [ms] (TauQA) * PI: Kolber, Zbigniew S (zkolber@gmail.com) * METHOD/DEVICE: Fast repetition rate fluorometry (FRRF) (Kolber & Falkowski, 1993) (URI: http://aslo.org/lo/toc/vol_38/issue_8/1646.pdf) * COMMENT: measured at 445 nm excitation [TauQA_445]
Time constant, electron transport from plastoquinone to photosystem I pool [ms] (TauPQ) * PI: Kolber, Zbigniew S (zkolber@gmail.com) * METHOD/DEVICE: Fast repetition rate fluorometry (FRRF) (Kolber & Falkowski, 1993) (URI: http://aslo.org/lo/toc/vol_38/issue_8/1646.pdf) * COMMENT: measured at 445 nm excitation [TauPQ_445]
Initial slope of the photosynthesis vs irradiance relationship [1/µE/s] (Alp) * PI: Kolber, Zbigniew S (zkolber@gmail.com) * METHOD/DEVICE: Fast repetition rate fluorometry (FRRF) (Kolber & Falkowski, 1993) (URI: http://aslo.org/lo/toc/vol_38/issue_8/1646.pdf) * COMMENT: measured at 445 nm excitation [Alp_445] (electron/reaction centre/µE/s)
Irradiance half-saturation level [uE] (Ek) * PI: Kolber, Zbigniew S (zkolber@gmail.com) * METHOD/DEVICE: Fast repetition rate fluorometry (FRRF) (Kolber & Falkowski, 1993) (URI: http://aslo.org/lo/toc/vol_38/issue_8/1646.pdf) * COMMENT: measured at 445 nm excitation [Ek_445]
Maximum level of photosynthetic electron transport [1/s] (Pmax) * PI: Kolber, Zbigniew S (zkolber@gmail.com) * METHOD/DEVICE: Fast repetition rate fluorometry (FRRF) (Kolber & Falkowski, 1993) (URI: http://aslo.org/lo/toc/vol_38/issue_8/1646.pdf) * COMMENT: measured at 445 nm excitation [Pmax_445] (electron/reaction centre/s)

"""
# Header 2013:
"""
Optional event label (Event 2) * PI: Pesant, Stephane (https://orcid.org/0000-0002-4936-5209, spesant@marum.de)
Method/Device of event (Method/Device) * PI: Pesant, Stephane (https://orcid.org/0000-0002-4936-5209, spesant@marum.de)
Comment of event (Comment) * PI: Pesant, Stephane (https://orcid.org/0000-0002-4936-5209, spesant@marum.de)
Basis of event (Basis) * PI: Pesant, Stephane (https://orcid.org/0000-0002-4936-5209, spesant@marum.de)
Campaign of event (Campaign) * PI: Pesant, Stephane (https://orcid.org/0000-0002-4936-5209, spesant@marum.de)
Station label (Station) * PI: Pesant, Stephane (https://orcid.org/0000-0002-4936-5209, spesant@marum.de) * METHOD/DEVICE: registered at PANGAEA, Data Publisher for Earth and Environmental Science (URI: http://www.pangaea.de/) * COMMENT: TARA_station#
DATE/TIME (Date/Time) * GEOCODE * PI: Le Goff, Hervé (Herve.Legoff@locean-ipsl.upmc.fr) * METHOD/DEVICE: registered at the European Nucleotides Archive (ENA) (URI: http://www.ebi.ac.uk/ena)
LATITUDE (Latitude) * GEOCODE * PI: Le Goff, Hervé (Herve.Legoff@locean-ipsl.upmc.fr)
LONGITUDE (Longitude) * GEOCODE * PI: Le Goff, Hervé (Herve.Legoff@locean-ipsl.upmc.fr)
DEPTH, water [m] (Depth water) * GEOCODE * PI: Le Goff, Hervé (Herve.Legoff@locean-ipsl.upmc.fr)
Temperature, water [°C] (Temp) * PI: Le Goff, Hervé (Herve.Legoff@locean-ipsl.upmc.fr) * METHOD/DEVICE: Thermosalinograph (TSG)
Salinity (Sal) * PI: Le Goff, Hervé (Herve.Legoff@locean-ipsl.upmc.fr) * METHOD/DEVICE: Thermosalinograph (TSG)
Chlorophyll a [mg/m**3] (Chl a) * PI: Leeuw, Thomas * METHOD/DEVICE: Spectrophotometer, WET Labs AC-S (URI: http://www.seabird.com/wetlabs)
Particulate organic carbon [µg/kg] (POC) * PI: Leeuw, Thomas * METHOD/DEVICE: Spectrophotometer, WET Labs AC-S (URI: http://www.seabird.com/wetlabs)
Particle size distribution slope (PSD slope) * PI: Leeuw, Thomas * METHOD/DEVICE: Spectrophotometer, WET Labs AC-S (URI: http://www.seabird.com/wetlabs) * COMMENT: [gamma]
Backscattering coefficient of particles, 470 nm [1/m] (bbp470) * PI: Leeuw, Thomas * METHOD/DEVICE: Spectral backscattering sensor (WET Labs, Eco-bb3) (URI: http://www.seabird.com/wetlabs)
Backscattering coefficient of particles, 526 nm [1/m] (bbp526) * PI: Leeuw, Thomas * METHOD/DEVICE: Spectral backscattering sensor (WET Labs, Eco-bb3) (URI: http://www.seabird.com/wetlabs)
Backscattering coefficient of particles, 660 nm [1/m] (bbp660) * PI: Leeuw, Thomas * METHOD/DEVICE: Spectral backscattering sensor (WET Labs, Eco-bb3) (URI: http://www.seabird.com/wetlabs)
Fluorescence, chlorophyll [µg/l] (Fluores) * PI: Hafez, Mark * METHOD/DEVICE: Aquatic Laser Fluorescence Analyzer (ALFA) (URI: https://doi.org/10.1364/OE.21.014181) * COMMENT: measured at 514 nm excitation [CChl514]
Fluorescence, colored dissolved organic matter [ppb (QSE)] (fCDOM) * PI: Hafez, Mark * METHOD/DEVICE: Aquatic Laser Fluorescence Analyzer (ALFA) (URI: https://doi.org/10.1364/OE.21.014181) * COMMENT: normalized to water Raman, 405 nm excitation [FDOM405]
Maximum photochemical quantum yield of photosystem II (Fv/Fm) * PI: Hafez, Mark * METHOD/DEVICE: Aquatic Laser Fluorescence Analyzer (ALFA) (URI: https://doi.org/10.1364/OE.21.014181) * COMMENT: ratio of variable fluorescence over maximal fluorescence (Fv/Fm=(Fm-F0)/Fm), measured at 514 nm excitation
Fugacity of carbon dioxide (water) at equilibrator temperature (wet air) [µatm] (fCO2water_equ_wet) * PI: Pino, Diana Ruiz * METHOD/DEVICE: underway pCO2 measuring instrument (ProOceanus CO2-Pro)
pH (pH) * PI: Gattuso, Jean-Pierre (https://orcid.org/0000-0002-4533-4114, gattuso@obs-vlfr.fr) * METHOD/DEVICE: pH sensor, Satlantic SeaFET

"""

import pandas as pd
from cmapingest import DB
from cmapingest import common as cmn
from cmapingest import data
from cmapingest import vault_structure as vs
import sqlalchemy


df1 = pd.read_csv("TARA_meteorological_2009-2012.tab", skiprows=95, sep="\t")
df2 = pd.read_csv("TARA_meteorological_2013.tab", skiprows=65, sep="\t")
df = pd.concat([df1, df2])
# rename columns : df.columns = ['Event', 'Event 2', 'Method/Device', 'Comment', 'Basis', 'Campaign', 'Station', 'Date/Time', 'Latitude', 'Longitude', 'Altitude [m]', 'Temp [°C]', 'Sal', 'Chl a [mg/m**3]', 'POC [µg/kg]', 'PSD slope', 'Fo', 'Fm', 'Fv/Fm', 'Sig', 'PQPool', 'TauQA [ms]', 'TauPQ [ms]', 'Alp [1/µE/s]', 'Ek [uE]', 'Pmax [1/s]', 'Depth water [m]', 'bbp470 [1/m]', 'bbp526 [1/m]', 'bbp660 [1/m]', 'Fluores [µg/l]', 'fCDOM [ppb (QSE)]', 'fCO2water_equ_wet [µatm]', 'pH']
df.columns = [
    "Event",
    "Event2",
    "Method_Device",
    "Comment",
    "Basis",
    "Campaign",
    "Station",
    "time",
    "lat",
    "long",
    "const_depth",
    "temperature",
    "salinity",
    "chl_a",
    "POC",
    "PSD_slope",
    "Fo",
    "Fm",
    "Fv_Fm",
    "Sig",
    "PQPool",
    "TauQA",
    "TauPQ",
    "Alp",
    "Ek",
    "Pmax",
    "Depth_water",
    "bbp470",
    "bbp526",
    "bbp660",
    "Fluores",
    "fCDOM",
    "fCO2water_equ_wet",
    "pH",
]

# these columns do not varry, so they should be put in metadata
meta_dict = df[["Event2", "Method_Device", "Basis"]].iloc[0].to_dict()
df.drop(
    ["Event2", "Method_Device", "Basis", "const_depth", "Depth_water"],
    inplace=True,
    axis=1,
)
# depth is always 2 meters
df["depth"] = 2
df.rename(columns={"long": "lon"}, inplace=True)
df = df[
    [
        "time",
        "lat",
        "lon",
        "depth",
        "temperature",
        "salinity",
        "chl_a",
        "POC",
        "PSD_slope",
        "Fo",
        "Fm",
        "Fv_Fm",
        "Sig",
        "PQPool",
        "TauQA",
        "TauPQ",
        "Alp",
        "Ek",
        "Pmax",
        "bbp470",
        "bbp526",
        "bbp660",
        "Fluores",
        "fCDOM",
        "fCO2water_equ_wet",
        "pH",
        "Event",
        "Campaign",
        "Station",
    ]
]

df.to_excel(vs.combined + "TARA_surface_water.xlsx", sheet_name="data", index=False)


# create trajectory df
tara_traj_df = df[["time", "lat", "long"]]
Cruise_ID = cmn.get_cruise_IDS(["Tara"])[0]
tara_traj_df.insert(0, "Cruise_ID", Cruise_ID)
tara_traj_df = data.format_time_col(tara_traj_df, "time", format="%Y-%m-%d %H:%M:%S")

# create metadata for tara cruise
meta_df = pd.DataFrame(
    {
        "ID": 6187,
        "Nickname": ["Tara Oceans Expedition"],
        "Name": ["Tara"],
        "Ship_Name": ["Schooner Tara"],
        "Start_Time": [f"{tara_traj_df.time.min()}"],
        "End_Time": [f"{tara_traj_df.time.max()}"],
        "Lat_Min": [f"{tara_traj_df.lat.min()}"],
        "Lat_Max": [f"{tara_traj_df.lat.max()}"],
        "Lon_Min": [f"{tara_traj_df.lon.min()}"],
        "Lon_Max": [f"{tara_traj_df.lon.max()}"],
        "Chief_Name": ["Eric Karsenti"],
    }
)


def insert_metadata_into_tblCruise(meta_df):
    DB.toSQLpandas(meta_df, "tblCruise", "Rainier")
    Cruise_ID = cmn.get_cruise_IDS(["Tara"])[0]
    DB.toSQLpandas(meta_df, "tblCruise", "Mariana")
    DB.toSQLbcp_wrapper(meta_df, "tblCruise", "Mariana")


def insert_metadata_into_tblCruise_Trajectory(meta_df):
    DB.toSQLbcp_wrapper(tara_traj_df, "tblCruise_Trajectory", "Rainier")
    DB.toSQLbcp_wrapper(tara_traj_df, "tblCruise_Trajectory", "Mariana")


# insert_metadata_into_tblCruise(meta_df)
# insert_metadata_into_tblCruise_Trajectory(meta_df)
