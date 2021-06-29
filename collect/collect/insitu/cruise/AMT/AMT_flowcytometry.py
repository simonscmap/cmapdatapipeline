### Data Cleaning Notes:
# July 7th 2020
# Flow cytometry data for amt cruises: 06,12,13,14,15,16,17
# Only flag present in any flag column was "N", which specificed in FlagData.html corresponds to "Null Value". Because of this, flag columns were excluded from CMAP to reduce variable clutter.

# Idea: create master data, vars_meta_data, select from each to make specific datasets.. drop empty cols from specific...
# create shortname/longname other var dict?
"""
shortname dict. ex:
"""


import pandas as pd
import numpy as np
import os
import sys

from cmapingest import vault_structure as vs
from cmapingest import common as cmn

data_base_path = vs.collected_data + "AMT_cruise_data/"
""" cols with single value:
ODV_type
Site
Bot_Ref """


def combine_df_to_excel(filename, df, dataset_metadata, vars_metadata):
    writer = pd.ExcelWriter(filename, engine="xlsxwriter")
    df.to_excel(writer, sheet_name="data", index=False)
    dataset_metadata.to_excel(writer, sheet_name="dataset_meta_data", index=False)
    vars_metadata.to_excel(writer, sheet_name="vars_meta_data", index=False)
    writer.save()


def drop_empty_cols(df):
    df.dropna(axis=1, how="all", inplace=True)
    return df


def fix_amt_cruise_names(cruise):
    if cruise.lower().split("amt")[1][0] == "0":  # over ATM10, dont zero pad
        new_cruise = cruise
    else:
        new_cruise = "amt" + "0" + cruise.lower().split("amt")[1]
    return new_cruise


metadata = pd.read_html(
    data_base_path + "AMT_Flow_Cytometry/metadata/BODC_sample_metadata_report.html"
)
flag_data = pd.read_html(data_base_path + "AMT_Flow_Cytometry/metadata/FlagData.html")[
    1
]
flag_data.columns = flag_data.columns.droplevel(0)
df = pd.read_csv(data_base_path + "AMT_Flow_Cytometry/data/botlist.csv", sep=",")
df["yyyy-mm-ddThh24:mi:ss[GMT]"] = pd.to_datetime(
    df["yyyy-mm-ddThh24:mi:ss[GMT]"], format="%d/%m/%Y %H:%M"
)

dataset_meta_data_df = pd.DataFrame(
    columns=[
        "dataset_short_name",
        "dataset_long_name",
        "dataset_version",
        "dataset_release_date",
        "dataset_make",
        "dataset_source",
        "dataset_distributor",
        "dataset_acknowledgement",
        "dataset_history",
        "dataset_description",
        "dataset_references",
        "climatology",
        "cruise_names",
    ]
)
vars_meta_data_df = pd.DataFrame(
    columns=[
        "var_short_name",
        "var_long_name",
        "var_sensor",
        "var_unit",
        "var_spatial_res",
        "var_temporal_res",
        "var_discipline",
        "visualize",
        "var_keywords",
        "var_comment",
    ]
)


def combined_cleaning(df):
    col_rename = [
        "cruise",
        "bodc_station",
        "original_station",
        "ODV_type",
        "gear",
        "lat",
        "lon",
        "w_depth",
        "time",
        "site",
        "depth",
        "bottle_pressure",
        "BODC_bottle_code",
        "bottle_flag",
        "rosette_position",
        "firing_sequence",
        "bottle_reference",
        "synechococcus_carbon_biomass_C700A90Z",  # C700A90Z
        "synechococcus_carbon_biomass_C700A90Z_flag",  # QV:BODC
        "prochlorococcus_carbon_biomass_C701A90Z",  # C701A90Z
        "prochlorococcus_carbon_biomass_C701A90Z_flag",  # QV:BODC.1
        "bacteria_abundance_C804B6A6",  # C804B6A6
        "bacteria_abundance_C804B6A6_flag",  # QV:BODC.2
        "picoeukaryotic_carbon_biomass_CYEUA00A",  # CYEUA00A
        "picoeukaryotic_carbon_biomass_CYEUA00A_flag",  # QV:BODC.3
        "bacteria_abundance_std_dev_H396080A",  # H396080A
        "bacteria_abundance_std_dev_H396080A_flag",  # QV:BODC.4
        "bacteria_carbon_biomass_HBBMAFTX",  # HBBMAFTX
        "bacteria_carbon_biomass_HBBMAFTX_flag",  # QV:BODC.5
        "bacteria_abundance_HBCCAFTX_Tarran",  # HBCCAFTX_Tarran
        "bacteria_abundance_HBCCAFTX_Tarran_flag",  # QV:BODC.6
        "bacteria_abundance_HBCCAFTX_Zubkov",  # HBCCAFTX_Zubkov
        "bacteria_abundance_HBCCAFTX_Zubkov_flag",  # QV:BODC.7
        "cryptophyceae_abundance_J79A0596",  # J79A0596
        "cryptophyceae_abundance_J79A0596_flag",  # QV:BODC.8
        "bacteria_abundance_P18318A9",  # P18318A9
        "bacteria_abundance_P18318A9_flag",  # QV:BODC.9
        "prymnesiophyceae_abundance_P490A00Z",  # P490A00Z
        #     "prymnesiophyrbon_biomass_HBBMAFTX",
        #  'bacteria_abundance_HBCCAFTX_Tarran',
        #  'bacteria_abundance_HBCCAFTX_Zubkov',
        #  'cryptophyceae_abundance_J79A0596',
        #  'bacteria_abundance_P18318A9',
        #  'prymnesiophyceae_abundance_P490A00Z',
        "synechococcusceae_abundance_P490A00Z_flag",  # QV:BODC.10
        "synechococcus_abundance_P700A90Z_Tarran",  # P700A90Z_Tarran
        "synechococcus_abundance_P700A90Z_Tarran_flag",  # QV:BODC.11
        "synechococcus_abundance_P700A90Z_Zubkov",  # P700A90Z_Zubkov
        "synechococcus_abundance_P700A90Z_Zubkov_flag",  # QV:BODC.12
        "prochlorococcus_abundance_P701A90Z_Tarran",  # P701A90Z_Tarran
        "prochlorococcus_abundance_P701A90Z_Tarran_flag",  # QV:BODC.13
        "prochlorococcus_abundance_P701A90Z_Zubkov",  # P701A90Z_Zubkov
        "prochlorococcus_abundance_P701A90Z_Zubkov_flag",  # QV:BODC.14
        "picoeukaryotic_abundance_PYEUA00A_Tarran",  # PYEUA00A_Tarran
        "picoeukaryotic_abundance_PYEUA00A_Tarran_flag",  # QV:BODC.15
        "picoeukaryotic_abundance_PYEUA00A_Zubkov",  # PYEUA00A_Zubkov
        "picoeukaryotic_abundance_PYEUA00A_Zubkov_flag",  # QV:BODC.16
        "bacteria_abundance_TBCCAFTX_Tarran",  # TBCCAFTX_Tarran
        "bacteria_abundance_TBCCAFTX_Tarran_flag",  # QV:BODC.17
        "bacteria_abundance_TBCCAFTX_Zubkov",  # TBCCAFTX_Zubkov
        "bacteria_abundance_TBCCAFTX_Zubkov_flag",  # QV:BODC.18
        "nanoeukaryotic_abundance_X726A86B",  # X726A86B
        "nanoeukaryotic_abundance_X726A86B_flag",  # QV:BODC.19
    ]

    df.columns = col_rename
    # These columns only have a single value, should belong in metadata or are flag columns.
    df.drop(
        columns=[
            "ODV_type",
            "site",
            "bottle_reference",
            "synechococcus_carbon_biomass_C700A90Z_flag",
            "prochlorococcus_carbon_biomass_C701A90Z_flag",
            "bacteria_abundance_C804B6A6_flag",
            "picoeukaryotic_carbon_biomass_CYEUA00A_flag",
            "bacteria_abundance_std_dev_H396080A_flag",
            "bacteria_carbon_biomass_HBBMAFTX_flag",
            "bacteria_abundance_HBCCAFTX_Tarran_flag",
            "bacteria_abundance_HBCCAFTX_Zubkov_flag",
            "bacteria_abundance_P18318A9_flag",
            # "prymnesiophyceae_abundance_P490A00Z_flag",
            "synechococcus_abundance_P700A90Z_Tarran_flag",
            "synechococcus_abundance_P700A90Z_Zubkov_flag",
            "prochlorococcus_abundance_P701A90Z_Tarran_flag",
            "prochlorococcus_abundance_P701A90Z_Zubkov_flag",
            "picoeukaryotic_abundance_PYEUA00A_Tarran_flag",
            "picoeukaryotic_abundance_PYEUA00A_Zubkov_flag",
            "bacteria_abundance_TBCCAFTX_Tarran_flag",
            "bacteria_abundance_TBCCAFTX_Zubkov_flag",
            "nanoeukaryotic_abundance_X726A86B_flag",
        ],
        inplace=True,
    )
    # reorder columns
    df = df[
        [
            "time",
            "lat",
            "lon",
            "depth",
            "synechococcus_carbon_biomass_C700A90Z",
            "prochlorococcus_carbon_biomass_C701A90Z",
            "bacteria_abundance_C804B6A6",
            "picoeukaryotic_carbon_biomass_CYEUA00A",
            "bacteria_abundance_std_dev_H396080A",
            "bacteria_carbon_biomass_HBBMAFTX",
            "bacteria_abundance_HBCCAFTX_Tarran",
            "bacteria_abundance_HBCCAFTX_Zubkov",
            "cryptophyceae_abundance_J79A0596",
            "bacteria_abundance_P18318A9",
            "prymnesiophyceae_abundance_P490A00Z",
            "synechococcus_abundance_P700A90Z_Tarran",
            "synechococcus_abundance_P700A90Z_Zubkov",
            "prochlorococcus_abundance_P701A90Z_Tarran",
            "prochlorococcus_abundance_P701A90Z_Zubkov",
            "picoeukaryotic_abundance_PYEUA00A_Tarran",
            "picoeukaryotic_abundance_PYEUA00A_Zubkov",
            "bacteria_abundance_TBCCAFTX_Tarran",
            "bacteria_abundance_TBCCAFTX_Zubkov",
            "nanoeukaryotic_abundance_X726A86B",
            "bodc_station",
            "original_station",
            "gear",
            "BODC_bottle_code",
            "bottle_flag",
            "rosette_position",
            "firing_sequence",
            "w_depth",
            "bottle_pressure",
            "cruise",
        ]
    ]
    df = df.replace(" ", np.nan, regex=True)
    return df


cleaned_df = combined_cleaning(df)
dataset_meta_data_df = pd.DataFrame(
    columns=[
        "dataset_short_name",
        "dataset_long_name",
        "dataset_version",
        "dataset_release_date",
        "dataset_make",
        "dataset_source",
        "dataset_distributor",
        "dataset_acknowledgement",
        "dataset_history",
        "dataset_description",
        "dataset_references",
        "climatology",
        "cruise_names",
    ]
)
vars_meta_data_df = pd.DataFrame(
    columns=[
        "var_short_name",
        "var_long_name",
        "var_sensor",
        "var_unit",
        "var_spatial_res",
        "var_temporal_res",
        "var_discipline",
        "visualize",
        "var_keywords",
        "var_comment",
    ]
)

# def combined_vars_metadata():

#     vars_meta_data_df["var_short_name"] = ['synechococcus_carbon_biomass_C700A90Z','prochlorococcus_carbon_biomass_C701A90Z','picoeukaryotic_carbon_biomass_CYEUA00A','bacteria_carbon_biomass_HBBMAFTX','heterotrophic_bacteria_abundance_HBCCAFTX_Zubkov'             ,'synechococcus_abundance_P700A90Z_Zubkov','prochlorococcus_abundance_P701A90Z_Zubkov','picoeukaryotic_abundance_PYEUA00A_Zubkov','bodc_station','original_station','gear' ,'BODC_bottle_code' ,'bottle_flag','w_depth','bottle_pressure']
#     vars_meta_data_df["var_long_name"] = ['Carbon Biomass of Synechococcus'       ,'Carbon Biomass of Prochlorococcus'      ,'Carbon Biomass of Picoeukaryotic Cells','Carbon Biomass of Bacteria'      ,'Abundance of Heterotrophic Bacteria','Abundance of Synechococcus'             ,'Abundance of Prochlorococcus'             ,'Abundance of Picoeukaryotic Cells'       ,'BODC Station','Original Station', 'Gear', 'BODC Bottle Code','Bottle Flag', 'W Depth', 'Bottle Pressure']
#     vars_meta_data_df["var_sensor"] = ['Flow Cytometry'] * len(vars_meta_data_df["var_short_name"])
#     vars_meta_data_df["var_unit"] = ['mg/m^3','mg/m^3','mg/m^3','mg/m^3','number/ml','number/ml','number/ml','number/ml','','','','','','','']
#     vars_meta_data_df["var_spatial_res"] = ['irregular'] * len(vars_meta_data_df["var_short_name"])
#     vars_meta_data_df["var_temporal_res"] = ['irregular'] * len(vars_meta_data_df["var_short_name"])
#     vars_meta_data_df["var_discipline"] = ['Biology+BioGeoChemistry+Biogeography'] * 8 + ['Uncategorized'] * 7
#     vars_meta_data_df["visualize"] = [1,1,1,1,1,1,1,1,0,0,0,0,0,0,1]
#     vars_meta_data_df["var_keywords"] = [
#     "CTD,cruise,observation,insitu,in-situ,flow cytometry, flow cytometer, ATM, Atlantic Meridional Transect,biology,biogeochemistry,amt06,JR19980514,cyanobacteria,synechococcus carbon biomass,C700A90Z",
#     "CTD,cruise,observation,insitu,in-situ,flow cytometry, flow cytometer, ATM, Atlantic Meridional Transect,biology,biogeochemistry,amt06,JR19980514,cyanobacteria,prochlorococcus carbon biomass,C701A90Z",
#     "CTD,cruise,observation,insitu,in-situ,flow cytometry, flow cytometer, ATM, Atlantic Meridional Transect,biology,biogeochemistry,amt06,JR19980514,picoeukaryotic carbon biomass,CYEUA00A",
#     "CTD,cruise,observation,insitu,in-situ,flow cytometry, flow cytometer, ATM, Atlantic Meridional Transect,biology,biogeochemistry,amt06,JR19980514,cyanobacteria,bacteria carbon biomass,HBBMAFTX",
#     "CTD,cruise,observation,insitu,in-situ,flow cytometry, flow cytometer, ATM, Atlantic Meridional Transect,biology,biogeochemistry,amt06,JR19980514,cyanobacteria,bacteria abundance,HBCCAFTX,Zubkov",
#     "CTD,cruise,observation,insitu,in-situ,flow cytometry, flow cytometer, ATM, Atlantic Meridional Transect,biology,biogeochemistry,amt06,JR19980514,cyanobacteria,synechococcus abundance,P700A90Z Zubkov",
#     "CTD,cruise,observation,insitu,in-situ,flow cytometry, flow cytometer, ATM, Atlantic Meridional Transect,biology,biogeochemistry,amt06,JR19980514,cyanobacteria,prochlorococcus abundance,P701A90Z Zubkov",
#     "CTD,cruise,observation,insitu,in-situ,flow cytometry, flow cytometer, ATM, Atlantic Meridional Transect,biology,biogeochemistry,amt06,JR19980514,picoeukaryotic abundance,PYEUA00A Zubkov",
#     "CTD,cruise,observation,insitu,in-situ,flow cytometry, flow cytometer, ATM, Atlantic Meridional Transect,biology,biogeochemistry,amt06,JR19980514,bodc_station,bodc station,station",
#     "CTD,cruise,observation,insitu,in-situ,flow cytometry, flow cytometer, ATM, Atlantic Meridional Transect,biology,biogeochemistry,amt06,JR19980514,original_station,original station,",
#     "CTD,cruise,observation,insitu,in-situ,flow cytometry, flow cytometer, ATM, Atlantic Meridional Transect,biology,biogeochemistry,amt06,JR19980514,gear",
#     "CTD,cruise,observation,insitu,in-situ,flow cytometry, flow cytometer, ATM, Atlantic Meridional Transect,biology,biogeochemistry,amt06,JR19980514,BODC_bottle_code,BODC bottle code",
#     "CTD,cruise,observation,insitu,in-situ,flow cytometry, flow cytometer, ATM, Atlantic Meridional Transect,biology,biogeochemistry,amt06,JR19980514,bottle_flag,bottle flag",
#     "CTD,cruise,observation,insitu,in-situ,flow cytometry, flow cytometer, ATM, Atlantic Meridional Transect,biology,biogeochemistry,amt06,JR19980514,w_depth,w depth",
#     "CTD,cruise,observation,insitu,in-situ,flow cytometry, flow cytometer, ATM, Atlantic Meridional Transect,biology,biogeochemistry,amt06,JR19980514,bottle_pressure,bottle pressure"
#     ]
#     vars_meta_data_df["var_comment"] = [
#     'Carbon biomass of Synechococcus (ITIS: 773: WoRMS 160572) per unit volume of the water body by automated flow cytometry and abundance to carbon conversion. Doc Reference number = 209612. Originator Name and Institute: Mikhail V Zubkov, National Oceanography Centre, Southampton',
#     'Carbon biomass of Prochlorococcus (ITIS: 610076: WoRMS 345515) per unit volume of the water body by automated flow cytometry and abundance to carbon conversion. Doc Reference number = 209612. Originator Name and Institute: Mikhail V Zubkov, National Oceanography Centre, Southampton',
#     'Carbon biomass of picoeukaryotic cells per unit volume of the water body by automated flow cytometry and abundance to carbon conversion. Doc Reference number = 209612. Originator Name and Institute: Mikhail V Zubkov, National Oceanography Centre, Southampton',
#     'Carbon biomass of Bacteria (ITIS: 202421: WoRMS 6) [Subgroup: heterotrophic] per unit volume of the water body by automated flow cytometry and abundance to carbon conversion. Doc Reference number = 209612. Originator Name and Institute: Mikhail V Zubkov, National Oceanography Centre, Southampton',
#     'Abundance of bacteria (ITIS: 202421: WoRMS 6) [Subgroup: heterotrophic] per unit volume of the water body by flow cytometry and subtraction of Synechococcus+Prochlorococcus from total bacteria. Doc Reference number = 209612. Originator Name and Institute: Mikhail V Zubkov, National Oceanography Centre, Southampton',
#     'Abundance of Synechococcus (ITIS: 773: WoRMS 160572) per unit volume of the water body by flow cytometry. Doc Reference number = 209612. Originator Name and Institute: Mikhail V Zubkov, National Oceanography Centre, Southampton',
#     'Abundance of Prochlorococcus (ITIS: 610076: WoRMS 345515) per unit volume of the water body by flow cytometry. Doc Reference number = 209612. Originator Name and Institute: Mikhail V Zubkov, National Oceanography Centre, Southampton',
#     'Abundance of picoeukaryotic cells per unit volume of the water body by flow cytometry. Doc Reference number = 209612. Originator Name and Institute: Mikhail V Zubkov, National Oceanography Centre, Southampton',
#     '',
#     '',
#     '',
#     '',
#     '{}'.format(str(flag_data)),
#     '',
#     '']
def compile_AMT_cruise(cleaned_df, dataset_meta_data_df, vars_meta_data_df):
    for cruise in cleaned_df["cruise"].unique():
        cruise_data = drop_empty_cols(
            cleaned_df[cleaned_df["cruise"] == cruise]
        ).replace(np.nan, "", regex=True)
        cruise_data.drop(columns=["cruise"], inplace=True)

        """data metadata"""
        cruise_details = cmn.getCruiseDetails(fix_amt_cruise_names(cruise))
        amt_core_keywords = """cruise,observation,insitu,in-situ,flow cytometry, flow cytometer, ATM, Atlantic Meridional Transect,biology,biogeochemistry,{cruise_name},{cruise_nickname}""".format(
            cruise_name=cruise_details["Name"].iloc[0],
            cruise_nickname=cruise_details["Nickname"].iloc[0],
        )

        vars_dict = {  # ["var_discipline","visualize","var_keywords","var_comment"]
            "synechococcus_carbon_biomass_C700A90Z": [
                "Carbon Biomass of Synechococcus",
                "Flow Cytometry",
                "mg/m^3",
                "irregular",
                "irregular",
                "Biology+BioGeoChemistry+Biogeography",
                "1",
                amt_core_keywords,
                "Carbon biomass of Synechococcus (ITIS: 773: WoRMS 160572) per unit volume of the water body by automated flow cytometry and abundance to carbon conversion. Doc Reference number = 209612. Originator Name and Institute: Mikhail V Zubkov, National Oceanography Centre, Southampton",
            ],
            "prochlorococcus_carbon_biomass_C701A90Z": [
                "Carbon Biomass of Prochlorococcus",
                "Flow Cytometry",
                "mg/m^3",
                "irregular",
                "irregular",
                "Biology+BioGeoChemistry+Biogeography",
                "1",
                amt_core_keywords,
                "Carbon biomass of Prochlorococcus (ITIS: 610076: WoRMS 345515) per unit volume of the water body by automated flow cytometry and abundance to carbon conversion. Doc Reference number = 209612. Originator Name and Institute: Mikhail V Zubkov, National Oceanography Centre, Southampton",
            ],
            "picoeukaryotic_carbon_biomass_CYEUA00A": [
                "Carbon Biomass of Picoeukaryotic Cells",
                "Flow Cytometry",
                "mg/m^3",
                "irregular",
                "irregular",
                "Biology+BioGeoChemistry+Biogeography",
                "1",
                amt_core_keywords,
                "Carbon biomass of picoeukaryotic cells per unit volume of the water body by automated flow cytometry and abundance to carbon conversion. Doc Reference number = 209612. Originator Name and Institute: Mikhail V Zubkov, National Oceanography Centre, Southampton",
            ],
            "bacteria_carbon_biomass_HBBMAFTX": [
                "Carbon Biomass of Bacteria",
                "Flow Cytometry",
                "mg/m^3",
                "irregular",
                "irregular",
                "Biology+BioGeoChemistry+Biogeography",
                "1",
                amt_core_keywords,
                "Carbon biomass of Bacteria (ITIS: 202421: WoRMS 6) [Subgroup: heterotrophic] per unit volume of the water body by automated flow cytometry and abundance to carbon conversion. Doc Reference number = 209612. Originator Name and Institute: Mikhail V Zubkov, National Oceanography Centre, Southampton",
            ],
            "bacteria_abundance_C804B6A6": [
                "Abundance of Heterotrophic Bacteria",
                "Flow Cytometry",
                "number/ml",
                "irregular",
                "irregular",
                "Biology+BioGeoChemistry+Biogeography",
                "1",
                amt_core_keywords,
                "Abundance of bacteria (ITIS: 202421: WoRMS 6) [Subgroup: heterotrophic] per unit volume of the water body by flow cytometry and subtraction of Synechococcus+Prochlorococcus from total bacteria. Doc Reference number = 209612. Originator Name and Institute: Mikhail V Zubkov, National Oceanography Centre, Southampton",
            ],
            "bacteria_abundance_std_dev_H396080A": [
                "Abundance of Heterotrophic Bacteria Standard Deviation",
                "Flow Cytometry",
                "irregular",
                "irregular",
                "Biology+BioGeoChemistry+Biogeography",
                "1",
                amt_core_keywords,
                "Abundance standard deviation of bacteria (ITIS: 202421: WoRMS 6) per unit volume of the water body by flow cytometry. Doc Reference number = 218765. Originator Name and Institute: Mikhail V Zubkov, National Oceanography Centre, Southampton",
            ],
            "bacteria_abundance_HBCCAFTX_Zubkov": [
                "Abundance of Heterotrophic Bacteria",
                "Flow Cytometry",
                "number/ml",
                "irregular",
                "irregular",
                "Biology+BioGeoChemistry+Biogeography",
                "1",
                amt_core_keywords,
                "Abundance of bacteria (ITIS: 202421: WoRMS 6) [Subgroup: heterotrophic] per unit volume of the water body by flow cytometry and subtraction of Synechococcus+Prochlorococcus from total bacteria. Doc Reference number = 207136 . Originator Name and Institute: Mikhail V Zubkov, National Oceanography Centre, Southampton",
            ],
            "cryptophyceae_abundance_J79A0596": [
                "Abundance of Cryptophyceae",
                "Flow Cytometry",
                "number/ml",
                "irregular",
                "irregular",
                "Biology+BioGeoChemistry+Biogeography",
                "1",
                amt_core_keywords,
                "Abundance of Cryptophyceae (ITIS: 10598: WoRMS 17639) per unit volume of the water body by flow cytometry. Doc Reference number = 207153. Originator Name and Institute: Glen A Tarran, Plymouth Marine Laboratory",
            ],
            "bacteria_abundance_P18318A9": [
                "Abundance of Heterotrophic Bacteria",
                "Flow Cytometry",
                "number/ml",
                "irregular",
                "irregular",
                "Biology+BioGeoChemistry+Biogeography",
                "1",
                amt_core_keywords,
                "Abundance of bacteria (ITIS: 202421: WoRMS 6) [Subgroup: heterotrophic; high nucleic acid cell content] per unit volume of the water body by flow cytometry. Doc Reference number = 207153. Originator Name and Institute: Glen A Tarran, Plymouth Marine Laboratory",
            ],
            "prymnesiophyceae_abundance_P490A00Z": [
                "Abundance of Prymnesiophyceae",
                "Flow Cytometry",
                "number/ml",
                "irregular",
                "irregular",
                "Biology+BioGeoChemistry+Biogeography",
                "1",
                amt_core_keywords,
                "Abundance of Prymnesiophyceae (ITIS: 2135: WoRMS 115057) [Subgroup: coccolithophores] per unit volume of the water body by flow cytometry. Doc Reference number = 207153. Originator Name and Institute: Glen A Tarran, Plymouth Marine Laboratory",
            ],
            "synechococcus_abundance_P700A90Z_Tarran": [
                "Abundance of Synechococcus",
                "Flow Cytometry",
                "number/ml",
                "irregular",
                "irregular",
                "Biology+BioGeoChemistry+Biogeography",
                "1",
                amt_core_keywords,
                "Abundance of Synechococcus (ITIS: 773: WoRMS 160572) per unit volume of the water body by flow cytometry. Doc Reference number = 207153. Originator Name and Institute: Glen A Tarran, Plymouth Marine Laboratory",
            ],
            "synechococcus_abundance_P700A90Z_Zubkov": [
                "Abundance of Synechococcus",
                "Flow Cytometry",
                "number/ml",
                "irregular",
                "irregular",
                "Biology+BioGeoChemistry+Biogeography",
                "1",
                amt_core_keywords,
                "Abundance of Synechococcus (ITIS: 773: WoRMS 160572) per unit volume of the water body by flow cytometry. Doc Reference number = . Originator Name and Institute:",
            ],
            "prochlorococcus_abundance_P701A90Z_Tarran": [
                "Abundance of Prochlorococcus",
                "Flow Cytometry",
                "number/ml",
                "irregular",
                "irregular",
                "Biology+BioGeoChemistry+Biogeography",
                "1",
                amt_core_keywords,
                "Abundance of Prochlorococcus (ITIS: 610076: WoRMS 345515) per unit volume of the water body by flow cytometry. Doc Reference number = 207153. Originator Name and Institute:Glen A Tarran, Plymouth Marine Laboratory",
            ],
            "prochlorococcus_abundance_P701A90Z_Zubkov": [
                "Abundance of Prochlorococcus",
                "Flow Cytometry",
                "number/ml",
                "irregular",
                "irregular",
                "Biology+BioGeoChemistry+Biogeography",
                "1",
                amt_core_keywords,
                "Abundance of Prochlorococcus (ITIS: 610076: WoRMS 345515) per unit volume of the water body by flow cytometry. Doc Reference number = 207061. Originator Name and Institute: Mikhail V Zubkov, National Oceanography Centre, Southampton",
            ],
            "picoeukaryotic_abundance_PYEUA00A_Tarran": [
                "Abundance of Picoeukaryotic",
                "Flow Cytometry",
                "number/ml",
                "irregular",
                "irregular",
                "Biology+BioGeoChemistry+Biogeography",
                "1",
                amt_core_keywords,
                "Abundance of picoeukaryotic cells per unit volume of the water body by flow cytometry. Doc Reference number = 207153. Originator Name and Institute:Glen A Tarran, Plymouth Marine Laboratory",
            ],
            "picoeukaryotic_abundance_PYEUA00A_Zubkov": [
                "Abundance of Picoeukaryotic",
                "Flow Cytometry",
                "number/ml",
                "irregular",
                "irregular",
                "Biology+BioGeoChemistry+Biogeography",
                "1",
                amt_core_keywords,
                "Abundance of picoeukaryotic cells per unit volume of the water body by flow cytometry. Doc Reference number = 207061. Originator Name and Institute: Mikhail V Zubkov, National Oceanography Centre, Southampton",
            ],
            "bacteria_abundance_TBCCAFTX_Tarran": [
                "Abundance of Heterotrophic Bacteria",
                "Flow Cytometry",
                "number/ml",
                "irregular",
                "irregular",
                "Biology+BioGeoChemistry+Biogeography",
                "1",
                amt_core_keywords,
                "Abundance of bacteria (ITIS: 202421: WoRMS 6) per unit volume of the water body by flow cytometry. Doc Reference number = 231326. Originator Name and Institute: Glen A Tarran, Plymouth Marine Laboratory",
            ],
            "bacteria_abundance_TBCCAFTX_Zubkov": [
                "Abundance of Heterotrophic Bacteria",
                "Flow Cytometry",
                "number/ml",
                "irregular",
                "irregular",
                "Biology+BioGeoChemistry+Biogeography",
                "1",
                amt_core_keywords,
                "Abundance of bacteria (ITIS: 202421: WoRMS 6) per unit volume of the water body by flow cytometry. Doc Reference number = 218765 . Originator Name and Institute: Mikhail V Zubkov, National Oceanography Centre, Southampton",
            ],
            "nanoeukaryotic_abundance_X726A86B": [
                "Abundance of Nanoeukaryotes",
                "Flow Cytometry",
                "number/ml",
                "irregular",
                "irregular",
                "Biology+BioGeoChemistry+Biogeography",
                "1",
                amt_core_keywords,
                "Abundance of nanoeukaryotic cells [Size: 2-12um] per unit volume of the water body by flow cytometry . Doc Reference number = 231326. Originator Name and Institute: Glen A Tarran, Plymouth Marine Laboratory",
            ],
            "bodc_station": [
                "BODC Station",
                "Uncategorized",
                "",
                "irregular",
                "irregular",
                "Biology+BioGeoChemistry+Biogeography",
                "0",
                amt_core_keywords,
                "",
            ],
            "original_station": [
                "Original Station",
                "Uncategorized",
                "",
                "irregular",
                "irregular",
                "Biology+BioGeoChemistry+Biogeography",
                "0",
                amt_core_keywords,
                "",
            ],
            "gear": [
                "Gear",
                "Uncategorized",
                "irregular",
                "irregular",
                "",
                "Biology+BioGeoChemistry+Biogeography",
                "0",
                amt_core_keywords,
                "",
            ],
            "BODC_bottle_code": [
                "BODC Bottle Code",
                "Uncategorized",
                "",
                "irregular",
                "irregular",
                "Biology+BioGeoChemistry+Biogeography",
                "0",
                amt_core_keywords,
                "",
            ],
            "bottle_flag": [
                "Bottle Flag",
                "Uncategorized",
                "irregular",
                "",
                "irregular",
                "Biology+BioGeoChemistry+Biogeography",
                "0",
                amt_core_keywords,
                """asdf""",
            ],  # "{flag_data}".format(flag_data =(str(flag_data))],
            "rosette_position": [
                "Rosette Position",
                "Uncategorized",
                "",
                "irregular",
                "irregular",
                "Biology+BioGeoChemistry+Biogeography",
                "0",
                amt_core_keywords,
                "",
            ],
            "firing_sequence": [
                "Firing Sequence",
                "Uncategorized",
                "",
                "irregular",
                "irregular",
                "Biology+BioGeoChemistry+Biogeography",
                "0",
                amt_core_keywords,
                "",
            ],
            "w_depth": [
                "Wire Depth",
                "Uncategorized",
                "m",
                "irregular",
                "irregular",
                "Biology+BioGeoChemistry+Biogeography",
                "0",
                amt_core_keywords,
                "",
            ],
            "bottle_pressure": [
                "Bottle Pressure",
                "Uncategorized",
                "",
                "irregular",
                "irregular",
                "Biology+BioGeoChemistry+Biogeography",
                "0",
                amt_core_keywords,
                "",
            ],
        }

        dataset_meta_data_df["dataset_short_name"] = [
            cruise_details["Name"].iloc[0]
            + "_"
            + cruise_details["Nickname"].iloc[0]
            + "_flow_cytometry"
        ]
        dataset_meta_data_df["dataset_long_name"] = [
            cruise_details["Name"].iloc[0]
            + " "
            + cruise_details["Nickname"].iloc[0]
            + " - Atlantic Meridional Transect Flow Cytometry Data"
        ]
        dataset_meta_data_df["dataset_version"] = ["Final"]
        dataset_meta_data_df["dataset_release_date"] = ["2020-07-07"]
        dataset_meta_data_df["dataset_make"] = ["observation"]
        dataset_meta_data_df["dataset_source"] = [
            "British Oceanography Data Centre - National Oceanography Centre"
        ]
        dataset_meta_data_df["dataset_distributor"] = [""]
        dataset_meta_data_df["dataset_acknowledgement"] = [
            "Contains data supplied by Natural Environment Research Council. The programme was funded by the Natural Environment Research Council (NERC) and further support was received from the National Aeronautics and Space Administration (NASA) with equipment and funding from the Sea-viewing Wild Field-of-view Sensor (SeaWiFS) project."
        ]
        dataset_meta_data_df["dataset_history"] = [""]
        dataset_meta_data_df["dataset_description"] = [
            """This dataset contains flow-cytometry data from the {name} {nickname} research cruise. Data was provided by the British Oceanography Data Centre (BODC).""".format(
                name=cruise_details["Name"].iloc[0],
                nickname=cruise_details["Nickname"].iloc[0],
            )
        ]
        dataset_meta_data_df["dataset_references"] = [""]
        dataset_meta_data_df["climatology"] = [""]
        dataset_meta_data_df["cruise_names"] = [cruise_details["Name"].iloc[0]]
        # """variable meta_data"""
        # vars_meta_data_df["var_short_name"] = list(cruise_data)
        # print(vars_meta_data_df)
        # vars_meta_data_df["var_short_name"] = ['synechococcus_carbon_biomass_C700A90Z','prochlorococcus_carbon_biomass_C701A90Z','picoeukaryotic_carbon_biomass_CYEUA00A','bacteria_carbon_biomass_HBBMAFTX','heterotrophic_bacteria_abundance_HBCCAFTX_Zubkov'             ,'synechococcus_abundance_P700A90Z_Zubkov','prochlorococcus_abundance_P701A90Z_Zubkov','picoeukaryotic_abundance_PYEUA00A_Zubkov','bodc_station','original_station','gear' ,'BODC_bottle_code' ,'bottle_flag','w_depth','bottle_pressure']
        # vars_meta_data_df["var_long_name"] = ['Carbon Biomass of Synechococcus'       ,'Carbon Biomass of Prochlorococcus'      ,'Carbon Biomass of Picoeukaryotic Cells','Carbon Biomass of Bacteria'      ,'Abundance of Heterotrophic Bacteria','Abundance of Synechococcus'             ,'Abundance of Prochlorococcus'             ,'Abundance of Picoeukaryotic Cells'       ,'BODC Station','Original Station', 'Gear', 'BODC Bottle Code','Bottle Flag', 'W Depth', 'Bottle Pressure']
        # vars_meta_data_df["var_sensor"] = ['Flow Cytometry'] * len(vars_meta_data_df["var_short_name"])
        # vars_meta_data_df["var_unit"] = ['mg/m^3','mg/m^3','mg/m^3','mg/m^3','number/ml','number/ml','number/ml','number/ml','','','','','','','']
        # vars_meta_data_df["var_spatial_res"] = ['irregular'] * len(vars_meta_data_df["var_short_name"])
        # vars_meta_data_df["var_temporal_res"] = ['irregular'] * len(vars_meta_data_df["var_short_name"])
        # vars_meta_data_df["var_discipline"] = ['Biology+BioGeoChemistry+Biogeography'] * 8 + ['Uncategorized'] * 7
        # vars_meta_data_df["visualize"] = [1,1,1,1,1,1,1,1,0,0,0,0,0,0,1]
        # vars_meta_data_df["var_keywords"] = [
        # "CTD,cruise,observation,insitu,in-situ,flow cytometry, flow cytometer, ATM, Atlantic Meridional Transect,biology,biogeochemistry,amt06,JR19980514,cyanobacteria,synechococcus carbon biomass,C700A90Z",
        # "CTD,cruise,observation,insitu,in-situ,flow cytometry, flow cytometer, ATM, Atlantic Meridional Transect,biology,biogeochemistry,amt06,JR19980514,cyanobacteria,prochlorococcus carbon biomass,C701A90Z",
        # "CTD,cruise,observation,insitu,in-situ,flow cytometry, flow cytometer, ATM, Atlantic Meridional Transect,biology,biogeochemistry,amt06,JR19980514,picoeukaryotic carbon biomass,CYEUA00A",
        # "CTD,cruise,observation,insitu,in-situ,flow cytometry, flow cytometer, ATM, Atlantic Meridional Transect,biology,biogeochemistry,amt06,JR19980514,cyanobacteria,bacteria carbon biomass,HBBMAFTX",
        # "CTD,cruise,observation,insitu,in-situ,flow cytometry, flow cytometer, ATM, Atlantic Meridional Transect,biology,biogeochemistry,amt06,JR19980514,cyanobacteria,bacteria abundance,HBCCAFTX,Zubkov",
        # "CTD,cruise,observation,insitu,in-situ,flow cytometry, flow cytometer, ATM, Atlantic Meridional Transect,biology,biogeochemistry,amt06,JR19980514,cyanobacteria,synechococcus abundance,P700A90Z Zubkov",
        # "CTD,cruise,observation,insitu,in-situ,flow cytometry, flow cytometer, ATM, Atlantic Meridional Transect,biology,biogeochemistry,amt06,JR19980514,cyanobacteria,prochlorococcus abundance,P701A90Z Zubkov",
        # "CTD,cruise,observation,insitu,in-situ,flow cytometry, flow cytometer, ATM, Atlantic Meridional Transect,biology,biogeochemistry,amt06,JR19980514,picoeukaryotic abundance,PYEUA00A Zubkov",
        # "CTD,cruise,observation,insitu,in-situ,flow cytometry, flow cytometer, ATM, Atlantic Meridional Transect,biology,biogeochemistry,amt06,JR19980514,bodc_station,bodc station,station",
        # "CTD,cruise,observation,insitu,in-situ,flow cytometry, flow cytometer, ATM, Atlantic Meridional Transect,biology,biogeochemistry,amt06,JR19980514,original_station,original station,",
        # "CTD,cruise,observation,insitu,in-situ,flow cytometry, flow cytometer, ATM, Atlantic Meridional Transect,biology,biogeochemistry,amt06,JR19980514,gear",
        # "CTD,cruise,observation,insitu,in-situ,flow cytometry, flow cytometer, ATM, Atlantic Meridional Transect,biology,biogeochemistry,amt06,JR19980514,BODC_bottle_code,BODC bottle code",
        # "CTD,cruise,observation,insitu,in-situ,flow cytometry, flow cytometer, ATM, Atlantic Meridional Transect,biology,biogeochemistry,amt06,JR19980514,bottle_flag,bottle flag",
        # "CTD,cruise,observation,insitu,in-situ,flow cytometry, flow cytometer, ATM, Atlantic Meridional Transect,biology,biogeochemistry,amt06,JR19980514,w_depth,w depth",
        # "CTD,cruise,observation,insitu,in-situ,flow cytometry, flow cytometer, ATM, Atlantic Meridional Transect,biology,biogeochemistry,amt06,JR19980514,bottle_pressure,bottle pressure"
        # ]
        # vars_meta_data_df["var_comment"] = [
        "Carbon biomass of Synechococcus (ITIS: 773: WoRMS 160572) per unit volume of the water body by automated flow cytometry and abundance to carbon conversion. Doc Reference number = 209612. Originator Name and Institute: Mikhail V Zubkov, National Oceanography Centre, Southampton",
        "Carbon biomass of Prochlorococcus (ITIS: 610076: WoRMS 345515) per unit volume of the water body by automated flow cytometry and abundance to carbon conversion. Doc Reference number = 209612. Originator Name and Institute: Mikhail V Zubkov, National Oceanography Centre, Southampton",
        "Carbon biomass of picoeukaryotic cells per unit volume of the water body by automated flow cytometry and abundance to carbon conversion. Doc Reference number = 209612. Originator Name and Institute: Mikhail V Zubkov, National Oceanography Centre, Southampton",
        "Carbon biomass of Bacteria (ITIS: 202421: WoRMS 6) [Subgroup: heterotrophic] per unit volume of the water body by automated flow cytometry and abundance to carbon conversion. Doc Reference number = 209612. Originator Name and Institute: Mikhail V Zubkov, National Oceanography Centre, Southampton",
        "Abundance of bacteria (ITIS: 202421: WoRMS 6) [Subgroup: heterotrophic] per unit volume of the water body by flow cytometry and subtraction of Synechococcus+Prochlorococcus from total bacteria. Doc Reference number = 209612. Originator Name and Institute: Mikhail V Zubkov, National Oceanography Centre, Southampton",
        "Abundance of Synechococcus (ITIS: 773: WoRMS 160572) per unit volume of the water body by flow cytometry. Doc Reference number = 209612. Originator Name and Institute: Mikhail V Zubkov, National Oceanography Centre, Southampton",
        "Abundance of Prochlorococcus (ITIS: 610076: WoRMS 345515) per unit volume of the water body by flow cytometry. Doc Reference number = 209612. Originator Name and Institute: Mikhail V Zubkov, National Oceanography Centre, Southampton",
        "Abundance of picoeukaryotic cells per unit volume of the water body by flow cytometry. Doc Reference number = 209612. Originator Name and Institute: Mikhail V Zubkov, National Oceanography Centre, Southampton",
        "",
        # '',
        # '',
        # '',
        # '{}'.format(str(flag_data)),
        # '',
        # '']
        return vars_dict, cruise_data
        break


#     combine_df_to_excel(vs.staging+'combined/AMT06_flow_cytometry.xlsx',amt06,dataset_meta_data_df, vars_meta_data_df)
# clean_amt06(cleaned_df,dataset_meta_data_df, vars_meta_data_df)


vars_dict, cruise_data = compile_AMT_cruise(
    cleaned_df, dataset_meta_data_df, vars_meta_data_df
)

common_vars = list(set(list(vars_dict.keys())).intersection(set(list(cruise_data))))

dict_you_want = {your_key: vars_dict[your_key] for your_key in common_vars}


# """dataset metadata"""
# dataset_meta_data_df["dataset_short_name"] = ""
# dataset_meta_data_df["dataset_long_name"] = ""
# dataset_meta_data_df["dataset_version"] = ""
# dataset_meta_data_df["dataset_release_date"] = ""
# dataset_meta_data_df["dataset_make"] = ""
# dataset_meta_data_df["dataset_source"] = ""
# dataset_meta_data_df["dataset_distributor"] = ""
# dataset_meta_data_df["dataset_acknowledgement"] = ""
# dataset_meta_data_df["dataset_history"] = ""
# dataset_meta_data_df["dataset_description"] = ""
# dataset_meta_data_df["dataset_references"] = ""
# dataset_meta_data_df["climatology"] = ""
# dataset_meta_data_df["cruise_names"] = ""
#
# """variable meta_data"""
# vars_meta_data_df["var_short_name"] = ""
# vars_meta_data_df["var_long_name"] = ""
# vars_meta_data_df["var_sensor"] = ""
# vars_meta_data_df["var_unit"] = ""
# vars_meta_data_df["var_spatial_res"] = ""
# vars_meta_data_df["var_temporal_res"] = ""
# vars_meta_data_df["var_discipline"] = ""
# vars_meta_data_df["visualize"] = ""
# vars_meta_data_df["var_keywords"] = ""
# vars_meta_data_df["var_comment"] = ""
