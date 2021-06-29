"""This python script is an assorted collection of functions to process data from the AMT (Atlantic Meridional Transect) cruise program."""


"""AMT finishing plan:
write new datafiles with new name
manually merge new data and metadata
manually run MARKDOWN on all metadata...


"""

import pandas as pd
import numpy as np
import os
import sys
import glob

from cmapingest import vault_structure as vs
from cmapingest import common as cmn
from cmapingest import data


data_base_path = vs.collected_data + "AMT_cruise_data/"


""" generalized funcs"""


def timelatlon_reorder(df):
    st_col_list = data.ST_columns(df)
    st_cols = df[st_col_list]
    non_st_cols = df.drop(st_col_list, axis=1)
    reorder_df = pd.concat([st_cols, non_st_cols], axis=1, sort=False)
    return reorder_df


def built_meta_DataFrame():
    dataset_meta = pd.DataFrame(
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
    vars_meta = pd.DataFrame(
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
    return dataset_meta, vars_meta


def combine_df_to_excel(filename, df, dataset_metadata, vars_metadata):
    writer = pd.ExcelWriter(filename, engine="xlsxwriter")
    df.to_excel(writer, sheet_name="data", index=False)
    dataset_metadata.to_excel(writer, sheet_name="dataset_meta_data", index=False)
    vars_metadata.to_excel(writer, sheet_name="vars_meta_data", index=False)
    writer.save()


def drop_empty_cols(df):
    """drops any empty cols"""
    for col in list(df):
        df[col].replace("    ", np.nan, inplace=True)
    df.dropna(inplace=True, axis=1, how="all")
    return df


def fill_depth_if_missing(df, depth_replace_col):
    """This function fills the depth column with depth_below_surface if it is missing"""
    if depth_replace_col in list(df):
        if "depth" not in list(df):
            all_depth_neg = all(
                df[depth_replace_col] < 0
            )  # all values are above sea surface..
            if (
                all_depth_neg == False
            ):  # some vals may be outliers, but will be used for depth
                df = df[df[depth_replace_col] >= 0]
                df["depth"] = df[depth_replace_col]
    return df

    # check if depth col exists
    # if all depth_below_surface are negative don't fill
    # if some depth_below_surface are negative, remove negative rows... then fill
    # if depth_below_surface good, fill


def extract_edmo_cruise_metadata(df):
    # cruise_name = df["cruise"].unique()[0]
    # cruise_type = df["type"].unique()[0]
    EDMO_code = df["EDMO_code"].unique()[0]
    edmo_cruise_metadata_list = [EDMO_code]
    edmo_cruise_metadata_list = [str(i) for i in edmo_cruise_metadata_list]
    return edmo_cruise_metadata_list


def clean_AMT_specific_cols(df, depth_replace_col):
    """A generalized function that:
    - metadata, edmo code, cruise name and cruise type moved to metadata, cols removed from df
    - if depth doesn't exist, use depth_below_surf
    - if col empty, remove
    Args:
        df (Pandas DataFrame): Input Pandas Dataframe

    Returns:
        Pandas DataFrame: Cleaned Pandas DataFrame
    """
    # Drop any empty columns
    df = drop_empty_cols(df)
    df = fill_depth_if_missing(df, depth_replace_col)
    edmo_cruise_metadata_list = extract_edmo_cruise_metadata(df)

    return df, edmo_cruise_metadata_list


def remove_non_var_columns(df):
    name_list = []
    data_list = []
    for col in list(df):
        if int(len(df[col].unique())) == 1:
            print(col, "dropped")
            name_list.append(col)
            data_list.append(df[col].unique()[0])
            df = df.drop(col, 1)
    dum_df = pd.DataFrame(data=[data_list], columns=name_list)
    return df, dum_df


def shared_AMT_data_processing(df, meta_html, depth_replace_col):
    try:
        df["time"] = pd.to_datetime(df["time"], format="%d/%m/%Y %H:%M")
    except:
        df["time"] = pd.to_datetime(df["time"])

    cruise_name = df["cruise"].unique()[0]

    df = data.clean_data_df(df)

    # cleaned_df, dum_df = remove_non_var_columns(df)
    cleaned_df, edmo_cruise_metadata_list = clean_AMT_specific_cols(
        df, depth_replace_col
    )
    cleaned_df = timelatlon_reorder(cleaned_df)

    dataset_meta, vars_meta = built_meta_DataFrame()

    """add to dataset_metadata"""
    dataset_meta["dataset_version"] = "Final"
    dataset_meta["dataset_make"] = "observation"
    dataset_meta["dataset_source"] = "Atlantic Meridional Transect (AMT)"
    dataset_meta["dataset_distributor"] = "British Oceanographic Data Centre (BODC)"

    dataset_meta["cruise_names"] = [cruise_name]
    # dataset_meta["dataset_description"] = ', '.join(edmo_cruise_metadata_list)
    dataset_meta["dataset_description"] = edmo_cruise_metadata_list

    """add to vars_metadata"""
    cleaned_df.drop(["cruise", "type", "EDMO_code"], axis=1, inplace=True)

    vars_meta["var_short_name"] = list(cleaned_df)
    if "Data Flags" in meta_html[0].iloc[0].to_string():
        # vars_meta["var_comment"][vars_meta["var_short_name"].str.contains("flag")] = meta_html[0].to_string(index=False)
        vars_meta["var_comment"][
            vars_meta["var_short_name"].str.contains("flag")
        ] = flow_cyto_dataflag_markdown()

    st_col_list = data.ST_columns(cleaned_df)
    for col in st_col_list:
        vars_meta = vars_meta[~vars_meta["var_short_name"].str.contains(col)]

    return cleaned_df, dataset_meta, vars_meta


def flow_cyto_dataflag_markdown():
    markdown = pd.DataFrame(
        {
            "Flag": [
                "NaN",
                "<",
                ">",
                "A",
                "B",
                "C",
                "D",
                "E",
                "G",
                "H",
                "I",
                "K",
                "L",
                "M",
                "N",
                "O",
                "P",
                "Q",
                "R",
                "S",
                "T",
                "U",
                "W",
                "X",
            ],
            "Description": [
                "Unqualified",
                "Below detection limit",
                "In excess of quoted value",
                "Taxonomic flag for affinis (aff.)",
                "Beginning of CTD Down/Up Cast",
                "Taxonomic flag for confer (cf.)",
                "Thermometric depth",
                "End of CTD Down/Up Cast",
                "Non-taxonomic biological characteristic uncertainty",
                "Extrapolated value",
                "Taxonomic flag for single species (sp.)",
                "Improbable value - unknown quality control source",
                "Improbable value - originators quality control",
                "Improbable value - BODC quality control",
                "Null value",
                "Improbable value - user quality control",
                "Trace/calm",
                "Indeterminate",
                "Replacement value",
                "Estimated value",
                "Interpolated value",
                "Uncalibrated",
                "Control value",
                "Excessive difference",
            ],
        }
    ).to_markdown()
    return markdown


#################
#    AMT 01     #
#################

"""bathemetry"""


def process_amt_01_bathemetry():
    meta_html = pd.read_html(
        data_base_path + "AMT01/Bathymetry_Meteorology_TSG/amt_01_bathemetry_meta.html"
    )
    df = pd.read_csv(
        data_base_path + "AMT01/Bathymetry_Meteorology_TSG/amt_01_bathemetry.txt",
        sep="\t",
        skiprows=15,
        index_col=False,
        names=[
            "cruise",
            "station",
            "type",
            "time",
            "lon",
            "lat",
            "local_cdi_ID",
            "EDMO_code",
            "bottle_depth",
            "depth_below_surf",
            "depth_below_surf_flag",
            "velocity_east",
            "velocity_east_flag",
            "velocity_north",
            "velocity_north_flag",
            "distance_traveled",
            "distance_traveled_flag",
            "heading",
            "heading_flag",
            "bathymetric_depth",
            "bathymetric_depth_flag",
        ],
    )
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(
        df, meta_html, "depth_below_surf"
    )
    combine_df_to_excel(
        vs.staging + "combined/" + "tblJR19950921_AMT01_bathemetry_qa.xlsx",
        cleaned_df,
        dataset_meta,
        vars_meta,
    )


"""meteorology"""


def process_amt_01_meterology():
    meta_html = pd.read_html(
        data_base_path + "AMT01/Bathymetry_Meteorology_TSG/amt_01_meteorology_meta.html"
    )
    df = pd.read_csv(
        data_base_path + "AMT01/Bathymetry_Meteorology_TSG/amt_01_meteorology.txt",
        sep="\t",
        skiprows=17,
        index_col=False,
        names=[
            "cruise",
            "station",
            "type",
            "time",
            "lon",
            "lat",
            "local_cdi_ID",
            "EDMO_code",
            "bottle_depth",
            "depth_below_surf",
            "depth_below_surf_flag",
            "air_pressure",
            "air_pressure_flag",
            "air_temperature",
            "air_temperature_flag",
            "solar_radiation",
            "solar_radiation_flag",
            "SurfVPAR",
            "SurfVPAR_flag",
            "relative_wind_dir",
            "relative_wind_dir_flag",
            "relative_wind_speed",
            "relative_wind_speed_flag",
            "wind_direction_from",
            "wind_direction_from_flag",
            "wind_speed",
            "wind_speed_flag",
        ],
    )
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(
        df, meta_html, "depth_below_surf"
    )
    combine_df_to_excel(
        vs.staging + "combined/" + "tblJR19950921_AMT01_meterology_qa.xlsx",
        cleaned_df,
        dataset_meta,
        vars_meta,
    )


"""TSG"""


def process_amt_01_tsg():
    meta_html = pd.read_html(
        data_base_path + "AMT01/Bathymetry_Meteorology_TSG/amt_01_tsg_meta.html"
    )
    df = pd.read_csv(
        data_base_path + "AMT01/Bathymetry_Meteorology_TSG/amt_01_tsg.txt",
        sep="\t",
        skiprows=13,
        index_col=False,
        names=[
            "cruise",
            "station",
            "type",
            "time",
            "lon",
            "lat",
            "local_cdi_ID",
            "EDMO_code",
            "bottle_depth",
            "depth_below_surf",
            "depth_below_surf_flag",
            "chl_a",
            "chl_a_flag",
            "fluorescence",
            "fluorescence_flag",
            "salinity",
            "salinity_flag",
            "temperature",
            "temperature_flag",
        ],
    )
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(
        df, meta_html, "depth_below_surf"
    )
    combine_df_to_excel(
        vs.staging + "combined/" + "tblJR19950921_AMT01_TSG_qa.xlsx",
        cleaned_df,
        dataset_meta,
        vars_meta,
    )


"""CTD"""


def process_amt_01_CTD():
    meta_html = pd.read_html(data_base_path + "AMT01/CTD/CTD/1056481.html")
    txt_files = glob.glob(data_base_path + "AMT01/CTD/CTD/*.txt")
    new = [
        pd.read_csv(
            f,
            sep="\t",
            skiprows=13,
            index_col=False,
            names=[
                "cruise",
                "station",
                "type",
                "time",
                "lon",
                "lat",
                "local_cdi_ID",
                "EDMO_code",
                "bottle_depth",
                "sensor_pressure",
                "sensor_pressure_flag",
                "potential_temperature",
                "potential_temperature_flag",
                "practical_salinity",
                "practical_salinity_flag",
                "sigma_theta",
                "sigma_theta_flag",
                "uncalibrated_CTD_temperature",
                "uncalibrated_CTD_temperature_flag",
            ],
        )
        for f in txt_files
    ]
    df_concat = pd.concat(new).fillna(method="ffill")
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(
        df_concat, meta_html, "sensor_pressure"
    )
    combine_df_to_excel(
        vs.staging + "combined/" + "tblJR19950921_AMT01_CTD_qa.xlsx",
        cleaned_df,
        dataset_meta,
        vars_meta,
    )


"""Nutrient"""


def process_amt_01_Nutrient():
    meta_html = pd.read_html(data_base_path + "AMT01/Nutrient/Nutrient/1093184.html")
    txt_files = glob.glob(data_base_path + "AMT01/Nutrient/Nutrient/*.txt")
    new = [
        pd.read_csv(
            f,
            sep="\t",
            skiprows=15,
            index_col=False,
            names=[
                "cruise",
                "station",
                "type",
                "time",
                "lon",
                "lat",
                "local_cdi_ID",
                "EDMO_code",
                "bottle_depth",
                "depth_below_surf",
                "depth_below_surf_flag",
                "C22_flag",
                "C22_flag_quality",
                "nitrate",
                "nitrate_flag",
                "nitrate_nitrite",
                "nitrate_nitrite_flag",
                "phosphate",
                "phosphate_flag",
                "sample_reference",
                "sample_reference_flag",
                "silicate",
                "silicate_flag",
            ],
        )
        for f in txt_files
    ]
    df_concat = pd.concat(new).fillna(method="ffill")
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(
        df_concat, meta_html, "depth_below_surf"
    )
    combine_df_to_excel(
        vs.staging + "combined/" + "tblJR19950921_AMT01_Nutrient_qa.xlsx",
        cleaned_df,
        dataset_meta,
        vars_meta,
    )


#################
#    AMT 02     #
#################


def process_amt_02_bathemetry():
    meta_html = pd.read_html(
        data_base_path + "AMT02/Bathymetry_Meteorology_TSG/amt_02_bathemetry_meta.html"
    )
    df = pd.read_csv(
        data_base_path + "AMT02/Bathymetry_Meteorology_TSG/amt_02_bathemetry.txt",
        sep="\t",
        skiprows=15,
        index_col=False,
        names=[
            "cruise",
            "station",
            "type",
            "time",
            "lon",
            "lat",
            "local_cdi_ID",
            "EDMO_code",
            "bottle_depth",
            "depth_below_surf",
            "depth_below_surf_flag",
            "velocity_east",
            "velocity_east_flag",
            "velocity_north",
            "velocity_north_flag",
            "distance_traveled",
            "distance_traveled_flag",
            "heading",
            "heading_flag",
            "bathymetric_depth",
            "bathymetric_depth_flag",
        ],
    )
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(
        df, meta_html, "depth_below_surf"
    )
    combine_df_to_excel(
        vs.staging + "combined/" + "tblJR19960422_AMT02_bathemetry_qa.xlsx",
        cleaned_df,
        dataset_meta,
        vars_meta,
    )


def process_amt_02_meterology():
    meta_html = pd.read_html(
        data_base_path + "AMT02/Bathymetry_Meteorology_TSG/amt_02_meteorology_meta.html"
    )
    df = pd.read_csv(
        data_base_path + "AMT02/Bathymetry_Meteorology_TSG/amt_02_meteorology.txt",
        sep="\t",
        skiprows=17,
        index_col=False,
        names=[
            "cruise",
            "station",
            "type",
            "time",
            "lon",
            "lat",
            "local_cdi_ID",
            "EDMO_code",
            "bottle_depth",
            "depth_below_surf",
            "depth_below_surf_flag",
            "air_pressure",
            "air_pressure_flag",
            "air_temperature",
            "air_temperature_flag",
            "solar_radiation",
            "solar_radiation_flag",
            "SurfVPAR",
            "SurfVPAR_flag",
            "relative_wind_dir",
            "relative_wind_dir_flag",
            "relative_wind_speed",
            "relative_wind_speed_flag",
            "wind_direction_from",
            "wind_direction_from_flag",
            "wind_speed",
            "wind_speed_flag",
        ],
    )
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(
        df, meta_html, "depth_below_surf"
    )
    combine_df_to_excel(
        vs.staging + "combined/" + "tblJR19960422_AMT02_meterology_qa.xlsx",
        cleaned_df,
        dataset_meta,
        vars_meta,
    )


def process_amt_02_tsg():
    meta_html = pd.read_html(
        data_base_path + "AMT02/Bathymetry_Meteorology_TSG/amt_02_tsg_meta.html"
    )
    df = pd.read_csv(
        data_base_path + "AMT02/Bathymetry_Meteorology_TSG/amt_02_tsg.txt",
        sep="\t",
        skiprows=13,
        index_col=False,
        names=[
            "cruise",
            "station",
            "type",
            "time",
            "lon",
            "lat",
            "local_cdi_ID",
            "EDMO_code",
            "bottle_depth",
            "depth_below_surf",
            "depth_below_surf_flag",
            "chl_a",
            "chl_a_flag",
            "fluorescence",
            "fluorescence_flag",
            "salinity",
            "salinity_flag",
            "temperature",
            "temperature_flag",
        ],
    )
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(
        df, meta_html, "depth_below_surf"
    )
    cleaned_df = cleaned_df[
        [
            "time",
            "lat",
            "lon",
            "depth",
            "station",
            "chl_a",
            "chl_a_flag",
            "fluorescence",
            "fluorescence_flag",
            "salinity",
            "salinity_flag",
            "temperature",
            "temperature_flag",
        ]
    ]
    combine_df_to_excel(
        vs.staging + "combined/" + "tblJR19960422_AMT02_TSG_qa.xlsx",
        cleaned_df,
        dataset_meta,
        vars_meta,
    )


def process_amt_02_CTD():
    meta_html = pd.read_html(data_base_path + "AMT02/CTD/CTD/1073921.html")
    txt_files = glob.glob(data_base_path + "AMT02/CTD/CTD/*.txt")
    new = [
        pd.read_csv(
            f,
            sep="\t",
            skiprows=13,
            index_col=False,
            names=[
                "cruise",
                "station",
                "type",
                "time",
                "lon",
                "lat",
                "local_cdi_ID",
                "EDMO_code",
                "bottle_depth",
                "sensor_pressure",
                "sensor_pressure_flag",
                "potential_temperature",
                "potential_temperature_flag",
                "practical_salinity",
                "practical_salinity_flag",
                "sigma_theta",
                "sigma_theta_flag",
                "uncalibrated_CTD_temperature",
                "uncalibrated_CTD_temperature_flag",
            ],
        )
        for f in txt_files
    ]
    df_concat = pd.concat(new).fillna(method="ffill")
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(
        df_concat, meta_html, "sensor_pressure"
    )
    combine_df_to_excel(
        vs.staging + "combined/" + "tblJR19960422_AMT02_CTD_qa.xlsx",
        cleaned_df,
        dataset_meta,
        vars_meta,
    )


def process_amt_02_extracted_pigments():
    meta_html = pd.read_html(
        data_base_path + "AMT02/Extracted_Pigments/Extracted_Pigments.html"
    )
    df = pd.read_csv(
        data_base_path + "AMT02/Extracted_Pigments/Extracted_Pigments.txt",
        sep="\t",
        skiprows=60,
        index_col=False,
        names=[
            "cruise",
            "station",
            "type",
            "time",
            "lon",
            "lat",
            "local_cdi_ID",
            "EDMO_code",
            "bottle_depth",
            "depth_below_surface",
            "depth_below_surface_flag",
            "alloxanthin",
            "alloxanthin_flag",
            "alloxanthin_SD",
            "alloxanthin_SD_flag",
            "beta_beta_carotine",
            "beta_beta_carotine_flag",
            "beta_beta_carotine_SD",
            "beta_beta_carotine_SD_flag",
            "19_butanoyloxyfucoxanthin",
            "19_butanoyloxyfucoxanthin_flag",
            "19_butanoyloxyfucoxanthin_SD",
            "19_butanoyloxyfucoxanthin_SD_flag",
            "chlorophyll_b",
            "chlorophyll_b_flag",
            "chlorophyll_b_SD",
            "chlorophyll_b_SD_flag",
            "chlorophyll_c",
            "chlorophyll_c_flag",
            "chlorophyll_c_SD",
            "chlorophyll_c_SD_flag",
            "chlorophyllide_a",
            "chlorophyllide_a_flag",
            "chlorophyllide_a_SD",
            "chlorophyllide_a_SD_flag",
            "chlorophyllide_b",
            "chlorophyllide_b_flag",
            "chlorophyllide_b_SD",
            "chlorophyllide_b_SD_flag",
            "chlorophyll_a_allomer",
            "chlorophyll_a_allomer_flag",
            "chlorophyll_a_allomer_SD",
            "chlorophyll_a_allomer_SD_flag",
            "chlorophyll_a_epimer",
            "chlorophyll_a_epimer_flag",
            "chlorophyll_a_epimer_SD",
            "chlorophyll_a_epimer_SD_flag",
            "chlorophyll_a_SD_fluorometer",
            "chlorophyll_a_SD_fluorometer_flag",
            "chlorophyll_a_fluorometer",
            "chlorophyll_a_fluorometer_flag",
            "chlorophyll_a",
            "chlorophyll_a_flag",
            "diadinoxanthin",
            "diadinoxanthin_flag",
            "diadinoxanthin_SD",
            "diadinoxanthin_SD_flag",
            "diatoxanthin",
            "diatoxanthin_flag",
            "diatoxanthin_SD",
            "diatoxanthin_SD_flag",
            "dinoxanthin",
            "dinoxanthin_flag",
            "dinoxanthin_SD",
            "dinoxanthin_SD_flag",
            "divinyl_chlorophyll_a",
            "divinyl_chlorophyll_a_flag",
            "divinyl_chlorophyll_a_SD",
            "divinyl_chlorophyll_a_SD_flag",
            "fucoxanthin",
            "fucoxanthin_flag",
            "fucoxanthin_SD",
            "fucoxanthin_SD_flag",
            "19_hexanoyloxyfucoxanthin",
            "19_hexanoyloxyfucoxanthin_flag",
            "19_hexanoyloxyfucoxanthin_SD",
            "19_hexanoyloxyfucoxanthin_SD_flag",
            "monovinyl_chlorophyll_a",
            "monovinyl_chlorophyll_a_flag",
            "monovinyl_chlorophyll_a_SD",
            "monovinyl_chlorophyll_a_SD_flag",
            "phaeophorbide_a",
            "phaeophorbide_a_flag",
            "phaeophorbide_a_SD",
            "phaeophorbide_a_SD_flag",
            "peridinin",
            "peridinin_flag",
            "peridinin_SD",
            "peridinin_SD_flag",
            "phaeopigments_fluorometer",
            "phaeopigments_fluorometer_flag",
            "phaeopigments_SD_fluorometer",
            "phaeopigments_SD_fluorometer_flag",
            "prasinoxanthin",
            "prasinoxanthin_flag",
            "prasinoxanthin_SD",
            "prasinoxanthin_SD_flag",
            "phaeophytin_a",
            "phaeophytin_a_flag",
            "phaeophytin_a_SD",
            "phaeophytin_a_SD_flag",
            "phaeophytin_b",
            "phaeophytin_b_flag",
            "phaeophytin_b_SD",
            "phaeophytin_b_SD_flag",
            "chlorophyll_a_SD",
            "chlorophyll_a_SD_flag",
            "zeaxanthin_lutein",
            "zeaxanthin_lutein_flag",
            "zeaxanthin_lutein_SD",
            "zeaxanthin_lutein_SD_flag",
        ],
        skipinitialspace=True,
    )
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(
        df, meta_html, "depth_below_surface"
    )
    combine_df_to_excel(
        vs.staging + "combined/" + "tblJR19960422_AMT02_Extracted_Pigments_qa.xlsx",
        cleaned_df,
        dataset_meta,
        vars_meta,
    )


def process_amt_02_nutrient_underway():
    meta_html = pd.read_html(
        data_base_path
        + "AMT02/Nutrient/surface_underway/surface_underway_nutrient.html"
    )
    df = pd.read_csv(
        data_base_path
        + "AMT02/Nutrient/surface_underway/surface_underway_nutrient.txt",
        sep="\t",
        skiprows=15,
        index_col=False,
        names=[
            "cruise",
            "station",
            "type",
            "time",
            "lon",
            "lat",
            "local_cdi_ID",
            "EDMO_code",
            "bottle_depth",
            "depth_below_surf",
            "depth_below_surf_flag",
            "C22_flag",
            "C22_flag_quality",
            "nitrite",
            "nitrite_flag",
            "nitrate_nitrite",
            "nitrate_nitrite_flag",
            "phosphate",
            "phosphate_flag",
            "sample_reference",
            "sample_reference_flag",
            "silicate",
            "silicate_flag",
        ],
        skipinitialspace=True,
    )
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(
        df, meta_html, "depth_below_surf"
    )
    combine_df_to_excel(
        vs.staging + "combined/" + "tblJR19960422_AMT02_Nutrient_qa.xlsx",
        cleaned_df,
        dataset_meta,
        vars_meta,
    )


def process_amt_02_nutrient_depth_profiles():
    meta_html = pd.read_html(
        data_base_path + "AMT02/Nutrient/depth_profiles/1093485.html"
    )
    txt_files = glob.glob(data_base_path + "AMT02/Nutrient/depth_profiles/*.txt")
    new = [
        pd.read_csv(
            f,
            sep="\t",
            skiprows=15,
            index_col=False,
            names=[
                "cruise",
                "station",
                "type",
                "time",
                "lon",
                "lat",
                "local_cdi_ID",
                "EDMO_code",
                "bottle_depth",
                "depth_below_surf",
                "depth_below_surf_flag",
                "C22_flag",
                "C22_flag_quality",
                "nitrite",
                "nitrite_flag",
                "nitrate_nitrite",
                "nitrate_nitrite_flag",
                "phosphate",
                "phosphate_flag",
                "sample_reference",
                "sample_reference_flag",
                "silicate",
                "silicate_flag",
            ],
        )
        for f in txt_files
    ]
    df_concat = pd.concat(new).fillna(method="ffill")
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(
        df_concat, meta_html, "depth_below_surf"
    )
    combine_df_to_excel(
        vs.staging + "combined/" + "tblJR19960422_AMT02_Nutrient_depth_profile_qa.xlsx",
        cleaned_df,
        dataset_meta,
        vars_meta,
    )


#################
#    AMT 03     #
#################


def process_amt_03_bathemetry():
    meta_html = pd.read_html(
        data_base_path + "AMT03/Bathymetry_Meteorology_TSG/amt03_bathymetry_meta.html"
    )
    df = pd.read_csv(
        data_base_path + "AMT03/Bathymetry_Meteorology_TSG/amt03_bathymetry.txt",
        sep="\t",
        skiprows=15,
        index_col=False,
        names=[
            "cruise",
            "station",
            "type",
            "time",
            "lon",
            "lat",
            "local_cdi_ID",
            "EDMO_code",
            "bottle_depth",
            "depth_below_surface",
            "depth_below_surface_flag",
            "eastward_velocity",
            "eastward_velocity_flag",
            "northward_velocity",
            "northward_velocity_flag",
            "distance_traveled",
            "distance_traveled_flag",
            "heading",
            "heading_flag",
            "bathymetric_depth",
            "bathymetric_depth_flag",
        ],
    )
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(
        df, meta_html, "depth_below_surface"
    )
    combine_df_to_excel(
        vs.staging + "combined/" + "tblJR19960916_AMT03_bathemetry_qa.xlsx",
        cleaned_df,
        dataset_meta,
        vars_meta,
    )


def process_amt_03_meterology():
    meta_html = pd.read_html(
        data_base_path + "AMT03/Bathymetry_Meteorology_TSG/amt03_meteorology_meta.html"
    )
    df = pd.read_csv(
        data_base_path + "AMT03/Bathymetry_Meteorology_TSG/amt03_meteorology.txt",
        sep="\t",
        skiprows=17,
        index_col=False,
        names=[
            "cruise",
            "station",
            "type",
            "time",
            "lon",
            "lat",
            "local_cdi_ID",
            "EDMO_code",
            "bottle_depth",
            "depth_below_surface",
            "depth_below_surface_flag",
            "air_pressure",
            "air_pressure_flag",
            "air_temperature",
            "air_temperature_flag",
            "solar_radiation",
            "solar_radiation_flag",
            "surface_PAR",
            "surface_PAR_flag",
            "relative_wind_direction",
            "relative_wind_direction_flag",
            "relative_wind_speed",
            "relative_wind_speed_flag",
            "wind_direction_north",
            "wind_direction_north_flag",
            "wind_speed",
            "wind_speed_flag",
        ],
    )
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(
        df, meta_html, "depth_below_surface"
    )
    combine_df_to_excel(
        vs.staging + "combined/" + "tblJR19960916_AMT03_meterology_qa.xlsx",
        cleaned_df,
        dataset_meta,
        vars_meta,
    )


def process_amt_03_tsg():
    meta_html = pd.read_html(
        data_base_path + "AMT03/Bathymetry_Meteorology_TSG/amt03_TSG_meta.html"
    )
    df = pd.read_csv(
        data_base_path + "AMT03/Bathymetry_Meteorology_TSG/amt03_TSG.txt",
        sep="\t",
        skiprows=13,
        index_col=False,
        names=[
            "cruise",
            "station",
            "type",
            "time",
            "lon",
            "lat",
            "local_cdi_ID",
            "EDMO_code",
            "bottle_depth",
            "depth_below_surface",
            "depth_below_surface_flag",
            "chl_a",
            "chl_a_flag",
            "fluorescence",
            "fluorescence_flag",
            "salinity",
            "salinity_flag",
            "temperature",
            "temperature_flag",
        ],
    )
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(
        df, meta_html, "depth_below_surface"
    )
    cleaned_df = cleaned_df[
        [
            "time",
            "lat",
            "lon",
            "depth",
            "station",
            "chl_a",
            "chl_a_flag",
            "fluorescence",
            "fluorescence_flag",
            "salinity",
            "salinity_flag",
            "temperature",
            "temperature_flag",
        ]
    ]
    combine_df_to_excel(
        vs.staging + "combined/" + "tblJR19960916_AMT03_TSG_qa.xlsx",
        cleaned_df,
        dataset_meta,
        vars_meta,
    )


def process_amt_03_CTD():
    meta_html = pd.read_html(data_base_path + "AMT03/CTD/1071951.html")
    txt_files = glob.glob(data_base_path + "AMT03/CTD/*.txt")
    new = [
        pd.read_csv(
            f,
            sep="\t",
            skiprows=15,
            index_col=False,
            names=[
                "cruise",
                "station",
                "type",
                "time",
                "lon",
                "lat",
                "local_cdi_ID",
                "EDMO_code",
                "bottle_depth",
                "sensor_pressure",
                "sensor_pressure_flag",
                "chl_a",
                "chl_a_flag",
                "fluorometer_voltage",
                "fluorometer_voltage_flag",
                "potential_temperature",
                "potential_temperature_flag",
                "salinity",
                "salinity_flag",
                "sigma_theta",
                "sigma_theta_flag",
                "temperature",
                "temperature_flag",
            ],
        )
        for f in txt_files
    ]
    df_concat = pd.concat(new).fillna(method="ffill")
    df_concat = df_concat[~df_concat["cruise"].isnull()]
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(
        df_concat, meta_html, "sensor_pressure"
    )
    combine_df_to_excel(
        vs.staging + "combined/" + "tblJR19960916_AMT03_CTD_qa.xlsx",
        cleaned_df,
        dataset_meta,
        vars_meta,
    )


def process_amt_03_extracted_chlorophyll():
    meta_html = pd.read_html(
        data_base_path + "AMT03/Extracted_Chlorophyll/Extracted_Chlorophyll.html"
    )
    df = pd.read_csv(
        data_base_path + "AMT03/Extracted_Chlorophyll/Extracted_Chlorophyll.txt",
        sep="\t",
        skiprows=10,
        index_col=False,
        names=[
            "cruise",
            "station",
            "type",
            "time",
            "lon",
            "lat",
            "local_cdi_ID",
            "EDMO_code",
            "bottle_depth",
            "depth_below_surface",
            "depth_below_surface_flag",
            "chl_a",
            "chl_a_flag",
        ],
        skipinitialspace=True,
    )
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(
        df, meta_html, "depth_below_surface"
    )
    combine_df_to_excel(
        vs.staging + "combined/" + "tblJR19960916_AMT03_Extracted_Chlorophyll_qa.xlsx",
        cleaned_df,
        dataset_meta,
        vars_meta,
    )


def process_amt_03_nutrient():
    meta_html = pd.read_html(data_base_path + "AMT03/Nutrient/1093750.html")
    txt_files = glob.glob(data_base_path + "AMT03/Nutrient/*.txt")
    new = [
        pd.read_csv(
            f,
            sep="\t",
            skiprows=15,
            index_col=False,
            names=[
                "cruise",
                "station",
                "type",
                "time",
                "lon",
                "lat",
                "local_cdi_ID",
                "EDMO_code",
                "bottle_depth",
                "depth_below_surf",
                "depth_below_surf_flag",
                "C22_flag",
                "C22_flag_quality",
                "nitrite",
                "nitrite_flag",
                "nitrate_nitrite",
                "nitrate_nitrite_flag",
                "phosphate",
                "phosphate_flag",
                "sample_reference",
                "sample_reference_flag",
                "silicate",
                "silicate_flag",
            ],
        )
        for f in txt_files
    ]
    df_concat = pd.concat(new).fillna(method="ffill")
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(
        df_concat, meta_html, "depth_below_surf"
    )
    combine_df_to_excel(
        vs.staging + "combined/" + "tblJR19960916_AMT03_Nutrient_qa.xlsx",
        cleaned_df,
        dataset_meta,
        vars_meta,
    )


#################
#    AMT 04     #
#################


def process_amt_04_bathemetry():
    meta_html = pd.read_html(
        data_base_path
        + "AMT04/Bathymetry_Meteorology_TSG/amt04_bathymetry_metadata.html"
    )
    df = pd.read_csv(
        data_base_path + "AMT04/Bathymetry_Meteorology_TSG/amt04_bathymetry.txt",
        sep="\t",
        skiprows=15,
        index_col=False,
        names=[
            "cruise",
            "station",
            "type",
            "time",
            "lon",
            "lat",
            "local_cdi_ID",
            "EDMO_code",
            "bottle_depth",
            "depth_below_surface",
            "depth_below_surface_flag",
            "eastward_velocity",
            "eastward_velocity_flag",
            "northward_velocity",
            "northward_velocity_flag",
            "distance_traveled",
            "distance_traveled_flag",
            "heading",
            "heading_flag",
            "bathymetric_depth",
            "bathymetric_depth_flag",
        ],
    )
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(
        df, meta_html, "depth_below_surface"
    )
    combine_df_to_excel(
        vs.staging + "combined/" + "tblJR19970421_AMT04_bathemetry_qa.xlsx",
        cleaned_df,
        dataset_meta,
        vars_meta,
    )


def process_amt_04_meterology():
    meta_html = pd.read_html(
        data_base_path
        + "AMT04/Bathymetry_Meteorology_TSG/amt04_meteorology_metadata.html"
    )
    df = pd.read_csv(
        data_base_path + "AMT04/Bathymetry_Meteorology_TSG/amt04_meteorology.txt",
        sep="\t",
        skiprows=17,
        index_col=False,
        names=[
            "cruise",
            "station",
            "type",
            "time",
            "lon",
            "lat",
            "local_cdi_ID",
            "EDMO_code",
            "bottle_depth",
            "depth_below_surface",
            "depth_below_surface_flag",
            "air_pressure",
            "air_pressure_flag",
            "air_temperature",
            "air_temperature_flag",
            "solar_radiation",
            "solar_radiation_flag",
            "surface_PAR",
            "surface_PAR_flag",
            "relative_wind_direction",
            "relative_wind_direction_flag",
            "relative_wind_speed",
            "relative_wind_speed_flag",
            "wind_direction_north",
            "wind_direction_north_flag",
            "wind_speed",
            "wind_speed_flag",
        ],
    )
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(
        df, meta_html, "depth_below_surface"
    )
    combine_df_to_excel(
        vs.staging + "combined/" + "tblJR19970421_AMT04_meterology_qa.xlsx",
        cleaned_df,
        dataset_meta,
        vars_meta,
    )


def process_amt_04_tsg():
    meta_html = pd.read_html(
        data_base_path + "AMT04/Bathymetry_Meteorology_TSG/amt04_TSG_metadata.html"
    )
    df = pd.read_csv(
        data_base_path + "AMT04/Bathymetry_Meteorology_TSG/amt04_TSG.txt",
        sep="\t",
        skiprows=13,
        index_col=False,
        names=[
            "cruise",
            "station",
            "type",
            "time",
            "lon",
            "lat",
            "local_cdi_ID",
            "EDMO_code",
            "bottle_depth",
            "depth_below_surface",
            "depth_below_surface_flag",
            "chl_a",
            "chl_a_flag",
            "fluorescence",
            "fluorescence_flag",
            "salinity",
            "salinity_flag",
            "temperature",
            "temperature_flag",
        ],
    )
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(
        df, meta_html, "depth_below_surface"
    )
    cleaned_df["depth"] = 6
    cleaned_df = cleaned_df[
        [
            "time",
            "lat",
            "lon",
            "depth",
            "station",
            "chl_a",
            "chl_a_flag",
            "fluorescence",
            "fluorescence_flag",
            "salinity",
            "salinity_flag",
            "temperature",
            "temperature_flag",
        ]
    ]
    combine_df_to_excel(
        vs.staging + "combined/" + "tblJR19970421_AMT04_TSG_qa.xlsx",
        cleaned_df,
        dataset_meta,
        vars_meta,
    )


def process_amt_04_CTD():
    meta_html = pd.read_html(data_base_path + "AMT04/CTD/1072493.html")
    txt_files = glob.glob(data_base_path + "AMT04/CTD/*.txt")
    new = [
        pd.read_csv(
            f,
            sep="\t",
            skiprows=13,
            index_col=False,
            names=[
                "cruise",
                "station",
                "type",
                "time",
                "lon",
                "lat",
                "local_cdi_ID",
                "EDMO_code",
                "bottle_depth",
                "sensor_pressure",
                "sensor_pressure_flag",
                "chl_a",
                "chl_a_flag",
                "fluorometer_voltage",
                "fluorometer_voltage_flag",
                "potential_temperature",
                "potential_temperature_flag",
                "salinity",
                "salinity_flag",
                "sigma_theta",
                "sigma_theta_flag",
                "temperature",
                "temperature_flag",
            ],
        )
        for f in txt_files
    ]
    df_concat = pd.concat(new).fillna(method="ffill")
    df_concat = df_concat[~df_concat["cruise"].isnull()]
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(
        df_concat, meta_html, "sensor_pressure"
    )
    combine_df_to_excel(
        vs.staging + "combined/" + "tblJR19970421_AMT04_CTD_qa.xlsx",
        cleaned_df,
        dataset_meta,
        vars_meta,
    )


def process_amt_04_extracted_chlorophyll_profiles():
    meta_html = pd.read_html(
        data_base_path + "AMT04/Extracted_Chlorophyll/1870667.html"
    )
    txt_files = glob.glob(data_base_path + "AMT04/Extracted_Chlorophyll/*.txt")
    new = [
        pd.read_csv(
            f,
            sep="\t",
            skiprows=12,
            index_col=False,
            names=[
                "cruise",
                "station",
                "type",
                "time",
                "lon",
                "lat",
                "local_cdi_ID",
                "EDMO_code",
                "bottle_depth",
                "depth_below_surface",
                "depth_below_surface_flag",
                "C22_flag",
                "C22_flag_quality",
                "chl_a",
                "chl_a_flag",
                "sample_reference",
                "sample_reference_flag",
            ],
        )
        for f in txt_files
    ]
    df_concat = pd.concat(new).fillna(method="ffill")
    df_concat = df_concat[~df_concat["cruise"].isnull()]
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(
        df_concat, meta_html, "depth_below_surface"
    )
    combine_df_to_excel(
        vs.staging
        + "combined/"
        + "tblJR19970421_AMT04_Extracted_Chlorophyll_Profiles_qa.xlsx",
        cleaned_df,
        dataset_meta,
        vars_meta,
    )


def process_amt_04_nutrient():
    meta_html = pd.read_html(data_base_path + "AMT04/Nutrient/1094022.html")
    txt_files = glob.glob(data_base_path + "AMT04/Nutrient/*.txt")
    new = [
        pd.read_csv(
            f,
            sep="\t",
            skiprows=15,
            index_col=False,
            names=[
                "cruise",
                "station",
                "type",
                "time",
                "lon",
                "lat",
                "local_cdi_ID",
                "EDMO_code",
                "bottle_depth",
                "depth_below_surf",
                "depth_below_surf_flag",
                "C22_flag",
                "C22_flag_quality",
                "nitrite",
                "nitrite_flag",
                "nitrate_nitrite",
                "nitrate_nitrite_flag",
                "phosphate",
                "phosphate_flag",
                "sample_reference",
                "sample_reference_flag",
                "silicate",
                "silicate_flag",
            ],
        )
        for f in txt_files
    ]
    df_concat = pd.concat(new).fillna(method="ffill")
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(
        df_concat, meta_html, "depth_below_surf"
    )
    combine_df_to_excel(
        vs.staging + "combined/" + "tblJR19970421_AMT04_Nutrient_qa.xlsx",
        cleaned_df,
        dataset_meta,
        vars_meta,
    )


def process_amt_04_pigments():
    meta_html = pd.read_html(data_base_path + "AMT04/Pigments/1872766.html")
    txt_files = glob.glob(data_base_path + "AMT04/Pigments/*.txt")
    new = [
        pd.read_csv(
            f,
            sep="\t",
            skiprows=33,
            index_col=False,
            names=[
                "cruise",
                "station",
                "type",
                "time",
                "lon",
                "lat",
                "local_cdi_ID",
                "EDMO_code",
                "bottle_depth",
                "depth_below_surface",
                "depth_below_surface_flag",
                "alloxanthin",
                "alloxanthin_flag",
                "beta_beta_carotene",
                "beta_beta_carotene_flag",
                "beta_epislon_carotene",
                "beta_epsilon_carotene_flag",
                "C22_flag",
                "C22_flag_quality",
                "butanoyloxyfucoxanthin_19",
                "butanoyloxyfucoxanthin_19_flag",
                "chlorophyll_c1_c2",
                "chlorophyll_c1_c2_flag",
                "chlorophyll_b",
                "chlorophyll_b_flag",
                "chlorophyll_c3",
                "chlorophyll_c3_flag",
                "chlorophyll_a",
                "chlorophyll_a_flag",
                "diadinoxanthin",
                "diadinoxanthin_flag",
                "diatoxanthin",
                "diatoxanthin_flag",
                "divinyl_chlorophyll_a",
                "divinyl_chlorophyll_a_flag",
                "fucoxanthin",
                "fucoxanthin_flag",
                "hexanoyloxyfucoxanthin_19",
                "hexanoyloxyfucoxanthin_19_flag",
                "lutein",
                "lutein_flag",
                "chlorophyll_a_divinyl_chlorophyll_a",
                "chlorophyll_a_divinyl_chlorophyll_a_flag",
                "monovinyl_chlorophyll_a",
                "monovinyl_chlorophyll_a_flag",
                "phaeophorbide",
                "phaeophorbide_flag",
                "peridinin",
                "peridinin_flag",
                "prasinoxanthin_fucoxanthin",
                "prasinoxanthin_fucoxanthin_flag",
                "phaeophytin",
                "phaeophytin_flag",
                "sample_reference",
                "sample_reference_flag",
                "violaxanthin",
                "violaxanthin_flag",
                "zeaxanthin",
                "zeaxanthin_flag",
            ],
        )
        for f in txt_files
    ]
    df_concat = pd.concat(new).fillna(method="ffill")
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(
        df_concat, meta_html, "depth_below_surface"
    )
    combine_df_to_excel(
        vs.staging + "combined/" + "tblJR19970421_AMT04_Pigments_qa.xlsx",
        cleaned_df,
        dataset_meta,
        vars_meta,
    )


#################
#    AMT 18     #
#################
def process_amt_18_flow_cyto():
    meta_html = pd.read_html(data_base_path + "AMT18/flow_cytometry/FlagData.html")
    df = pd.read_csv(
        data_base_path + "AMT18/flow_cytometry/AMT18_JR20081003_AFC_Dataset.csv",
        skiprows=1,
        names=[
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
            "cryptophyceae_abundance_J79A0596",
            "cryptophyceae_abundance_J79A0596_flag",
            "prymnesiophyceae_abundance_P490A00Z",
            "prymnesiophyceae_abundance_P490A00Z_flag",
            "synechococcus_abundance_P700A90Z_Tarran",
            "synechococcus_abundance_P700A90Z_Tarran_flag",
            "prochlorococcus_abundance_P701A90Z_Tarran",
            "prochlorococcus_abundance_P701A90Z_Tarran_flag",
            "picoeukaryotic_abundance_PYEUA00A_Tarran",
            "picoeukaryotic_abundance_PYEUA00A_Tarran_flag",
            "nanoeukaryotic_abundance_X726A86B",
            "nanoeukaryotic_abundance_X726A86B_flag",
        ],
    )
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(df, meta_html)
    combine_df_to_excel(
        vs.staging + "combined/" + "date_edit_tblJR20081003_AMT18_flow_cytometry.xlsx",
        cleaned_df,
        dataset_meta,
        vars_meta,
    )


def process_amt_19_flow_cyto():
    meta_html = pd.read_html(data_base_path + "AMT19/flow_cytometry/FlagData.html")
    df = pd.read_csv(
        data_base_path + "AMT19/flow_cytometry/AMT19_JC039_AFC_Dataset.csv",
        skiprows=1,
        names=[
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
            "cryptophyceae_abundance_J79A0596",
            "cryptophyceae_abundance_J79A0596_flag",
            "prymnesiophyceae_abundance_P490A00Z",
            "prymnesiophyceae_abundance_P490A00Z_flag",
            "synechococcus_abundance_P700A90Z_Tarran",
            "synechococcus_abundance_P700A90Z_Tarran_flag",
            "prochlorococcus_abundance_P701A90Z_Tarran",
            "prochlorococcus_abundance_P701A90Z_Tarran_flag",
            "picoeukaryotic_abundance_PYEUA00A_Tarran",
            "picoeukaryotic_abundance_PYEUA00A_Tarran_flag",
            "nanoeukaryotic_abundance_X726A86B",
            "nanoeukaryotic_abundance_X726A86B_flag",
        ],
    )
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(df, meta_html)
    combine_df_to_excel(
        vs.staging + "combined/" + "tblJC039_AMT19_flow_cytometry.xlsx",
        cleaned_df,
        dataset_meta,
        vars_meta,
    )


def process_amt_20_flow_cyto_unstained():
    meta_html = pd.read_html(
        data_base_path + "AMT20/flow_cytometry/unstained/FlagData.html"
    )
    df = pd.read_csv(
        data_base_path + "AMT20/flow_cytometry/unstained/AMT20_JC053_AFC_Dataset.csv",
        skiprows=1,
        names=[
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
            "cryptophyceae_abundance_J79A0596",
            "cryptophyceae_abundance_J79A0596_flag",
            "prymnesiophyceae_abundance_P490A00Z",
            "prymnesiophyceae_abundance_P490A00Z_flag",
            "synechococcus_abundance_P700A90Z_Tarran",
            "synechococcus_abundance_P700A90Z_Tarran_flag",
            "prochlorococcus_abundance_P701A90Z_Tarran",
            "prochlorococcus_abundance_P701A90Z_Tarran_flag",
            "picoeukaryotic_abundance_PYEUA00A_Tarran",
            "picoeukaryotic_abundance_PYEUA00A_Tarran_flag",
            "nanoeukaryotic_abundance_X726A86B",
            "nanoeukaryotic_abundance_X726A86B_flag",
        ],
    )
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(df, meta_html)
    combine_df_to_excel(
        vs.staging + "combined/" + "tblJC053_AMT20_unstained_flow_cytometry.xlsx",
        cleaned_df,
        dataset_meta,
        vars_meta,
    )


def process_amt_20_flow_cyto_stained():
    meta_html = pd.read_html(
        data_base_path + "AMT20/flow_cytometry/stained/FlagData.html"
    )
    df = pd.read_csv(
        data_base_path + "AMT20/flow_cytometry/stained/AMT20_AFC.csv",
        skiprows=1,
        names=[
            "CTD",
            "lat",
            "lon",
            "bottle_number",
            "depth",
            "bacterioplankton",
            "high_nucleic_acid_containing_bacteria",
            "low_nucleic_acid_containing_bacteria",
            "prochlorococcus_cyanobacteria",
            "aplastidic_protists",
            "plastidic_protists",
        ],
    )
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(df, meta_html)
    combine_df_to_excel(
        vs.staging + "combined/" + "tblJC053_AMT20_stained_flow_cytometry.xlsx",
        cleaned_df,
        dataset_meta,
        vars_meta,
    )


def process_amt_21_flow_cyto():
    meta_html = pd.read_html(data_base_path + "AMT21/flow_cytometry/FlagData.html")
    df = pd.read_csv(
        data_base_path + "AMT21/flow_cytometry/AMT21_D371_AFC_Dataset.csv",
        skiprows=1,
        names=[
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
            "cryptophyceae_abundance_J79A0596",
            "cryptophyceae_abundance_J79A0596_flag",
            "prymnesiophyceae_abundance_P490A00Z",
            "prymnesiophyceae_abundance_P490A00Z_flag",
            "synechococcus_abundance_P700A90Z_Tarran",
            "synechococcus_abundance_P700A90Z_Tarran_flag",
            "prochlorococcus_abundance_P701A90Z_Tarran",
            "prochlorococcus_abundance_P701A90Z_Tarran_flag",
            "picoeukaryotic_abundance_PYEUA00A_Tarran",
            "picoeukaryotic_abundance_PYEUA00A_Tarran_flag",
            "nanoeukaryotic_abundance_X726A86B",
            "nanoeukaryotic_abundance_X726A86B_flag",
        ],
    )
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(df, meta_html)
    combine_df_to_excel(
        vs.staging + "combined/" + "tblD371_AMT21_flow_cytometry.xlsx",
        cleaned_df,
        dataset_meta,
        vars_meta,
    )


def process_amt_22_flow_cyto():
    meta_html = pd.read_html(data_base_path + "AMT22/flow_cytometry/FlagData.html")
    df = pd.read_csv(
        data_base_path + "AMT22/flow_cytometry/AMT22_JC079_AFC_Dataset.csv",
        skiprows=1,
        names=[
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
            "cryptophyceae_abundance_J79A0596",
            "cryptophyceae_abundance_J79A0596_flag",
            "prymnesiophyceae_abundance_P490A00Z",
            "prymnesiophyceae_abundance_P490A00Z_flag",
            "synechococcus_abundance_P700A90Z_Tarran",
            "synechococcus_abundance_P700A90Z_Tarran_flag",
            "prochlorococcus_abundance_P701A90Z_Tarran",
            "prochlorococcus_abundance_P701A90Z_Tarran_flag",
            "picoeukaryotic_abundance_PYEUA00A_Tarran",
            "picoeukaryotic_abundance_PYEUA00A_Tarran_flag",
            "nanoeukaryotic_abundance_X726A86B",
            "nanoeukaryotic_abundance_X726A86B_flag",
        ],
    )
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(df, meta_html)
    combine_df_to_excel(
        vs.staging + "combined/" + "tblJC079_AMT22_flow_cytometry.xlsx",
        cleaned_df,
        dataset_meta,
        vars_meta,
    )


def process_amt_23_flow_cyto():
    meta_html = pd.read_html(data_base_path + "AMT23/flow_cytometry/FlagData.html")
    df = pd.read_csv(
        data_base_path + "AMT23/flow_cytometry/AMT23_JR20131005_AFC_Dataset.csv",
        skiprows=1,
        names=[
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
            "synechococcus_abundance_P700A90Z_Tarran",
            "synechococcus_abundance_P700A90Z_Tarran_flag",
            "picoeukaryotic_abundance_PYEUA00A_Tarran",
            "picoeukaryotic_abundance_PYEUA00A_Tarran_flag",
            "prymnesiophyceae_abundance_P490A00Z",
            "prymnesiophyceae_abundance_P490A00Z_flag",
            "nanoeukaryotic_abundance_X726A86B",
            "nanoeukaryotic_abundance_X726A86B_flag",
            "prochlorococcus_abundance_P701A90Z_Tarran",
            "prochlorococcus_abundance_P701A90Z_Tarran_flag",
            "cryptophyceae_abundance_J79A0596",
            "cryptophyceae_abundance_J79A0596_flag",
        ],
    )
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(df, meta_html)
    combine_df_to_excel(
        vs.staging + "combined/" + "tblJR20131005_AMT23_flow_cytometry.xlsx",
        cleaned_df,
        dataset_meta,
        vars_meta,
    )


def process_amt_24_flow_cyto():
    meta_html = pd.read_html(data_base_path + "AMT24/flow_cytometry/FlagData.html")
    df = pd.read_csv(
        data_base_path + "AMT24/flow_cytometry/AMT24_JR20140922_AFC_Dataset.csv",
        skiprows=1,
        names=[
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
            "cryptophyceae_abundance_J79A0596",
            "cryptophyceae_abundance_J79A0596_flag",
            "prymnesiophyceae_abundance_P490A00Z",
            "prymnesiophyceae_abundance_P490A00Z_flag",
            "synechococcus_abundance_P700A90Z_Tarran",
            "synechococcus_abundance_P700A90Z_Tarran_flag",
            "prochlorococcus_abundance_P701A90Z_Tarran",
            "prochlorococcus_abundance_P701A90Z_Tarran_flag",
            "picoeukaryotic_abundance_PYEUA00A_Tarran",
            "picoeukaryotic_abundance_PYEUA00A_Tarran_flag",
            "nanoeukaryotic_abundance_X726A86BX726A86B",
            "nanoeukaryotic_abundance_X726A86BX726A86B_flag",
        ],
    )
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(df, meta_html)
    combine_df_to_excel(
        vs.staging + "combined/" + "tblJR20140922_AMT24_flow_cytometry.xlsx",
        cleaned_df,
        dataset_meta,
        vars_meta,
    )


def process_amt_25_flow_cyto():
    meta_html = pd.read_html(data_base_path + "AMT25/flow_cytometry/FlagData.html")
    df = pd.read_csv(
        data_base_path + "AMT25/flow_cytometry/AMT25_JR15001_AFC_Dataset.csv",
        skiprows=1,
        names=[
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
            "cryptophyceae_abundance_J79A0596",
            "cryptophyceae_abundance_J79A0596_flag",
            "prymnesiophyceae_abundance_P490A00Z",
            "prymnesiophyceae_abundance_P490A00Z_flag",
            "synechococcus_abundance_P700A90Z_Tarran",
            "synechococcus_abundance_P700A90Z_Tarran_flag",
            "prochlorococcus_abundance_P701A90Z_Tarran",
            "prochlorococcus_abundance_P701A90Z_Tarran_flag",
            "picoeukaryotic_abundance_PYEUA00A_Tarran",
            "picoeukaryotic_abundance_PYEUA00A_Tarran_flag",
            "nanoeukaryotic_abundance_X726A86B",
            "nanoeukaryotic_abundance_X726A86B_flag",
        ],
    )
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(df, meta_html)
    combine_df_to_excel(
        vs.staging + "combined/" + "tblJR15001_AMT25_flow_cytometry.xlsx",
        cleaned_df,
        dataset_meta,
        vars_meta,
    )


def process_amt_26_flow_cyto():
    meta_html = pd.read_html(data_base_path + "AMT26/flow_cytometry/FlagData.html")
    df = pd.read_csv(
        data_base_path + "AMT26/flow_cytometry/AMT26_JR16001_AFC_Dataset.csv",
        skiprows=1,
        names=[
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
            "bacteria_abundance_C804B6A6",
            "bacteria_abundance_C804B6A6_flag",
            "cryptophyceae_abundance_J79A0596",
            "cryptophyceae_abundance_J79A0596_flag",
            "bacteria_abundance_P18318A9",
            "bacteria_abundance_P18318A9_flag",
            "prymnesiophyceae_abundance_P490A00Z",
            "prymnesiophyceae_abundance_P490A00Z_flag",
            "synechococcus_abundance_P700A90Z_Tarran",
            "synechococcus_abundance_P700A90Z_Tarran_flag",
            "prochlorococcus_abundance_P701A90Z_Tarran",
            "prochlorococcus_abundance_P701A90Z_Tarran_flag",
            "picoeukaryotic_abundance_PYEUA00A_Tarran",
            "picoeukaryotic_abundance_PYEUA00A_Tarran_flag",
            "nanoeukaryotic_abundance_X726A86B",
            "nanoeukaryotic_abundance_X726A86B_flag",
        ],
    )
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(df, meta_html)
    combine_df_to_excel(
        vs.staging + "combined/" + "tblJR16001_AMT26_flow_cytometry.xlsx",
        cleaned_df,
        dataset_meta,
        vars_meta,
    )


def process_amt_27_flow_cyto():
    meta_html = pd.read_html(data_base_path + "AMT27/flow_cytometry/FlagData.html")
    df = pd.read_csv(
        data_base_path + "AMT27/flow_cytometry/AMT27_DY084_AFC_Dataset.csv",
        skiprows=1,
        names=[
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
            "bacteria_abundance_C804B6A6",
            "bacteria_abundance_C804B6A6_flag",
            "cryptophyceae_abundance_J79A0596",
            "cryptophyceae_abundance_J79A0596_flag",
            "bacteria_abundance_P18318A9",
            "bacteria_abundance_P18318A9_flag",
            "prymnesiophyceae_abundance_P490A00Z",
            "prymnesiophyceae_abundance_P490A00Z_flag",
            "synechococcus_abundance_P700A90Z_Tarran",
            "synechococcus_abundance_P700A90Z_Tarran_flag",
            "prochlorococcus_abundance_P701A90Z_Tarran",
            "prochlorococcus_abundance_P701A90Z_Tarran_flag",
            "picoeukaryotic_abundance_PYEUA00A_Tarran",
            "picoeukaryotic_abundance_PYEUA00A_Tarran_flag",
            "nanoeukaryotic_abundance_X726A86B",
            "nanoeukaryotic_abundance_X726A86B_flag",
        ],
    )
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(df, meta_html)
    combine_df_to_excel(
        vs.staging + "combined/" + "tblDY084_AMT27_flow_cytometry.xlsx",
        cleaned_df,
        dataset_meta,
        vars_meta,
    )


def process_amt_28_flow_cyto():
    meta_html = pd.read_html(data_base_path + "AMT28/flow_cytometry/FlagData.html")
    df = pd.read_csv(
        data_base_path + "AMT28/flow_cytometry/AMT28_JR18001_AFC_Dataset.csv",
        skiprows=1,
        names=[
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
            "bacteria_abundance_C804B6A6",
            "bacteria_abundance_C804B6A6_flag",
            "cryptophyceae_abundance_J79A0596",
            "cryptophyceae_abundance_J79A0596_flag",
            "bacteria_abundance_P18318A9",
            "bacteria_abundance_P18318A9_flag",
            "prymnesiophyceae_abundance_P490A00Z",
            "prymnesiophyceae_abundance_P490A00Z_flag",
            "synechococcus_abundance_P700A90Z_Tarran",
            "synechococcus_abundance_P700A90Z_Tarran_flag",
            "prochlorococcus_abundance_P701A90Z_Tarran",
            "prochlorococcus_abundance_P701A90Z_Tarran_flag",
            "picoeukaryotic_abundance_PYEUA00A_Tarran",
            "picoeukaryotic_abundance_PYEUA00A_Tarran_flag",
            "nanoeukaryotic_abundance_X726A86B",
            "nanoeukaryotic_abundance_X726A86B_flag",
        ],
    )
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(df, meta_html)
    combine_df_to_excel(
        vs.staging + "combined/" + "tblJR18001_AMT28_flow_cytometry.xlsx",
        cleaned_df,
        dataset_meta,
        vars_meta,
    )


def process_amt_29_flow_cyto():
    meta_html = pd.read_html(data_base_path + "AMT29/flow_cytometry/FlagData.html")
    df = pd.read_csv(
        data_base_path + "AMT29/flow_cytometry/AMT29_DY110_AFC_Dataset.csv",
        skiprows=1,
        names=[
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
            "bacteria_abundance_C804B6A6",
            "bacteria_abundance_C804B6A6_flag",
            "cryptophyceae_abundance_J79A0596",
            "cryptophyceae_abundance_J79A0596_flag",
            "bacteria_abundance_P18318A9",
            "bacteria_abundance_P18318A9_flag",
            "prymnesiophyceae_abundance_P490A00Z",
            "prymnesiophyceae_abundance_P490A00Z_flag",
            "synechococcus_abundance_P700A90Z_Tarran",
            "synechococcus_abundance_P700A90Z_Tarran_flag",
            "prochlorococcus_abundance_P701A90Z_Tarran",
            "prochlorococcus_abundance_P701A90Z_Tarran_flag",
            "picoeukaryotic_abundance_PYEUA00A_Tarran",
            "picoeukaryotic_abundance_PYEUA00A_Tarran_flag",
            "nanoeukaryotic_abundance_X726A86B",
            "nanoeukaryotic_abundance_X726A86B_flag",
        ],
    )
    cleaned_df, dataset_meta, vars_meta = shared_AMT_data_processing(df, meta_html)
    combine_df_to_excel(
        vs.staging + "combined/" + "tblDY110_AMT29_flow_cytometry.xlsx",
        cleaned_df,
        dataset_meta,
        vars_meta,
    )


# AMT01
process_amt_01_bathemetry()
process_amt_01_meterology()
process_amt_01_tsg()
process_amt_01_CTD()
process_amt_01_Nutrient()

# AMT02
process_amt_02_bathemetry()
process_amt_02_meterology()
process_amt_02_tsg()
process_amt_02_CTD()
process_amt_02_extracted_pigments()
process_amt_02_nutrient_underway()
process_amt_02_nutrient_depth_profiles()

# AMT03
process_amt_03_bathemetry()
process_amt_03_meterology()
process_amt_03_tsg()
process_amt_03_CTD()
process_amt_03_extracted_chlorophyll()
process_amt_03_nutrient()

# AMT04

process_amt_04_bathemetry()
process_amt_04_meterology()
process_amt_04_tsg()
process_amt_04_CTD()
process_amt_04_extracted_chlorophyll_profiles()
process_amt_04_nutrient()
process_amt_04_pigments()


# process_amt_18_flow_cyto()
# process_amt_19_flow_cyto()
# process_amt_20_flow_cyto_unstained()
# process_amt_21_flow_cyto()
# process_amt_22_flow_cyto()
# process_amt_23_flow_cyto()
# process_amt_24_flow_cyto()
# process_amt_25_flow_cyto()
# process_amt_26_flow_cyto()
# process_amt_27_flow_cyto()
# process_amt_28_flow_cyto()
# process_amt_29_flow_cyto()
