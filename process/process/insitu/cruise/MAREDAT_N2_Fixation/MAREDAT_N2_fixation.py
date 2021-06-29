from cmapingest import vault_structure as vs
import pandas as pd

raw_data_path = vs.collected_data + "MAREDAT_N2_Fixation/MAREDAT_diazotroph.xls"

cell_count = pd.read_excel(
    raw_data_path,
    sheet_name="Cell_Count",
    names=[
        "SOURCE_Data",
        "SOURCE_Related_article",
        "METHODS",
        "time",
        "lat",
        "lon",
        "depth",
        "trichodesmium_colonies",
        "trichodesmium_free_trichomes",
        "trichodesmium_total_trichomes",
        "trichodesmium_biomass_conversion_factor",
        "trichodesmium_biomass",
        "UCYN_cells",
        "UCYN_biomass_conversion_facto",
        "UCYN_biomass",
        "richelia_associated_species",
        "richelia_cells",
        "richelia_biomass_conversion_factor)",
        "calothrix_associated_species",
        "calothrix_cells",
        "calothrix_biomass_conversion_factor",
        "heterocyst_richelia_calotrhix_biomass",
        "total_diazotrophic_biomass",
        "temperature",
        "salinity",
        "nitrate",
        "phosphate",
        "Fe",
        "chlorophyll",
        "notes",
    ],
)
cell_count.drop(
    ["SOURCE_Data", "SOURCE_Related_article", "METHODS"], axis=1, inplace=True
)


cell_count_integral = pd.read_excel(raw_data_path, sheet_name="Cell_Count_Integral")
n2_fixation_rate = pd.read_excel(raw_data_path, sheet_name="N2_Fixation_Rate")
n2_fixation_rate_integral = pd.read_excel(
    raw_data_path, sheet_name="N2_Fixation_Rate_Integral"
)
nifH_Gene = pd.read_excel(raw_data_path, sheet_name="nifH_Gene")
nifH_Gene_integral = pd.read_excel(raw_data_path, sheet_name="nifH_Gene_Integral")
