import sys

from cmapingest import credentials as cr


import glob
import geopandas
from geopandas.tools import sjoin

import markdown
import pandas as pd
import pycmap
import numpy as np


from cmapingest import common as cmn
from cmapingest import cruise
from cmapingest import data

from cmapingest import DB
from cmapingest import vault_structure as vs

api = pycmap.API(token=cr.api_key)


def ID_Var_Map(series_to_map, res_col, tableName, server):
    query = f"""SELECT * FROM {tableName}"""
    call = DB.dbRead(query, server)
    series = series_to_map.astype(str).str.lower()
    sdict = dict(zip(call[res_col].str.lower(), call.ID))
    mapped_series = series.map(sdict)
    mapped_series = list(cmn.nanToNA(mapped_series).replace("", 0))
    return mapped_series


def import_metadata(branch, tableName):
    branch_path = cmn.vault_struct_retrieval(branch)
    ds_meta_list = glob.glob(
        branch_path + tableName + "/metadata/" + "*dataset_metadata*"
    )
    vars_meta_list = glob.glob(
        branch_path + tableName + "/metadata/" + "*vars_metadata*"
    )
    dataset_metadata_df = pd.read_csv(ds_meta_list[0], sep=",")
    vars_metadata_df = pd.read_csv(vars_meta_list[0], sep=",")

    dataset_metadata_df = cmn.nanToNA(cmn.strip_whitespace_headers(dataset_metadata_df))
    vars_metadata_df = cmn.nanToNA(cmn.strip_whitespace_headers(vars_metadata_df))
    return dataset_metadata_df, vars_metadata_df


def tblDatasets_Insert(dataset_metadata_df, tableName, server):
    last_dataset_ID = cmn.get_last_ID("tblDatasets", server) + 1
    dataset_metadata_df = cmn.nanToNA(dataset_metadata_df)
    dataset_metadata_df.replace({"'": "''"}, regex=True, inplace=True)
    Dataset_Name = dataset_metadata_df["dataset_short_name"].iloc[0]
    Dataset_Long_Name = dataset_metadata_df["dataset_long_name"].iloc[0]
    Dataset_Version = dataset_metadata_df["dataset_version"].iloc[0]
    Dataset_Release_Date = dataset_metadata_df["dataset_release_date"].iloc[0]
    Dataset_Make = dataset_metadata_df["dataset_make"].iloc[0]
    Data_Source = dataset_metadata_df["dataset_source"].iloc[0]
    Distributor = dataset_metadata_df["dataset_distributor"].iloc[0]
    Acknowledgement = (
        dataset_metadata_df["dataset_acknowledgement"].iloc[0].replace("\xa0", " ")
    )
    Contact_Email = ""  # dataset_metadata_df["contact_email"].iloc[0]
    Dataset_History = dataset_metadata_df["dataset_history"].iloc[0]
    Description = (
        dataset_metadata_df["dataset_description"]
        .iloc[0]
        .replace("'", "")
        .replace("’", "")
        .replace("‘", "")
        .replace("\n", "")
        .replace("\xa0", "")
    )
    Climatology = dataset_metadata_df["climatology"].iloc[0]
    Db = "Opedia"
    # Temps
    Variables = ""
    Doc_URL = ""
    Icon_URL = f"""https://raw.githubusercontent.com/simonscmap/static/master/mission_icons/{tableName}.png"""

    query = (
        last_dataset_ID,
        Db,
        Dataset_Name,
        Dataset_Long_Name,
        Variables,
        Data_Source,
        Distributor,
        Description,
        Climatology,
        Acknowledgement,
        Doc_URL,
        Icon_URL,
        Contact_Email,
        Dataset_Version,
        Dataset_Release_Date,
        Dataset_History,
    )
    columnList = "(ID,DB,Dataset_Name,Dataset_Long_Name,Variables,Data_Source,Distributor,Description,Climatology,Acknowledgement,Doc_URL,Icon_URL,Contact_Email,Dataset_Version,Dataset_Release_Date,Dataset_History)"
    try:
        DB.lineInsert(
            server, "[opedia].[dbo].[tblDatasets]", columnList, query, ID_insert=True
        )
    except:
        DB.lineInsert(
            server, "[opedia].[dbo].[tblDatasets]", columnList, query, ID_insert=False
        )
    print("Metadata inserted into tblDatasets.")


def tblDataset_References_Insert(dataset_metadata_df, server, DOI_link_append=None):

    Dataset_Name = dataset_metadata_df["dataset_short_name"].iloc[0]
    IDvar = cmn.getDatasetID_DS_Name(Dataset_Name, server)
    columnList = "(Dataset_ID, Reference)"
    reference_list = (
        dataset_metadata_df["dataset_references"]
        .str.replace("\xa0", " ")
        .replace("", np.nan)
        .dropna()
        .to_list()
    )
    if DOI_link_append != None:
        reference_list.append(DOI_link_append)

    for ref in reference_list:
        query = (IDvar, ref)
        DB.lineInsert(
            server, "[opedia].[dbo].[tblDataset_References]", columnList, query
        )
    print("Inserting data into tblDataset_References.")


def tblVariables_Insert(
    data_df,
    dataset_metadata_df,
    variable_metadata_df,
    Table_Name,
    server,
    process_level="REP",
    CRS="CRS",
):
    Db_list = len(variable_metadata_df) * ["Opedia"]
    IDvar_list = len(variable_metadata_df) * [
        cmn.getDatasetID_DS_Name(
            dataset_metadata_df["dataset_short_name"].iloc[0], server
        )
    ]
    Table_Name_list = len(variable_metadata_df) * [Table_Name]
    Short_Name_list = variable_metadata_df["var_short_name"].tolist()
    Long_Name_list = variable_metadata_df["var_long_name"].tolist()
    Unit_list = variable_metadata_df["var_unit"].tolist()
    Temporal_Res_ID_list = ID_Var_Map(
        variable_metadata_df["var_temporal_res"],
        "Temporal_Resolution",
        "tblTemporal_Resolutions",
        server,
    )
    Spatial_Res_ID_list = ID_Var_Map(
        variable_metadata_df["var_spatial_res"],
        "Spatial_Resolution",
        "tblSpatial_Resolutions",
        server,
    )
    Temporal_Coverage_Begin_list, Temporal_Coverage_End_list = cmn.getColBounds(
        data_df, "time", list_multiplier=len(variable_metadata_df)
    )
    Lat_Coverage_Begin_list, Lat_Coverage_End_list = cmn.getColBounds(
        data_df, "lat", list_multiplier=len(variable_metadata_df)
    )
    Lon_Coverage_Begin_list, Lon_Coverage_End_list = cmn.getColBounds(
        data_df, "lon", list_multiplier=len(variable_metadata_df)
    )
    Grid_Mapping_list = [CRS] * len(variable_metadata_df)
    Sensor_ID_list = ID_Var_Map(
        variable_metadata_df["var_sensor"], "Sensor", "tblSensors", server
    )
    Make_ID_list = len(variable_metadata_df) * list(
        ID_Var_Map(
            dataset_metadata_df["dataset_make"].head(1), "Make", "tblMakes", server
        )
    )
    Process_ID_list = ID_Var_Map(
        pd.Series(len(variable_metadata_df) * [process_level]),
        "Process_Stage",
        "tblProcess_Stages",
        server,
    )
    Study_Domain_ID_list = ID_Var_Map(
        variable_metadata_df["var_discipline"],
        "Study_Domain",
        "tblStudy_Domains",
        server,
    )
    Comment_list = cmn.nanToNA(variable_metadata_df["var_comment"]).to_list()
    Visualize_list = cmn.nanToNA(variable_metadata_df["visualize"]).tolist()
    Data_Type_list = cmn.getTableName_Dtypes(Table_Name, server)["DATA_TYPE"].tolist()
    columnList = "(ID,DB, Dataset_ID, Table_Name, Short_Name, Long_Name, Unit, Temporal_Res_ID, Spatial_Res_ID, Temporal_Coverage_Begin, Temporal_Coverage_End, Lat_Coverage_Begin, Lat_Coverage_End, Lon_Coverage_Begin, Lon_Coverage_End, Grid_Mapping, Make_ID, Sensor_ID, Process_ID, Study_Domain_ID, Comment, Visualize, Data_Type)"

    for (
        Db,
        IDvar,
        Table_Name,
        Short_Name,
        Long_Name,
        Unit,
        Temporal_Res_ID,
        Spatial_Res_ID,
        Temporal_Coverage_Begin,
        Temporal_Coverage_End,
        Lat_Coverage_Begin,
        Lat_Coverage_End,
        Lon_Coverage_Begin,
        Lon_Coverage_End,
        Grid_Mapping,
        Make_ID,
        Sensor_ID,
        Process_ID,
        Study_Domain_ID,
        Comment,
        Visualize,
        Data_Type,
    ) in zip(
        Db_list,
        IDvar_list,
        Table_Name_list,
        Short_Name_list,
        Long_Name_list,
        Unit_list,
        Temporal_Res_ID_list,
        Spatial_Res_ID_list,
        Temporal_Coverage_Begin_list,
        Temporal_Coverage_End_list,
        Lat_Coverage_Begin_list,
        Lat_Coverage_End_list,
        Lon_Coverage_Begin_list,
        Lon_Coverage_End_list,
        Grid_Mapping_list,
        Make_ID_list,
        Sensor_ID_list,
        Process_ID_list,
        Study_Domain_ID_list,
        Comment_list,
        Visualize_list,
        Data_Type_list,
    ):
        last_var_ID = cmn.get_last_ID("tblVariables", server) + 1
        query = (
            last_var_ID,
            Db,
            IDvar,
            Table_Name,
            Short_Name,
            Long_Name,
            Unit,
            Temporal_Res_ID,
            Spatial_Res_ID,
            Temporal_Coverage_Begin,
            Temporal_Coverage_End,
            Lat_Coverage_Begin,
            Lat_Coverage_End,
            Lon_Coverage_Begin,
            Lon_Coverage_End,
            Grid_Mapping,
            Make_ID,
            Sensor_ID,
            Process_ID,
            Study_Domain_ID,
            Comment,
            Visualize,
            Data_Type,
        )

        DB.lineInsert(
            server, "[opedia].[dbo].[tblVariables]", columnList, query, ID_insert=True
        )

    print("Inserting data into tblVariables")


def tblKeywords_Insert(variable_metadata_df, dataset_metadata_df, Table_Name, server):
    IDvar = cmn.getDatasetID_DS_Name(
        dataset_metadata_df["dataset_short_name"].iloc[0], server
    )
    for index, row in variable_metadata_df.iterrows():
        VarID = cmn.findVarID(
            IDvar, variable_metadata_df.loc[index, "var_short_name"], server
        )
        keyword_list = (variable_metadata_df.loc[index, "var_keywords"]).split(",")
        for keyword in keyword_list:
            keyword = keyword.lstrip()
            query = (VarID, keyword)
            print(query)
            if len(keyword) > 0:  # won't insert empty values
                try:  # Cannot insert duplicate entries, so skips if duplicate
                    DB.lineInsert(
                        server,
                        "[opedia].[dbo].[tblKeywords]",
                        "(var_ID, keywords)",
                        query,
                    )
                except Exception as e:
                    print(e)


def user_input_build_cruise(df, dataset_metadata_df, server):
    tblCruise_df, cruise_name = cruise.build_cruise_metadata_from_user_input(df)
    print("The cruise metadata dataframe you generated looks like: ")
    print(tblCruise_df)
    meta_cont = input(
        "Do you want to ingest this [y], cancel the proces [n] or go through the metadata build again [r]? "
    )
    if meta_cont.lower() == "r":
        tblCruise_df, cruise_name = cruise.build_cruise_metadata_from_user_input(df)
    elif meta_cont.lower() == "y":
        DB.lineInsert(
            server,
            "tblCruise",
            "(Nickname,Name,Ship_Name,Start_Time,End_Time,Lat_Min,Lat_Max,Lon_Min,Lon_Max,Chief_Name)",
            tuple(tblCruise_df.iloc[0].astype(str).to_list()),
        )
    elif meta_cont.lower() == "n":
        sys.exit()

    Cruise_ID = cmn.get_cruise_IDS([cruise_name], server)
    rdf = cruise.resample_trajectory(df)
    traj_df = cruise.return_cruise_trajectory_from_df(rdf, Cruise_ID)
    # Dev note, in future generate map for QA? add user input
    data.data_df_to_db(
        traj_df, "tblCruise_Trajectory", server, clean_data_df_flag=False
    )


def tblDataset_Cruises_Insert(data_df, dataset_metadata_df, server):

    matched_cruises, unmatched_cruises = cmn.verify_cruise_lists(
        dataset_metadata_df, server
    )
    print("matched: ")
    print(matched_cruises)
    print("\n")
    print("umatched: ")
    print(unmatched_cruises)
    if matched_cruises == []:
        build_traj_yn = input(
            "Do you want to build cruise trajectory and metadata from this dataset? [Y/n]: "
        )
        if build_traj_yn.lower() == "y":
            print(
                "Building cruise trajectory and metadata from this dataset and user input. "
            )
            user_input_build_cruise(data_df, dataset_metadata_df, server)
            matched_cruises, unmatched_cruises = cmn.verify_cruise_lists(
                dataset_metadata_df, server
            )
    cruise_ID_list = cmn.get_cruise_IDS(matched_cruises, server)
    dataset_ID = cmn.getDatasetID_DS_Name(
        dataset_metadata_df["dataset_short_name"].iloc[0], server
    )
    for cruise_ID in cruise_ID_list:
        query = (dataset_ID, cruise_ID)
        DB.lineInsert(
            server,
            "[opedia].[dbo].[tblDataset_Cruises]",
            "(Dataset_ID, Cruise_ID)",
            query,
        )
    print("Dataset matched to cruises")


def deleteFromtblKeywords(Dataset_ID, server):
    Keyword_ID_list = cmn.getKeywordsIDDataset(Dataset_ID, server)
    Keyword_ID_str = "','".join(str(key) for key in Keyword_ID_list)
    # print(Keyword_ID_str)
    cur_str = (
        """DELETE FROM [Opedia].[dbo].[tblKeywords] WHERE [var_ID] IN ('"""
        + Keyword_ID_str
        + """')"""
    )
    DB.DB_modify(cur_str, server)
    print("tblKeyword entries deleted for Dataset_ID: ", Dataset_ID)


def deleteFromtblDataset_Stats(Dataset_ID, server):
    cur_str = (
        """DELETE FROM [Opedia].[dbo].[tblDataset_Stats] WHERE [Dataset_ID] = """
        + str(Dataset_ID)
    )
    DB.DB_modify(cur_str, server)
    print("tblDataset_Stats entries deleted for Dataset_ID: ", Dataset_ID)


def deleteFromtblDataset_Cruises(Dataset_ID, server):
    cur_str = (
        """DELETE FROM [Opedia].[dbo].[tblDataset_Cruises] WHERE [Dataset_ID] = """
        + str(Dataset_ID)
    )
    DB.DB_modify(cur_str, server)
    print("tblDataset_Cruises entries deleted for Dataset_ID: ", Dataset_ID)


def deleteFromtblDataset_Regions(Dataset_ID, server):
    cur_str = (
        """DELETE FROM [Opedia].[dbo].[tblDataset_Regions] WHERE [Dataset_ID] = """
        + str(Dataset_ID)
    )
    DB.DB_modify(cur_str, server)
    print("tblDataset_Regions entries deleted for Dataset_ID: ", Dataset_ID)


def deleteFromtblDataset_References(Dataset_ID, server):
    cur_str = (
        """DELETE FROM [Opedia].[dbo].[tblDataset_References] WHERE [Dataset_ID] = """
        + str(Dataset_ID)
    )
    DB.DB_modify(cur_str, server)
    print("tblDataset_References entries deleted for Dataset_ID: ", Dataset_ID)


def deleteFromtblVariables(Dataset_ID, server):
    cur_str = (
        """DELETE FROM [Opedia].[dbo].[tblVariables] WHERE [Dataset_ID] = """
        + str(Dataset_ID)
    )
    DB.DB_modify(cur_str, server)
    print("tblVariables entries deleted for Dataset_ID: ", Dataset_ID)


def deleteFromtblDatasets(Dataset_ID, server):
    cur_str = """DELETE FROM [Opedia].[dbo].[tblDatasets] WHERE [ID] = """ + str(
        Dataset_ID
    )
    DB.DB_modify(cur_str, server)
    print("tblDataset entries deleted for Dataset_ID: ", Dataset_ID)


def dropTable(tableName, server):
    cur_str = """DROP TABLE """ + tableName
    DB.DB_modify(cur_str, server)
    print(tableName, " Removed from DB")


def deleteCatalogTables(tableName, server):
    contYN = input(
        "Are you sure you want to delete all of the catalog tables for "
        + tableName
        + " ?  [yes/no]: "
    )
    Dataset_ID = cmn.getDatasetID_Tbl_Name(tableName, server)
    if contYN == "yes":
        deleteFromtblKeywords(Dataset_ID, server)
        deleteFromtblDataset_Stats(Dataset_ID, server)
        deleteFromtblDataset_Cruises(Dataset_ID, server)
        deleteFromtblDataset_Regions(Dataset_ID, server)
        deleteFromtblDataset_References(Dataset_ID, server)
        deleteFromtblVariables(Dataset_ID, server)
        deleteFromtblDatasets(Dataset_ID, server)
        dropTable(tableName, server)
        # dropTable(tableName, "Mariana")
    else:
        print("Catalog tables for ID" + Dataset_ID + " not deleted")


def removeKeywords(keywords_list, var_short_name_list, tableName, server):
    """Removes a list of keywords for list of variables in a table"""

    keywords_list = cmn.lowercase_List(keywords_list)
    """Removes keyword from specific variable in table"""
    keyword_IDs = str(
        tuple(cmn.getKeywordIDsTableNameVarName(tableName, var_short_name_list))
    )
    cur_str = (
        """DELETE FROM [Opedia].[dbo].[tblKeywords] WHERE [var_ID] IN """
        + keyword_IDs
        + """ AND LOWER([keywords]) IN """
        + str(tuple(keywords_list))
    )
    DB.DB_modify(cur_str, server)
    print(
        "tblKeyword entries deleted for: ",
        keywords_list,
        var_short_name_list,
        tableName,
    )


def addKeywords(keywords_list, tableName, server, var_short_name_list="*"):
    if var_short_name_list == "*":
        var_short_name_list = cmn.get_var_list_dataset(tableName)
    """Inserts list of keywords for list of variables in a table"""
    keywords_list = cmn.lowercase_List(keywords_list)
    """Removes keyword from specific variable in table"""
    keyword_IDs = cmn.getKeywordIDsTableNameVarName(tableName, var_short_name_list)
    columnList = "(var_ID, keywords)"
    for var_ID in keyword_IDs:
        for keyword in keywords_list:
            query = """('{var_ID}', '{keyword}')""".format(
                var_ID=var_ID, keyword=keyword
            )
            cur_str = """INSERT INTO [Opedia].[dbo].[tblKeywords] {columnList} VALUES {query}""".format(
                columnList=columnList, query=query
            )
            try:
                DB.lineInsert(server, "[opedia].[dbo].[tblKeywords]", columnList, query)
                print("Added keyword: " + keyword)
            except Exception as e:
                print(e)


"""
###############################################
###############################################
     Ocean Region Classification Functions
###############################################
###############################################
"""


def geopandas_load_gpkg(input_df):
    """

    Args:
        input_df (Pandas DataFrame): CMAP formatted DataFrame
    Returns:
        gdf (Geopandas GeoDataFrame): Geopandas formatted DataFrame. ie. geometry column.
    """
    gdf = geopandas.GeoDataFrame(
        input_df, geometry=geopandas.points_from_xy(input_df.lon, input_df.lat)
    )
    return gdf


def load_gpkg_ocean_region(input_gpkg_fpath):
    """Uses Geopandas to load input geopackage (gpkg)

    Args:
        input_gpkg_fpath (Geopackage - .gpkg): Input Geopackage containing geometries used for ocean region classifcation.
    Returns:
        gpkg_region (GeoDataFrame): Outputs geodataframe of ocean region geometries.
    """
    gdf = geopandas.read_file(input_gpkg_fpath)
    return gdf


def classify_gdf_with_gpkg_regions(data_gdf, region_gdf):
    """Takes sparse data geodataframe and classifies it to an ocean region

    Args:
        data_gdf (geopandas geodataframe): A geodataframe created from the input CMAP dataframe.
        region_gdf (geopandas geodataframe): A geodataframe created from ocean region gpkg.
    """
    classified_gdf = sjoin(data_gdf, region_gdf, how="left", op="within")
    # This line removes any rows where null exists. This might be do to points to close to land.
    classified_gdf_nonull = classified_gdf[~classified_gdf["NAME"].isnull()]
    return classified_gdf_nonull


def classified_gdf_to_list(classified_gdf):
    """Takes a classified/joined geodataframe and returns a set of ocean regions

    Args:
        classified_gdf (geopandas geodataframe): The joined geodataframe that contains points as well as regions.
    Returns:
        region_set : A unique list of regions belonging to that dataset.
    """
    region_set = list(set(classified_gdf["NAME"]))
    return region_set


def ocean_region_classification(data_df, dataset_name, server):
    """This function geographically classifes a sparse dataset into a specific ocean region

    Args:
        df (Pandas DataFrame): Input CMAP formatted DataFrame (ST Index: time,lat,lon,<depth>)
        dataset_name (string): name of dataset in CMAP
    """

    data_gdf = geopandas_load_gpkg(data_df)
    region_gdf = load_gpkg_ocean_region(
        vs.spatial_data + "World_Seas_IHO_v1_simplified/World_Seas_Simplifed.gpkg"
    )
    classified_gdf = classify_gdf_with_gpkg_regions(data_gdf, region_gdf)
    region_set = classified_gdf_to_list(classified_gdf)

    dataset_ID = cmn.getDatasetID_DS_Name(dataset_name, server)
    region_ID_list = cmn.get_region_IDS(region_set)
    print("Dataset matched to the following Regions: ", region_set)

    for region_ID in region_ID_list:
        query = (dataset_ID, region_ID)
        DB.lineInsert(
            server,
            "[opedia].[dbo].[tblDataset_Regions]",
            "(Dataset_ID, Region_ID)",
            query,
        )


# insitu_df = DB.dbRead(
#     """select distinct tblD.Dataset_Name, tblV.Table_Name from tblDatasets tblD
# inner join
# tblVariables tblV
# on tblD.ID = tblV.Dataset_ID

# where tblV.Make_ID = 1
# and tblV.Sensor_ID <> 1
# AND
# tblD.Dataset_Name <>  'Argo_BGC_REP' AND tblD.Dataset_Name <>  'ESV' AND tblD.Dataset_Name <>  'Global_Drifter_Program' AND tblD.Dataset_Name <> 'WOA_Climatology'"""
# )


def if_exists_dataset_region(dataset_name, server):
    """Checks if dataset ID is already in tblDatasets_Regions

    Args:
        dataset_name (string): The short name of the dataset in CMAP tblDatasets.
    Returns: Boolean
    """
    ds_ID = cmn.getDatasetID_DS_Name(dataset_name, server)
    cur_str = """SELECT * FROM [Opedia].[dbo].[tblDataset_Regions] WHERE [Dataset_ID] = {Dataset_ID}""".format(
        Dataset_ID=ds_ID
    )
    query_return = DB.dbRead(cur_str, server)
    if query_return.empty:
        bool_return = False
    else:
        bool_return = True
    return bool_return


# for index, row in insitu_df.iterrows():
#     print(row['Dataset_Name'], row['Table_Name'])
#     bool_return = if_exists_dataset_region(row['Dataset_Name'])
#     if bool_return == False:
#         data_df = DB.dbRead("""SELECT * FROM {tblname}""".format(tblname=row['Table_Name']))
#         ocean_region_classification(data_df,row['Dataset_Name'])


# for val in region_gdf["NAME"]:
#     DB.DB_modify("""INSERT INTO tblRegions (Region_Name) VALUES ('{val}')""".format(val=val),"Rainier")


""" tblDatasets: ID,Name, etc...
tblDataset_Region: Dataset_ID, Region_ID
tblRegion: ID, Name of Region


-Fill tblDataset_Region with ID's from gpkg...When/where?
-How to handle if Name of Region is missing...Warning/Flag? Warning then add..
-

"""
