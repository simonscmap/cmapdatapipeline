#!/usr/bin/env python3

"""
Author: Norland Raphael Hagen <norlandrhagen@gmail.com>
Date: 07-23-2021

cmapdata - general - main ingestion wrapper functions
"""

import sys
import os
import glob
import pandas as pd
import numpy as np

sys.path.append("..")

import credentials as cr

import pycmap

pycmap.API(cr.api_key)
import argparse


import vault_structure as vs
import transfer
import data
import DB
import metadata
import SQL
import mapping
import stats
import common as cmn
import cruise
import data_checks as dc


def getBranch_Path(args):
    """Wrapper function that returns branchpath given branch input. ex. float, cruise etc."""
    branch_path = cmn.vault_struct_retrieval(args.branch)
    return branch_path


def splitExcel(staging_filename, data_missing_flag):
    """Wrapper function for transfer.single_file_split."""
    transfer.single_file_split(staging_filename, data_missing_flag)

def splitVaultExcel(staging_filename, branch, tableName, data_missing_flag):
    """Wrapper function for transfer.single_file_split."""
    transfer.single_file_vault_split(staging_filename, branch, tableName, data_missing_flag)


def splitCruiseExcel(staging_filename, cruise_name):
    """Wrapper function for transfer.cruise_file_split"""
    transfer.cruise_file_split(staging_filename, cruise_name)


def staging_to_vault(
    staging_filename, branch, tableName, remove_file_flag, skip_data_flag, process_level
):
    """Wrapper function for transfer.staging_to_vault"""
    transfer.staging_to_vault(
        staging_filename,
        branch,
        tableName,
        remove_file_flag,
        skip_data_flag,
        process_level,
    )

def validator_to_vault(
    staging_filename, branch, tableName
):
    """Wrapper function for transfer.validator_to_vault"""
    transfer.validator_to_vault(
        staging_filename,
        branch,
        tableName
    )

def cruise_staging_to_vault(cruise_name, remove_file_flag):
    """Wrapper function for transfer.cruise_staging_to_vault"""
    transfer.cruise_staging_to_vault(cruise_name, remove_file_flag)


def import_cruise_data_dict(cruise_name):
    """Imports cruise metadata and trajectory dataframes into pandas and returns a data dict of both dataframes"""
    cruise_path = vs.r2r_cruise + cruise_name
    print(cruise_name)
    print(cruise_path)
    metadata_df = pd.read_csv(
        cruise_path + f"""/metadata/{cruise_name}_cruise_metadata.csv""", sep=","
    )
    traj_df = pd.read_csv(
        cruise_path + f"""/trajectory/{cruise_name}_cruise_trajectory.csv""", sep=","
    )
    metadata_df = metadata_df[
        metadata_df.columns.drop(list(metadata_df.filter(regex="Unnamed:")))
    ]
    traj_df = traj_df[traj_df.columns.drop(list(traj_df.filter(regex="Unnamed:")))]
    traj_df["time"] = pd.to_datetime(
        traj_df["time"].astype(str), format="%Y-%m-%d %H:%M:%S"
    ).astype("datetime64[s]")

    data_dict = {"metadata_df": metadata_df, "trajectory_df": traj_df}
    return data_dict


def importDataMemory(branch, tableName, process_level):
    """Given a branch, tablename and process level, imports data, dataset_metadata and vars_metadata into pandas DataFrames, wrapped in a dictionary.

    Args:
        branch (str): vault branch path, ex float.
        tableName (str): CMAP table name
        process_level (str): rep or nrt

    Returns:
        dictionary: dictionary containing data, dataset_metadata and vars_metadata dataframes.
    """
    data_file_name = data.fetch_single_datafile(branch, tableName, process_level)
    data_df = data.read_csv(data_file_name)
    dataset_metadata_df, variable_metadata_df = metadata.import_metadata(
        branch, tableName
    )
    data_dict = {
        "data_df": data_df,
        "dataset_metadata_df": dataset_metadata_df,
        "variable_metadata_df": variable_metadata_df,
    }
    return data_dict


def SQL_suggestion(data_dict, tableName, branch, server, db_name):
    """Creates suggested SQL table based on data types of input dataframe.

    Args:
        data_dict (dictionary): Input data dictionary containing all three sheets.
        tableName (str): CMAP table name
        branch (str): vault branch path, ex float.
        server (str): Valid CMAP server
    """
    if branch != "model" or branch != "satellite":
        make = "observation"
    else:
        make = branch
    cdt = SQL.build_SQL_suggestion_df(data_dict["data_df"])
    sql_tbl = SQL.SQL_tbl_suggestion_formatter(cdt, tableName, server, db_name)
    sql_index = SQL.SQL_index_suggestion_formatter(
        data_dict["data_df"], tableName, server, db_name
    )
    sql_combined_str = sql_tbl["sql_tbl"] + sql_index["sql_index"]
    print(sql_combined_str)
    contYN = input("Do you want to build this table in SQL? " + " ?  [yes/no]: ")
    if contYN.lower() == "yes":
        DB.DB_modify(sql_tbl["sql_tbl"], server)
        DB.DB_modify(sql_index["sql_index"], server)

    else:
        sys.exit()
    SQL.write_SQL_file(sql_combined_str, tableName, make)


def add_ST_cols_cruise(metadata_df, traj_df):
    """Wrapper function for cruiseadd_ST_cols_to_metadata_df"""
    metadata_df = cruise.add_ST_cols_to_metadata_df(metadata_df, traj_df)
    return metadata_df


def insertCruise(metadata_df, trajectory_df, cruise_name, db_name, server):
    """Inserts metadata_df, trajectory_df into server as well as ocean region classifcation into tblCruise_Regions. If you want to add more to the template such as keywords etc, they could be included here.

    Args:
        metadata_df (Pandas DataFrame): cruise metadata dataframe
        trajectory_df (Pandas DataFrame): cruise trajectory dataframe
        cruise_name (str): Valid CMAP cruise name (UNOLS ex. KM1906)
        Server (str): Valid CMAP server name
    """
    metadata_df = cmn.nanToNA(metadata_df)
    print(metadata_df['Cruise_Series'][0])
    
    if metadata_df['Cruise_Series'][0] == ' ' or metadata_df['Cruise_Series'][0] == '':
        metadata_df.at[0, 'Cruise_Series'] = 'NULL'
    
    query = 'SELECT MAX(ID) FROM tblCruise'
    max_id = DB.dbRead(query, server)

    if server.lower() == 'mariana':  
        insert_id = max_id.iloc[0,0] + 1
        metadata_df.insert(0,'ID',insert_id)
        print(metadata_df)       

        DB.lineInsert(
            server,
            "tblCruise",
            "(ID,Nickname,Name,Ship_Name,Start_Time,End_Time,Lat_Min,Lat_Max,Lon_Min,Lon_Max,Chief_Name,Cruise_Series)",
            tuple(metadata_df.iloc[0].astype(str).to_list()),
        )    
    else:
        DB.lineInsert(
            server,
            "tblCruise",
            "(Nickname,Name,Ship_Name,Start_Time,End_Time,Lat_Min,Lat_Max,Lon_Min,Lon_Max,Chief_Name,Cruise_Series)",
            tuple(metadata_df.iloc[0].astype(str).to_list()),
        )
    trajectory_df = cruise.add_ID_trajectory_df(trajectory_df, cruise_name, server)
    data.data_df_to_db(
        trajectory_df, "tblCruise_Trajectory", server, clean_data_df_flag=False
    )
    metadata.ocean_region_classification_cruise(trajectory_df, cruise_name, db_name, server)


def insertData(data_dict, tableName, server):
    "Wrapper function for data.data_df_to_db"
    data.data_df_to_db(data_dict["data_df"], tableName, server)


def insertMetadata_no_data(
    data_dict, tableName, DOI_link_append, icon_filename, server, db_name, process_level
):
    """Main argparse wrapper function for inserting metadata for large datasets that do not have a single data sheet (ex. ARGO, sat etc.)

    Args:
        data_dict (dictionary): data dictionary containing data and metadata dataframes
        tableName (str): CMAP table name
        DOI_link_append (str): DOI link to append to tblDataset_References
        server (str): Valid CMAP server
        process_level (str): rep or nrt

    Returns:
        org_check_passed (bool): True if passed organism table checks
    """
    org_check_passed = dc.validate_organism_ingest(data_dict["variable_metadata_df"], server)
    contYN = ''
    if not org_check_passed:
        contYN = input(
        "Stop ingest to check organism data?  [yes/no]: "
        )
    if contYN != 'yes':
        metadata.tblDatasets_Insert(data_dict["dataset_metadata_df"], tableName, icon_filename, server, db_name)

        ## spaces in cruise field
        df_clean_cruise = cmn.strip_leading_trailing_whitespace_column(data_dict["dataset_metadata_df"],"cruise_names").replace('',np.nan,regex=True)
        if df_clean_cruise["cruise_names"].dropna().empty == False:
            metadata.tblMetadata_Cruises_Insert(data_dict["dataset_metadata_df"], db_name, server)    
        
        metadata.tblDataset_References_Insert(
            data_dict["dataset_metadata_df"], server, db_name, DOI_link_append
        )
        
        metadata.tblVariables_Insert(
            False,
            data_dict["dataset_metadata_df"],
            data_dict["variable_metadata_df"],
            tableName,
            server,
            db_name,
            process_level,
            CRS="CRS",
        )
        metadata.tblKeywords_Insert(
            data_dict["variable_metadata_df"],
            data_dict["dataset_metadata_df"],
            tableName,
            db_name,
            server,
        )
        # region id 114 is global
        metadata.ocean_region_insert(
            ["114"], data_dict["dataset_metadata_df"]["dataset_short_name"].iloc[0], db_name, server
        )

        
    else:
        print('insertMetadata_no_data stopped to check organism data')
    # if data_dict["dataset_metadata_df"]["cruise_names"].dropna().empty == False:
    #     metadata.tblDataset_Cruises_Insert(
    #         data_dict["data_df"], data_dict["dataset_metadata_df"], server
    #     )
    return org_check_passed


def insertMetadata(data_dict, tableName, DOI_link_append, server, db_name, process_level):
    """Wrapper function for metadata ingestion. Used for datasets that can fit in memory and can pass through the validator.

    Args:
        data_dict (dictionary): Input data dictionary containing all three sheets.
        tableName (str): CMAP table name
        DOI_link_append (str): DOI link to append to tblDataset_References
        server (str): Valid CMAP server
        process_level (str): rep or nrt
    """
    contYN = ''
    org_check_passed = dc.validate_organism_ingest(data_dict["variable_metadata_df"], server)
    if not org_check_passed:
        contYN = input(
        "Stop ingest to check organism data?  [yes/no]: "
        )
    if contYN != 'yes':
        metadata.tblDatasets_Insert(data_dict["dataset_metadata_df"], tableName, server, db_name)
        metadata.tblDataset_References_Insert(
            data_dict["dataset_metadata_df"], server, db_name, DOI_link_append
        )
        metadata.tblVariables_Insert(
            data_dict["data_df"],
            data_dict["dataset_metadata_df"],
            data_dict["variable_metadata_df"],
            tableName,
            server,
            db_name,
            process_level,
            CRS="CRS",
        )
        metadata.tblKeywords_Insert(
            data_dict["variable_metadata_df"],
            data_dict["dataset_metadata_df"],
            tableName,
            db_name,
            server,
        )
        metadata.ocean_region_classification(
            data_dict["data_df"],
            data_dict["dataset_metadata_df"]["dataset_short_name"].iloc[0],
            db_name,
            server,
        )
        if data_dict["dataset_metadata_df"]["cruise_names"].dropna().empty == False:
            metadata.tblDataset_Cruises_Insert(
                data_dict["data_df"], data_dict["dataset_metadata_df"], db_name, server
            )
    else:
        print('insertMetadata stopped to check organism data')
    return org_check_passed
    

def insert_small_stats(data_dict, tableName, db_name, server):
    """Wrapper function for stats.updateStats_Small"""
    stats.updateStats_Small(tableName, db_name, server, data_dict["data_df"])


def insert_large_stats(tableName, db_name, server):
    """Wrapper function for stats.build_stats_df_from_db_calls and stats.update_stats_large"""
    stats_df = stats.build_stats_df_from_db_calls(tableName, server)
    stats.update_stats_large(tableName, stats_df, db_name, server)


def createIcon(data_dict, tableName):
    """Wrapper function for mapping.folium_map"""
    mapping.folium_map(data_dict["data_df"], tableName)


def push_icon():
    """Pushes newly creation mission icon gto github"""
    os.chdir(vs.mission_icons)
    os.system('git add . && git commit -m "add mission icons to git repo" && git push')


def cruise_ingestion(args):
    """Main wrapper function for inserting cruise metadata and trajectory"""
    splitCruiseExcel(args.staging_filename, args.cruise_name)
    cruise_staging_to_vault(args.cruise_name, remove_file_flag=False)
    data_dict = import_cruise_data_dict(args.cruise_name)
    data_dict["metadata_df"] = add_ST_cols_cruise(
        data_dict["metadata_df"], data_dict["trajectory_df"]
    )

    insertCruise(
        data_dict["metadata_df"],
        data_dict["trajectory_df"],
        args.cruise_name,
        args.Database,
        args.Server,
    )


def full_ingestion(args):
    """Main argparse function for small dataset ingestion. Used for datasets that can fit in memory and can pass through the validator."""

    print("Full Ingestion")
    splitExcel(args.staging_filename, data_missing_flag=False)
    staging_to_vault(
        args.staging_filename,
        getBranch_Path(args),
        args.tableName,
        remove_file_flag=False,
        skip_data_flag=False,
        process_level=args.process_level,
    )
    data_dict = data.importDataMemory(
        args.branch, args.tableName, args.process_level, import_data=True
    )
    SQL_suggestion(data_dict, args.tableName, args.branch, args.Server, args.Database)
    insertData(data_dict, args.tableName, args.Server)
    org_check_pass = insertMetadata(
        data_dict, args.tableName, args.DOI_link_append, args.Server, args.Database, args.process_level
    )
    if org_check_pass:
        insert_small_stats(data_dict, args.tableName, args.Database, args.Server)
        if args.Server == "Rainier" and args.icon_filename is None:
            createIcon(data_dict, args.tableName)
            push_icon()


def dataless_ingestion(args):
    """This wrapper function adds metadata into the database for large datasets that already exist in the database. ex. satellite, model, argo etc."""
    splitExcel(args.staging_filename, data_missing_flag=True)
    staging_to_vault(
        args.staging_filename,
        getBranch_Path(args),
        args.tableName,
        remove_file_flag=False,
        skip_data_flag=True,
        process_level=args.process_level,
    )
    data_dict = data.importDataMemory(
        args.branch, args.tableName, args.process_level, import_data=False
    )
    org_check_passed = insertMetadata_no_data(
        data_dict, args.tableName, args.DOI_link_append, args.icon_filename, args.Server, args.Database, args.process_level
    )
   
    if org_check_passed:
        insert_large_stats(args.tableName, args.Database, args.Server)


def update_metadata(args):
    """This wrapper function deletes existing metadata, then adds updated metadata into the database"""
    ## Excel file name, 
    splitVaultExcel(args.staging_filename, args.branch, args.tableName, data_missing_flag=True)
    validator_to_vault(
        args.staging_filename,
        getBranch_Path(args),
        args.tableName
    )
    data_dict = data.importDataMemory(
        args.branch, args.tableName, args.process_level, import_data=False
    )
    metadata.deleteTableMetadata(args.tableName, args.Database, args.Server)
    org_check_pass = insertMetadata_no_data(
        data_dict, args.tableName, args.DOI_link_append, args.icon_filename, args.Server, args.Database, args.process_level
    )
    if org_check_pass:
        insert_large_stats(args.tableName, args.Database, args.Server)
    #     transfer.df_to_parquet(data_dict["variable_metadata_df"],'variable_metadata',args.branch, args.tableName,'metadata')
    #     transfer.df_to_parquet(data_dict["dataset_metadata_df"],'dataset_metadata',args.branch, args.tableName,'metadata')


def main():
    """Main function that parses arguments and determines which data ingestion path depending on args"""
    parser = argparse.ArgumentParser(description="Ingestion datasets into CMAP")

    parser.add_argument(
        "tableName",
        type=str,
        help="Desired SQL and Vault Table Name. Ex: tblSeaFlow",
        nargs="?",
    )
    parser.add_argument(
        "branch",
        type=str,
        help="Branch where dataset should be placed in Vault. Ex's: cruise, float, station, satellite, model, assimilation.",
        nargs="?",
    )
    parser.add_argument(
        "staging_filename",
        type=str,
        help="Filename from staging area. Ex: 'SeaFlow_ScientificData_2019-09-18.csv'",
    )
    parser.add_argument("-p", "--process_level", nargs="?", default="rep")
    parser.add_argument(
        "-d",
        "--DOI_link_append",
        help="DOI string to append to reference_list",
        nargs="?",
    )

    parser.add_argument("-N", "--Dataless_Ingestion", nargs="?", const=True)
    parser.add_argument("-U", "--Update_Metadata", nargs="?", const=True)
    parser.add_argument("-C", "--cruise_name", help="UNOLS Name", nargs="?")
    parser.add_argument(
        "-S", "--Server", help="Server choice: Rainier, Mariana", nargs="?"
    )
    parser.add_argument(
        "-D", "--Database", help="Database name: Opedia, Opedia_Sandbox", nargs="?", default="Opedia"
    )
    parser.add_argument(
        "-i", 
        "--icon_filename", 
        help="Filename for icon in Github instead of creating a map thumbnail of data. Ex: argo_small.jpg", 
        nargs="?",
        default=""
    )
    args = parser.parse_args()

    if args.cruise_name:
        cruise_ingestion(args)

    elif args.Dataless_Ingestion:
        dataless_ingestion(args)
    
    elif args.Update_Metadata:
        update_metadata(args)

    else:
        full_ingestion(args)


if __name__ == main():
    main()
