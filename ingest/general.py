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


def getBranch_Path(args):
    branch_path = cmn.vault_struct_retrieval(args.branch)
    return branch_path


def splitExcel(staging_filename, data_missing_flag):
    transfer.single_file_split(staging_filename, data_missing_flag)

def splitCruiseExcel(staging_filename):
    transfer.cruise_file_split(staging_filename)


def staging_to_vault(
    staging_filename, branch, tableName, remove_file_flag, skip_data_flag, process_level
):
    transfer.staging_to_vault(
        staging_filename,
        branch,
        tableName,
        remove_file_flag,
        skip_data_flag,
        process_level,
    )
def cruise_staging_to_vault(
    staging_filename, cruise_name, remove_file_flag
):
    transfer.cruise_staging_to_vault(staging_filename, cruise_name, remove_file_flag)



def importDataMemory(branch, tableName, process_level):
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


def SQL_suggestion(data_dict, tableName, branch, server):
    if branch != "model" or branch != "satellite":
        make = "observation"
    else:
        make = branch
    cdt = SQL.build_SQL_suggestion_df(data_dict["data_df"])
    sql_tbl = SQL.SQL_tbl_suggestion_formatter(cdt, tableName)
    sql_index = SQL.SQL_index_suggestion_formatter(data_dict["data_df"], tableName)
    sql_combined_str = sql_tbl["sql_tbl"] + sql_index["sql_index"]
    print(sql_combined_str)
    contYN = input("Do you want to build this table in SQL? " + " ?  [yes/no]: ")
    if contYN.lower() == "yes":
        DB.DB_modify(sql_tbl["sql_tbl"], server)
        DB.DB_modify(sql_index["sql_index"], server)

    else:
        sys.exit()
    SQL.write_SQL_file(sql_combined_str, tableName, make)


def insertData(data_dict, tableName, server):
    data.data_df_to_db(data_dict["data_df"], tableName, server)


def insertMetadata_no_data(
    data_dict, tableName, DOI_link_append, server, process_level
):
    metadata.tblDatasets_Insert(data_dict["dataset_metadata_df"], tableName, server)
    metadata.tblDataset_References_Insert(
        data_dict["dataset_metadata_df"], server, DOI_link_append
    )

    metadata.tblVariables_Insert(
        False,
        data_dict["dataset_metadata_df"],
        data_dict["variable_metadata_df"],
        tableName,
        server,
        process_level,
        CRS="CRS",
    )
    metadata.tblKeywords_Insert(
        data_dict["variable_metadata_df"],
        data_dict["dataset_metadata_df"],
        tableName,
        server,
    )

    # region id 114 is global
    metadata.ocean_region_insert(
        ["114"], data_dict["dataset_metadata_df"]["dataset_short_name"].iloc[0], server
    )

    # if data_dict["dataset_metadata_df"]["cruise_names"].dropna().empty == False:
    #     metadata.tblDataset_Cruises_Insert(
    #         data_dict["data_df"], data_dict["dataset_metadata_df"], server
    #     )


def insertMetadata(data_dict, tableName, DOI_link_append, server, process_level):
    metadata.tblDatasets_Insert(data_dict["dataset_metadata_df"], tableName, server)
    metadata.tblDataset_References_Insert(
        data_dict["dataset_metadata_df"], server, DOI_link_append
    )
    metadata.tblVariables_Insert(
        data_dict["data_df"],
        data_dict["dataset_metadata_df"],
        data_dict["variable_metadata_df"],
        tableName,
        server,
        process_level="REP",
        CRS="CRS",
    )
    metadata.tblKeywords_Insert(
        data_dict["variable_metadata_df"],
        data_dict["dataset_metadata_df"],
        tableName,
        server,
    )
    metadata.ocean_region_classification(
        data_dict["data_df"],
        data_dict["dataset_metadata_df"]["dataset_short_name"].iloc[0],
        server,
    )
    if data_dict["dataset_metadata_df"]["cruise_names"].dropna().empty == False:
        metadata.tblDataset_Cruises_Insert(
            data_dict["data_df"], data_dict["dataset_metadata_df"], server
        )


def insert_small_stats(data_dict, tableName, server):
    stats.updateStats_Small(tableName, server, data_dict["data_df"])


def insert_large_stats(tableName, server):
    stats_df = stats.build_stats_df_from_db_calls(tableName, server)
    stats.update_stats_large(tableName, stats_df, server)


def createIcon(data_dict, tableName):
    mapping.folium_map(data_dict["data_df"], tableName)


def push_icon():
    os.chdir(vs.mission_icons)
    os.system('git add . && git commit -m "add mission icons to git repo" && git push')


def cruise_ingestion(args):
    splitCruiseExcel(args.staging_filename)
    cruise_staging_to_vault
    """need: 
    *path to cruise template
    *split cruise template and put somewhere
    load cruise metadata and trajectory
    insert into table cruise
    insert into table cruise metadata
    update table cruise with spatial bounds
    classify cruise region
    insert into tblCruise_Regions


    """


def full_ingestion(args):
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
    SQL_suggestion(data_dict, args.tableName, args.branch, args.Server)
    insertData(data_dict, args.tableName, args.Server)
    insertMetadata(
        data_dict, args.tableName, args.DOI_link_append, args.Server, args.process_level
    )
    insert_small_stats(data_dict, args.tableName, args.Server)
    if args.Server == "Rainier":
        createIcon(data_dict, args.tableName)
        push_icon()


def dataless_ingestion(args):
    """This wrapper function adds metadata into the database for large datasets that already exist in the database. ex. satellite, model, argo etc.


    Args:
        args (): Arguments from input argparse

    """
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
    insertMetadata_no_data(
        data_dict, args.tableName, args.DOI_link_append, args.Server, args.process_level
    )
    insert_large_stats(args.tableName, args.Server)


def main():
    parser = argparse.ArgumentParser(description="Ingestion datasets into CMAP")

    parser.add_argument(
        "tableName", type=str, help="Desired SQL and Vault Table Name. Ex: tblSeaFlow"
    )
    parser.add_argument(
        "branch",
        type=str,
        help="Branch where dataset should be placed in Vault. Ex's: cruise, float, station, satellite, model, assimilation.",
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

    parser.add_argument(
        "-S",
        "--Server",
        help="Server choice: Rainier, Mariana, Beast",
        nargs="?",
        default="Rainier",
    )

    args = parser.parse_args()

    if args.Cruise_Ingestion:
        cruise_ingestion():


    elif args.Dataless_Ingestion:
        dataless_ingestion(args)

    else:
        full_ingestion(args)


if __name__ == main():
    main()
