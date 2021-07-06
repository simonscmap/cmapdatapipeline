import sys
import os
import glob
import pandas as pd
import numpy as np

sys.path.append("../login/")
import credentials as cr

import pycmap

pycmap.API(cr.api_key)
import argparse


from cmapingest import vault_structure as vs
from cmapingest import transfer
from cmapingest import data
from cmapingest import DB
from cmapingest import metadata
from cmapingest import SQL
from cmapingest import mapping
from cmapingest import stats
from cmapingest import common as cmn


def getBranch_Path(args):
    branch_path = cmn.vault_struct_retrieval(args.branch)
    return branch_path


def splitExcel(staging_filename, metadata_filename):
    transfer.single_file_split(staging_filename, metadata_filename)


def staging_to_vault(staging_filename, branch, tableName, remove_file_flag=False):
    transfer.staging_to_vault(staging_filename, branch, tableName, remove_file_flag)


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


def insertMetadata(data_dict, tableName, DOI_link_append, server):
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


###   TESTING SUITE   ###
# ⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️ #
# ⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️ #
# ⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️ #
#########################


def insert_small_stats(data_dict, tableName, server):
    stats.updateStats_Small(tableName, server, data_dict["data_df"])


def insert_large_stats(tableName, branch, server):
    stats_df = stats.aggregate_large_stats(branch, tableName)
    stats.update_stats_large(tableName, stats_df, server)


def createIcon(data_dict, tableName):
    mapping.folium_map(data_dict["data_df"], tableName)


def push_icon():
    os.chdir(vs.mission_icons)
    os.system('git add . && git commit -m "add mission icons to git repo" && git push')


def full_ingestion(args):
    print("Full Ingestion")
    splitExcel(args.staging_filename, args.metadata_filename)
    staging_to_vault(
        args.staging_filename,
        getBranch_Path(args),
        args.tableName,
        remove_file_flag=True,
    )
    data_dict = data.importDataMemory(args.branch, args.tableName, args.process_level)
    SQL_suggestion(data_dict, args.tableName, args.branch, args.Server)
    insertData(data_dict, args.tableName, args.Server)
    insertMetadata(data_dict, args.tableName, args.DOI_link_append, args.Server)
    insert_small_stats(data_dict, args.tableName, args.Server)
    if args.Server == "Rainier":
        createIcon(data_dict, args.tableName)
        push_icon()


def partial_ingestion(args):
    print("Partial Ingestion")
    pass


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
        "-m", "--metadata_filename", nargs="?",
    )
    parser.add_argument(
        "-d",
        "--DOI_link_append",
        help="DOI string to append to reference_list",
        nargs="?",
    )
    parser.add_argument("-P", "--Partial_Ingestion", nargs="?", const=True)

    parser.add_argument("-A", "--Append_Ingestion", nargs="?", const=True)

    parser.add_argument(
        "-S",
        "--Server",
        help="Server choice: Rainier, Mariana, Beast",
        nargs="?",
        default="Rainier",
    )

    args = parser.parse_args()

    if args.Partial_Ingestion:
        partial_ingestion(args)

    elif args.Append_Ingestion:
        append_ingestion(args)
    else:
        full_ingestion(args)


if __name__ == main():
    main()
