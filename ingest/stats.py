import sys
import os
import credentials as cr

import pandas as pd
import numpy as np
import pycmap

import common as cmn
import DB
import transfer
import data


import dask.dataframe as dd

pycmap.API(cr.api_key)

api = pycmap.API()


def updateStatsTable(ID, json_str, server):
    conn, cursor = DB.dbConnect(server)
    deleteQuery = """DELETE FROM tblDataset_Stats WHERE Dataset_ID = '{}'""".format(ID)
    insertQuery = """INSERT INTO tblDataset_Stats (Dataset_ID, JSON_stats) VALUES('{}','{}')""".format(
        ID, json_str
    )
    try:
        DB.DB_modify(deleteQuery, server)
        DB.DB_modify(insertQuery, server)

    except Exception as e:
        print(e)


def updateStats_Small(tableName, server, data_df=None):
    if data_df is not None:
        data_df = data_df
    else:
        query = "SELECT * FROM {tableName}".format(tableName=tableName)
        data_df = DB.dbRead(query, server)
    Dataset_ID = cmn.getDatasetID_Tbl_Name(tableName, server)
    stats_df = data_df.describe()
    min_max_df = pd.DataFrame(
        {"time": [data_df["time"].min(), data_df["time"].max()]}, index=["min", "max"]
    )
    df = pd.concat([stats_df, min_max_df], axis=1, sort=True)
    json_str = df.to_json(date_format="iso")
    sql_df = pd.DataFrame({"Dataset_ID": [Dataset_ID], "JSON": [json_str]})
    updateStatsTable(Dataset_ID, json_str, server)
    print("Updated stats for " + tableName)


def retrieve_stats_from_DB(tableName, server):
    st_cols = data.ST_columns(df)

    qry = f"""SELECT MIN(time),MAX(time),MIN(lat),MAX(lat),MIN(lon),MAX(depth),"""


def buildLarge_Stats(df, datetime_slice, tableName, branch, transfer_flag="dropbox"):
    """Input is dataframe slice (daily, 8 day, monthly etc.) of a dataset that is split into multiple files"""
    df_stats = df.describe()
    df_stats.insert(loc=0, column="time", value="")
    df_stats.at["count", "time"] = len(df["time"])
    df_stats.at["min", "time"] = min(df["time"])
    df_stats.at["max", "time"] = max(df["time"])
    branch_path = cmn.vault_struct_retrieval(branch)

    if transfer_flag == "dropbox":
        df_stats.to_csv("stats_temp.csv")
        transfer.dropbox_file_transfer(
            "stats_temp.csv",
            branch_path.split("Vault")[1]
            + tableName
            + "/stats/"
            + datetime_slice
            + "_summary_stats.csv",
        )
        os.remove("stats_temp.csv")
    else:
        df_stats.to_csv(
            branch_path + tableName + "/stats/" + datetime_slice + "_summary_stats.csv"
        )
    print(
        "stats built for :"
        + tableName
        + "  in branch: "
        + branch
        + " for date: "
        + datetime_slice
    )


def aggregate_large_stats(branch, tableName, server):
    """aggregates summary stats files and computes stats of stats. Returns stats dataframe"""
    branch_path = cmn.vault_struct_retrieval(branch)
    df = dd.read_csv(branch_path + tableName + "/stats/" + "*.csv*")
    df = df.compute().set_index(df.columns[0])
    df.index.name = "Stats"
    st_cols = data.ST_columns(df)
    var_list = list(set(list(df)) - set(st_cols))
    var_max_list, var_min_list, var_mean_list, var_std_list = [], [], [], []
    max_var_count_list = [cmn.getLatCount(tableName, server)] * len(list(df))
    for var in list(df):  # var_list:
        var_max = max(df.loc["max", var])
        var_max_list.append(var_max)
        var_min = min(df.loc["min", var])
        var_min_list.append(var_min)
        var_mean = np.mean(df.loc["mean", var])
        var_mean_list.append(var_mean)
        var_std = np.std(df.loc["std", var])
        var_std_list.append(var_std)
    stats_DF = pd.DataFrame(index=["count", "max", "mean", "min", "std"])

    for var, max_var_count, var_max, var_min, var_mean, var_std in zip(
        list(df),
        max_var_count_list,
        var_max_list,
        var_min_list,
        var_mean_list,
        var_std_list,
    ):
        stats_DF[var] = ""  # create empty column to fill
        stats_DF.at["count", var] = max_var_count
        stats_DF.at["max", var] = var_max
        stats_DF.at["mean", var] = var_mean
        stats_DF.at["min", var] = var_min
        stats_DF.at["std", var] = var_std
    return stats_DF


def update_stats_large(tableName, stats_df, server):
    Dataset_ID = cmn.getDatasetID_Tbl_Name(tableName)
    json_str = stats_df.to_json(date_format="iso")
    sql_df = pd.DataFrame({"Dataset_ID": [Dataset_ID], "JSON": [json_str]})
    updateStatsTable(Dataset_ID, json_str, server)
