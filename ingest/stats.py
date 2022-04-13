"""
Author: Norland Raphael Hagen <norlandrhagen@gmail.com>
Date: 07-23-2021

cmapdata - stats - cmap summary stats functionallity. 
"""


import os
from tqdm import tqdm
import dask.dataframe as dd
import pandas as pd
import numpy as np

import credentials as cr
import common as cmn
import DB
import transfer
import data


def updateStatsTable(ID, json_str, server):
    """Updates entry for tblDataset_Stats

    Args:
        ID (str): Dataset_ID
        json_str (str): Formatted JSON string for tblDataset_Stats
        Server (str): Valid CMAP server name
    """
    conn, cursor = DB.dbConnect(server)
    deleteQuery = f"""DELETE FROM tblDataset_Stats WHERE Dataset_ID = '{ID}'"""
    insertQuery = f"""INSERT INTO tblDataset_Stats (Dataset_ID, JSON_stats) VALUES('{ID}','{json_str}')"""
    try:
        DB.DB_modify(deleteQuery, server)
        DB.DB_modify(insertQuery, server)
    except Exception as e:
        print(e)


def updateStats_Small(tableName, db_name, server, data_df=None):
    """Updates entry for tblDataset_Stats, wraps around updateStatsTable, but with common inputs.

    Args:
        tableName (str): CMAP table name
        db_name (str): CMAP database name (Opedia)
        Server (str): Valid CMAP server name
        data_df (Pandas DataFrame, optional): datframe to build stats from, if not provided table is queried from database. Defaults to None.
    """

    if data_df is not None:
        data_df = data_df
    else:
        query = "SELECT * FROM {tableName}".format(tableName=tableName)
        data_df = DB.dbRead(query, server)
    Dataset_ID = cmn.getDatasetID_Tbl_Name(tableName, db_name, server)
    stats_df = data_df.describe()
    if "_Climatology" in tableName:
        return

    min_max_df = pd.DataFrame(
        {"time": [data_df["time"].min(), data_df["time"].max()]}, index=["min", "max"]
    )
    df = pd.concat([min_max_df, stats_df], axis=1, sort=True)
    if 'datetime' in df.time.dtype.name:
        df['time'] =  df['time'].dt.date
    else:
        df['time'] =  df['time'].astype('datetime64[ns]').dt.date
    json_str = df.to_json(date_format="iso")
    sql_df = pd.DataFrame({"Dataset_ID": [Dataset_ID], "JSON": [json_str]})
    updateStatsTable(Dataset_ID, json_str, server)
    print("Updated stats for " + tableName)


def buildLarge_Stats(df, datetime_slice, tableName, branch, transfer_flag="dropbox"):
    """Calculates summary stats for a dataframe. Can be used when calculating stats for large dataset ingestion. Alternative is build_stats_df_from_db_calls().

    Args:
        df (Pandas DataFrame): Input cleaned dataframe
        datetime_slice (str): time slice of input data. Ex. daily sat would be daily.
        tableName (str): CMAP table name
        branch (str): vault branch path, ex float.
        transfer_flag (str, optional): Way to transfer data, either dropbox or sshfs. Defaults to "dropbox".
    """
    """Input is dataframe slice (daily, 8 day, monthly etc.) of a dataset that is split into multiple files"""
    
    df_stats = df.describe()
    if "_Climatology" not in tableName:
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
    """Aggregates stats of stats files for large dataset ingestion. Alternative is db calls using build_stats_df_from_db_calls()

    Args:
        branch (str): vault branch path, ex float.
        tableName (str): CMAP table name
        Server (str): Valid CMAP server name

    Returns:
        Pandas DataFrame: stats dataframe
    """
    """aggregates summary stats files and computes stats of stats. Returns stats dataframe"""
    branch_path = cmn.vault_struct_retrieval(branch)
    df = dd.read_csv(branch_path + tableName + "/stats/" + "*.csv*")
    df = df.compute().set_index(df.columns[0])
    df.index.name = "Stats"
    st_cols = data.ST_columns(df)
    ## ST_columns hard codes column names and order. if climatology, replace 'time' with 'month'
    if "_Climatology" in tableName:
        st_cols[0] = "month"
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


def build_stats_df_from_db_calls(tableName, server):
    """

    Builds basic (min,max,count) summary stats from existing table in DB

    Args:
        tableName (str): CMAP table name
        Server (str): Valid CMAP server name

    Returns:
        Pandas DataFrame: stats dataframe
    """
    if "_Climatology" in tableName:
        col_list = cmn.get_numeric_cols_in_table_excluding_climatology(
        tableName, server
        )
    else:
        col_list =  ["time"] + cmn.get_numeric_cols_in_table_excluding_climatology(
        tableName, server
        ) 
    stats_DF = pd.DataFrame(index=["count", "max", "mean", "min", "std"])
    for var in tqdm(col_list):
        print(var)
        stats_DF[var] = ""
        if var == 'time':
            stats_qry = (
                f"""SELECT count_big({var}),MAX(cast({var} as date)),MIN(cast({var} as date)),'','' FROM {tableName}"""
            )     
            stats_qry = (
                f"""SELECT count_big({var}),cast(MAX(cast({var} as date)) as nvarchar(10)),cast(MIN(cast({var} as date)) as nvarchar(10)),'','' FROM {tableName}"""
            )         
            var_df = DB.dbRead(stats_qry, server)    
        else:
            try:
                stats_qry = (
                    f"""SELECT count_big({var}),MAX({var}),MIN({var}),AVG({var}),STDEV({var}) FROM {tableName}"""
                )
                var_df = DB.dbRead(stats_qry, server)
            except:
                stats_qry = f"""SELECT count_big({var}),MAX(cast({var} as numeric(12, 0))),MIN(cast({var} as numeric(12, 0))),AVG(cast({var} as numeric(12, 0))),STDEV(cast({var} as numeric(12, 0))) FROM {tableName}"""
                var_df = DB.dbRead(stats_qry, server)            
        if (var == 'lat' or var == 'lon'):
            stats_DF.at["mean", var] = ""
            stats_DF.at["std", var] = ""
        else:
            stats_DF.at["mean", var] = var_df.iloc[:, 3][0]
            stats_DF.at["std", var] = var_df.iloc[:, 4][0]
        stats_DF.at["count", var] = var_df.iloc[:, 0][0] 
        stats_DF.at["max", var] = var_df.iloc[:, 1][0]
        stats_DF.at["min", var] = var_df.iloc[:, 2][0]

    return stats_DF


def update_stats_large(tableName, stats_df, db_name, server):
    """Updates tblDataset_Stats entry with new stats

    Args:
        tableName (str): CMAP table name
        stats_df (Pandas DataFrame): Input stats dataframe
        Server (str): Valid CMAP server name
    """
    Dataset_ID = cmn.getDatasetID_Tbl_Name(tableName, db_name, server)
    json_str = stats_df.to_json(date_format="iso")
    sql_df = pd.DataFrame({"Dataset_ID": [Dataset_ID], "JSON": [json_str]})
    updateStatsTable(Dataset_ID, json_str, server)
