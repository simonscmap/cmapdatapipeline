"""
Author: Norland Raphael Hagen <norlandrhagen@gmail.com>
Date: 07-23-2021

cmapdata - SQL - SQL table/index suggestions and formatting
"""

import sys
import pandas as pd
import numpy as np

import credentials as cr
import common as cmn
import DB

######## Python Specific ############


def write_SQL_file(sql_str, tableName, make="observation"):
    """Takes SQL sugested string and builds a .sql record file.

    Args:
        sql_str (str): SQL string for table creation
        tableName (str): Valid CMAP table name
        make (str, optional): observation, model, assimilation. Defaults to "observation".
    """
    with open(
        f"""../../DB/mssql/build/tables/{make}/{tableName}.sql""", "w"
    ) as sql_file:
        print(sql_str, file=sql_file)


def build_SQL_suggestion_df(df):
    """Builds a dataframe of colulmn name and datatype for an input data specific dataframe"""
    sug_df = pd.DataFrame(columns=["column_name", "dtype", "max_len"])
    exclude_list = [1, "", " ", np.nan, "nan", "NaN", "NAN"]
    max_len = None
    for cn in list(df):
        if cn == "time":
            col_dtype = "datetime"
        else:
            col_clean = cmn.exclude_val_from_col(df[cn], exclude_list)
            try:
                col_convert = pd.to_numeric(col_clean)
            except:
                col_convert = col_clean
                ## Get longest string length
                # max_len = df[cn].map(len).max()
                max_len = df[df[cn].notna()][cn].str.len().max()
            col_dtype = col_convert.dtype
        sug_list = [cn, col_dtype, max_len]
        sug_df.loc[len(sug_df)] = sug_list
    return sug_df


def SQL_index_suggestion_formatter(
    data_df, tableName, server, db_name, FG="Primary"
):
    """Creates SQL index suggestion from input data DF"""
    if "depth" in cmn.lowercase_List(list(data_df)):
        uniqueTF = data_df.duplicated(subset=["time", "lat", "lon", "depth"]).any()
        depth_str = "_depth"
        depth_flag_comma = ","
        depth_flag_lp = "[depth"
        depth_flag_rp = "]"
    else:
        uniqueTF = data_df.duplicated(subset=["time", "lat", "lon"]).any()
        depth_str = ""
        depth_flag_comma = ""
        depth_flag_lp = ""
        depth_flag_rp = ""

    if uniqueTF == True:
        UNIQUE_flag = "NON"
    else:
        UNIQUE_flag = "UNIQUE "
    # if any are True, there are duplicates in subset
    SQL_index_str = f"""
    USE [{db_name}]


    CREATE {UNIQUE_flag}CLUSTERED INDEX [IX_{tableName}_time_lat_lon{depth_str}] ON [dbo].[{tableName}]
    (
    	[time] ASC,
    	[lat] ASC,
    	[lon] ASC{depth_flag_comma}
    	{depth_flag_lp}{depth_flag_rp}
    )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [{FG}]"""

    SQL_index_dir = {"sql_index": SQL_index_str}
    return SQL_index_dir


def SQL_tbl_suggestion_formatter(sdf, tableName, server, db_name, FG="Primary"):
    """Creates SQL table suggestion from input dataframe. Default FG is Primary"""

    sdf["null_status"] = "NULL,"
    sdf.loc[sdf["column_name"] == "time", "dtype"] = "[datetime]"
    sdf.loc[sdf["column_name"] == "time", "null_status"] = "NOT NULL,"
    sdf.loc[sdf["column_name"] == "lat", "null_status"] = "NOT NULL,"
    sdf.loc[sdf["column_name"] == "lon", "null_status"] = "NOT NULL,"
    if "depth" in list(sdf["column_name"]):
        sdf.loc[sdf["column_name"] == "depth", "null_status"] = "NOT NULL,"
    sdf["null_status"].iloc[-1] = sdf["null_status"].iloc[-1].replace(",", "")
    sdf["column_name"] = "[" + sdf["column_name"].astype(str) + "]"
    ## was 200, fixed to be generated from df
    sdf["max_len"] = "[nvarchar]("+ sdf["max_len"].astype(str) + ")"
    
    # sdf["max_len"] = "[nvarchar](max)"
    
    sdf.loc[sdf["dtype"] == "object", "dtype"] = sdf.loc[sdf["dtype"] == "object", "max_len"]
    sdf["dtype"] = sdf["dtype"].replace("float64", "[float]")
    sdf["dtype"] = sdf["dtype"].replace("int64", "[bigint]")
    sdf = sdf.drop('max_len', axis=1)
    var_string = sdf.to_string(header=False, index=False)

    """ Print table as SQL format """
    SQL_tbl = f"""
    USE [{db_name}]

    SET ANSI_NULLS ON


    SET QUOTED_IDENTIFIER ON


    CREATE TABLE [dbo].[{tableName}](

    {var_string}


    ) ON [{FG}]"""

    sql_dict = {"sql_tbl": SQL_tbl}
    return sql_dict

def full_SQL_suggestion_build(df, tableName, branch, server, db_name):
    """Creates suggested SQL table based on data types of input dataframe.

    Args:
        df (dataframe): Pandas dataframe to build table from 
        tableName (str): CMAP table name
        branch (str): vault branch path, ex float.
        server (str): Valid CMAP server
    """
    if branch != "model" or branch != "satellite":
        make = "observation"
    else:
        make = branch
    cdt = build_SQL_suggestion_df(df)
    fg_input = input("Filegroup to use (ie FG3, FG4):")
    sql_tbl = SQL_tbl_suggestion_formatter(cdt, tableName, server, db_name, fg_input)
    sql_index = SQL_index_suggestion_formatter(
        df, tableName, server, db_name, fg_input
    )
    sql_combined_str = sql_tbl["sql_tbl"] + sql_index["sql_index"]
    print(sql_combined_str)
    contYN = input("Do you want to build this table in SQL? " + " ?  [yes/no]: ")
    if contYN.lower() == "yes":
        DB.DB_modify(sql_tbl["sql_tbl"], server)
        DB.DB_modify(sql_index["sql_index"], server)

    else:
        sys.exit()
    write_SQL_file(sql_combined_str, tableName, make)