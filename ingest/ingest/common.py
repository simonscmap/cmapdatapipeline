import sys
import os
import pandas as pd
import numpy as np
from cmapingest import DB
from cmapingest import vault_structure as vs
import pycmap


def normalize(vals, min_max=False):
    """Takes an array and either normalize to min/max, standardize it (remove the mean and divide by standard deviation)."""
    if min_max:
        normalized_vals = (vals - np.nanmin(vals)) / (np.nanmax(vals) - np.nanmin(vals))
    else:
        normalized_vals = (vals - np.nanmean(vals)) / np.nanstd(vals)
    return normalized_vals


def strip_whitespace_data(df):
    """Strips any whitespace from data"""
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    return df


def strip_whitespace_headers(df):
    """Strips any whitespace from dataframe headers"""
    df.columns = df.columns.str.strip()
    return df


def strip_leading_trailing_whitespace_column(df, col_name):
    df[col_name] = df[col_name].str.lstrip()
    df[col_name] = df[col_name].str.rstrip()
    return df


def nanToNA(df):
    """Replaces and numpy nans with ''"""
    df = df.replace(np.nan, "", regex=True)
    return df


def lowercase_List(list):
    """Converts every string in a list to lowercase string"""
    lower_list = [x.lower() for x in list]
    return lower_list


def getColBounds(df, col, list_multiplier="0"):
    """Gets the min and max bounds of a dataframe column

    Parameters
    ----------
    df : Pandas DataFrame
        Input DataFrame
    col : str
        Name of column
    list_multiplier: int, optional, default = 0
        Output is a a list of returned values with length of length list_multiplier integer


    Returns
    -------

    min_col_list, max_col_list
        returns two lists of column mins and maxes

    """
    if col == "time":
        min_col = [
            pd.to_datetime(df["time"], errors="coerce")
            .min()
            .strftime("%Y-%m-%d %H:%M:%S")
        ]
        max_col = [
            pd.to_datetime(df["time"], errors="coerce")
            .max()
            .strftime("%Y-%m-%d %H:%M:%S")
        ]
    else:
        min_col = [int(pd.to_numeric(df[col]).min())]
        max_col = [int(pd.to_numeric(df[col]).max())]
    if list_multiplier != "0":
        min_col = min_col * int(list_multiplier)
        max_col = max_col * int(list_multiplier)

    return min_col, max_col


def vault_struct_retrieval(branch):
    """Returns vault structure path for input branch"""
    if branch.lower() == "cruise":
        vs_struct = vs.cruise
    elif branch.lower() == "float":
        vs_struct = vs.float_dir
    elif branch.lower() == "station":
        vs_struct = vs.station
    elif branch.lower() == "satellite":
        vs_struct = vs.satellite
    elif branch.lower() == "model":
        vs_struct = vs.model
    elif branch.lower() == "assimilation":
        vs_struct = vs.assimilation
    else:
        print(
            "Vault branch structure not found in vault_structure.py. Please modify that script."
        )
    return vs_struct


def get_name_pkey(tableName, server):
    """Returns the name of the primary key column for a given table

    Args:
        tableName (string): CMAP table name
        Server (string): Valid CMAP server name
    """
    query = f"""
    SELECT Col.Column_Name from 
        INFORMATION_SCHEMA.TABLE_CONSTRAINTS Tab, 
        INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE Col 
    WHERE 
        Col.Constraint_Name = Tab.Constraint_Name
        AND Col.Table_Name = Tab.Table_Name
        AND Constraint_Type = 'PRIMARY KEY'
        AND Col.Table_Name = '{tableName}'"""
    pkey_name = DB.dbRead(query, server).iloc[0][0]
    return pkey_name


def get_last_ID(tableName, server):
    pkey_col_name = get_name_pkey(tableName, server)
    last_ID_qry = f"""SELECT TOP 1 * FROM {tableName} ORDER BY {pkey_col_name} DESC"""
    last_ID = DB.dbRead(last_ID_qry, server).iloc[0][0]
    return last_ID


def getDatasetID_DS_Name(datasetName, server):
    """Get DatasetID from input dataset name"""
    cur_str = (
        """select [ID] FROM [Opedia].[dbo].[tblDatasets] WHERE [Dataset_Name] = '"""
        + datasetName
        + """'"""
    )
    query_return = DB.dbRead(cur_str, server)
    # query_return = DB.DB_query(cur_str)
    dsID = query_return.iloc[0][0]
    return dsID


def getDatasetID_Tbl_Name(tableName, server):
    """Get DatasetID from input table name"""
    cur_str = (
        """select distinct [Dataset_ID] FROM [Opedia].[dbo].[tblVariables] WHERE [Table_Name] = '"""
        + tableName
        + """'"""
    )
    query_return = DB.dbRead(cur_str, server=server)

    dsID = query_return.iloc[0][0]
    return dsID


def getKeywordIDsTableNameVarName(tableName, var_short_name_list, server):
    """Get list of keyword ID's from input dataset ID"""
    vsnp = tuple(var_short_name_list)
    cur_str = f"""select [ID] from tblVariables where Table_Name = '{tableName}' AND [Short_Name] in {vsnp}"""
    if len(var_short_name_list) == 1:
        cur_str = cur_str.replace(",)", ")")

    query_return = DB.dbRead(cur_str, server=server)["ID"].to_list()
    return query_return


def getKeywordsIDDataset(dataset_ID, server):
    """Get list of keyword ID's from input dataset ID"""
    dataset_ID = str(dataset_ID)
    cur_str = f"""select [ID] from tblVariables where Dataset_ID = '{dataset_ID}'"""
    query_return = DB.dbRead(cur_str, server=server)["ID"].to_list()
    return query_return


def getTableName_Dtypes(tableName, server):
    """Get data types from input table name"""
    query = (
        """ select COLUMN_NAME, DATA_TYPE from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = '"""
        + tableName
        + """'"""
    )
    # query_return = DB.DB_query(query)
    query_return = DB.dbRead(query, server)

    return query_return


def getCruiseDetails(cruiseName, server):
    """Get cruise details from cruise name using uspCruiseByName"""
    query = """EXEC uspCruiseByName '""" + cruiseName + """'"""
    query_return = DB.dbRead(query, server)
    return query_return


def getListCruises(server):
    """Get list of available cruises using uspCruises"""
    query = """SELECT * FROM tblCruise"""
    query_return = DB.dbRead(query, server=server)
    return query_return


def findVarID(datasetID, Short_Name, server):
    """Get ID value from tblVariables for specific variable"""
    cur_str = (
        """select [ID] FROM [Opedia].[dbo].[tblVariables] WHERE [Dataset_ID] = '"""
        + str(datasetID)
        + """' AND [Short_Name] = '"""
        + Short_Name
        + """'"""
    )
    query = DB.dbRead(cur_str, server)
    print(query)
    VarID = query.iloc[0][0]
    return VarID


def find_File_Path_guess_tree(name):
    """Attempts to return vault structure path for input filename"""
    for root, dirs, files in os.walk(vs.vault):
        if name in files:
            fpath = os.path.join(root, name)
            if "cruise" in fpath:
                struct = vs.cruise
            elif "float" in fpath:
                struct = vs.float_dir
            elif "station" in fpath:
                struct = vs.station
            elif "satellite" in fpath:
                struct = vs.satellite
            elif "model" in fpath:
                struct = vs.model
            elif "assimilation" in fpath:
                struct = vs.assimilation
            else:
                struct = "File path not found"
            return struct


def verify_cruise_lists(dataset_metadata_df, server):
    """Returns matching and non matching cruises"""
    cruise_series = strip_leading_trailing_whitespace_column(
        dataset_metadata_df, "cruise_names"
    )["cruise_names"]
    """ check that every cruise_name in column exists in the database. map those that don't exist into return"""
    cruise_set = set(lowercase_List(cruise_series.to_list()))
    nickname_list = lowercase_List(getListCruises(server)["Nickname"].to_list())
    name_list = lowercase_List(getListCruises(server)["Name"].to_list())
    db_cruise_set = set(nickname_list + name_list)
    matched = list(cruise_set.intersection(db_cruise_set))
    unmatched = list(cruise_set.difference(db_cruise_set))
    return matched, unmatched


def get_cruise_IDS(cruise_name_list, server):
    """Returns IDs of input cruise names"""
    cruise_db_df = getListCruises(server)
    cruise_name_list = lowercase_List(cruise_name_list)
    cruise_ID_list_name = cruise_db_df["ID"][
        cruise_db_df["Name"].str.lower().isin(cruise_name_list)
    ].to_list()
    cruise_ID_list_nickname = cruise_db_df["ID"][
        cruise_db_df["Nickname"].str.lower().isin(cruise_name_list)
    ].to_list()
    combined_cruise_id = list(set(cruise_ID_list_name + cruise_ID_list_nickname))
    return combined_cruise_id


def get_region_IDS(region_name_list):
    """Returns IDs of input region names"""
    region_db_df = DB.DB_query("""SELECT * FROM tblRegions""")
    region_name_list = lowercase_List(region_name_list)
    region_ID_list = region_db_df["Region_ID"][
        region_db_df["Region_Name"].str.lower().isin(region_name_list)
    ].to_list()
    return region_ID_list


def exclude_val_from_col(series, exclude_list):
    """

    Parameters
    ----------
    series : Pandas Series
        Input Series
    exclude_list : list
        List of values to exclude from string.

    Returns
    -------

    Pandas Series
        Returns the dataframe with exclude list values removed.

    """
    mod_series = pd.Series(list(series[~series.isin(exclude_list)]))
    return mod_series


def empty_list_2_empty_str(inlist):
    """If a input list is empty returns an empty string, if not, list is returned"""
    if not inlist:
        inlist = ""
    return inlist


def cruise_has_trajectory(cruiseName):
    cruise_id = get_cruise_IDS([cruiseName.upper()])
    if len(cruise_id) == 0:
        print(cruiseName, " is not a valid cruise in the CMAP database.")
        cruise_has_traj = False
    else:
        cruise_id = cruise_id[0]
        cruise_traj_test_df = DB.DB_query(f"""SELECT TOP (1) * FROM tblCruise_Trajectory WHERE Cruise_ID = '{cruise_id}'""")
        if cruise_traj_test_df.empty:
            cruise_has_traj = False
        else:
            cruise_has_traj = True
    return cruise_has_traj


def getLatCount(tableName,server):
    query = (f"""SELECT SUM(p.rows) FROM sys.partitions AS p
    INNER JOIN sys.tables AS t
    ON p.[object_id] = t.[object_id]
    INNER JOIN sys.schemas AS s
    ON s.[schema_id] = t.[schema_id]
    WHERE t.name = N'{tableName}'
    AND s.name = N'dbo'
    AND p.index_id IN (0,1);""")

    df = DB.dbRead(query, server)
    lat_count = df.columns[0]
    return lat_count


def tableInDB(tableName):
    """Returns a boolean if tableName exists in DB."""
    qry = f"""SELECT TOP(1) * FROM {tableName}"""
    qry_result = DB.DB_query(qry)
    if len(qry_result) > 0:
        tableBool = True
    else:
        tableBool = False
    return tableBool


def datasetINtblDatasets(dataset_name):
    """Returns a boolean if dataset name exists in tblDatasets"""
    dataset_qry = DB.DB_query(f"""SELECT * FROM tblDatasets WHERE Dataset_Name = '{dataset_name}'""")
    if len(dataset_qry) >= 1:
        ds_bool = True
    else:
        ds_bool = False
    return ds_bool


def length_of_tbl(tableName):
    """Returns a string representation of the length of a SQL table. Alternate speedup?"""
    qry = f"""  select sum (spart.rows)
    from sys.partitions spart
    where spart.object_id = object_id('{tableName}')
    and spart.index_id < 2"""
    tableCount = list(DB.DB_query(qry))[0]
    return tableCount


def flist_in_daterange(start_date, end_date, tableName, branch, processing_lvl):
    base_path = (
        vault_struct_retrieval(branch) + "tableName" + "/" + processing_lvl + "/"
    )
    pass


def get_var_list_dataset(tableName):
    col_name_list = DB.DB_query(f"""EXEC uspColumns '{tableName}'""")["Columns"].to_list()
    return col_name_list


def double_chars_in_col(df, col, list_of_chars):
    """Replaces a single character with two of the same character. To be used in SQL Server escaping.

    Args:
        df (Pandas DataFrame): Input Pandas DataFrame
        col (str): column of DataFrame used in replace
        list_of_chars (list): A list of strings to be doubled

    Returns:
        df (Pandas DataFrame): Modified Pandas DataFrame
    """
    for char in list_of_chars:
        df[col] = df[col].str.replace(char, char * 2)
    return df
