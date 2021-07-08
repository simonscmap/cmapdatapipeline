###
# PI = post ingestion test
###
import pandas as pd
import numpy as np
import DB


def len_comparison_data_SQL(tableName, data_df):
    length_SQL = cmn.length_of_tbl(tableName)
    length_data_df = len(data_df)
    length_diff_int = np.abs(length_SQL - length_data_df)
    if length_diff_int != 0:
        len_comparison_data_SQL = False
        err_msg = """The lengths of the SQL table and the pandas dataframe do not match. Length of SQL Table: {sql_len}. Length of DataFrame: {data_df}""".format(
            sql_len=length_SQL, data_df=length_data_df
        )
    else:
        len_comparison_data_SQL = True
        err_msg = ""
    error_dict = {"bool": len_comparison_data_SQL, "msg": err_msg}
    return error_dict


def data_tests(data_df, tableName):
    tableInDB_bool = tableInDB(tableName)
    error_dict = len_comparison_data_SQL(tableName, data_df)
    pass


def metadata_tests():
    dataset_name_valid_bool = cmn.datasetINtblDatasets(dataset_name)

    pass


def stored_proc_tests():
    pass


def pycmap_tests():
    pass


def main(tableName):
    pass


"""

retrieval funcs will live in common...

dataset either full or partially ingested into DB:

-Data tests:
  -does the tableName exist? - func/test
  -does the len(tableName) == len(data_df)?
  -are there any empty columns?
  -do the column names match the variable names?


  -does the table contain (time,lat,lon(depth?))
  -can you select top(1) from tableName.
  -
  -Index:
  -Does the index exist?
  -Is the index on space-time?

-Metadata Table tests:
  -tblDatasets:
    -Does the table exist in tblDatasets?- func and test done
    -Are there any null cols in tblDatasets?
  -tblDatasetStats:
    -Do that stats match the min/max of stats from pandas (df.describe())
    -PI(check that stats have updated...)
  -tblDataset_References:
    -if refs exist in df_dataset_metadata, are they in table.
    -Do the ref strings match
  -tblVariables:
    -Do the num of rows match the num of rows in vars_meta_data
    -Are there any NULL values? Warning on some of them (ie. not history)
   -tblKeywords:
       -Do the len(distinct(keyword_IDs)) match len(vars_metadata).

   UDFCATALOG() where tablename = tablename:
   - does len of UDFCATALOG match len vars_meta_data?






Is data retrievable
Test sp_subset
Do the number of rows match in SQL match the # of rows in the dataframe?
Min,max of ST cols match min,max in dataframe and tblDataset_Stats?



Is data visualizable
Pycmap?
- pick single day in daterange. min/max bounds
Web App?



Is metadata complete
Does metadata retrieval match dataset metadata?

"""
