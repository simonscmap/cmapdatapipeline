Data Validation
========

There are multiple data validation tests built into the ingestion code. Additional functions can be added to the scripts below, depending on when you would like the checks to run.


Pre-Ingestion Tests
-----------------------

There are multiple touch points for a use submitted dataset to pass through data and formatting checks. The first is a successful submission through the validator. Following that, the CMAP data curation team uses the QC API (https://cmapdatavalidation.com/docs#tag/Pre-Ingestion-Checks) to run additional checks and fill in keywords. Prior to the existence of the QC API, pre-ingestion checks were written into the ingestion code that are now duplicative. Specifically, checks for outliers in data values are done during submission to the validator, in the viz output of the QC API, and when the data is read into memory for ingestion. 

**ingest/data_checks.py** contains various functions for checking data, either before or during ingestion.

**check_df_on_trajectory** was a test written before the QC API checked if the data submitted fell within the space and time bounds of the associate cruise trajectory in CMAP. It uses the CMAP_Sandbox database on Beast, which will likely be retired.

**check_df_values** checks that lat and lon are within range, depth is not below zero, and checks all numeric variables for min / max < or > 5 times standard deviation of dataset. Returns 0 if all checks pass, returns 1 or more for each variable with values out of expected range. This is called in **general.py** with each ingestion.

**check_df_nulls** checks a dataframe against an existing SQL table for nulls where a SQL column is defined as not null. Returns 0 if all checks pass, returns 1 or more for each dataframe variable with nulls where SQL column is not null.

**check_df_constraint** checks a dataframe again an existing SQL table's unique indicies. Returns 0 if all checks pass, returns 1 if duplicates are in the dataframe in columns that should contain unique values. 

**check_df_dtypes** checks a dataframe against an existing SQL table. Returns 0 if all checks pass, returns 1 or more for each column with different data types.

**check_df_ingest** runs the last three tests and returns 0 if all checks passed.

**check_metadata_for_organism** checks variable_metadata_df for variable names or units that could be associated with organisms. Checks variable short and long names for any name found in tblOrganism. Returns True if no potential organism variables are present. This is called in **general.py** with each ingestion.


**validate_organism_ingest** checks for the presence of org_id and conversion_coefficient in the vars_meta_data sheet. If those columns are present but blank, if checks that they should be blank. Checks that both columns are filled if one is filled for a variable. Checks that the conversion_coefficient is correct based on unit (this last check could use additional logic). Returns True if columns are not present or no issues are found. This is called in **general.py** with each ingestion.


Post-Ingestion Tests
-----------------------

All post-ingestion tests are run when the ingestion server is Rainier. As Rainier is the production database for the CMAP website, testing for a successful ingestion on Rossby first is suggested. With Rainier as the final server for ingestion, the following tests are run in **post_ingest.py**:

**checkServerAlias** checks the table is present on each server listed for a dataset in tblDataset_Servers. If less than three servers are listed in tblDataset_Server, it checks each on prem server if the table is present.  

**checkRegion** checks that at least one region is associated with a dataset in tblDataset_Regions. If the dataset only lives on the cluster it will either assign all regions associated with Argo datasets, or allow you to enter your own. If no regions are associated with a dataset and it is in an on-prem server, regions will be assigned automatically based on a distinct list of lat and lon. 

**checkHasDepth** checks that the Has_Depth flag in tblVariables is accurate based on the presence of a depth column in the data table. 

**compareDOI** downloads a CMAP template from a DOI link and checks the data against what is in SQL. Checks numeric columns with math.isclose() as the number of significant digits can change on import. Deletes downloaded template after checks.

**pycmapChecks** calls various pycmap functions. Skips tests on stats if the dataset is larger than 2 million rows (included to stop SELECT * FROM running on the cluster, specifically for Argo). Note: due to a cache of the dataset IDs on the api layer, it's possible the pycmap tests will fail if the cache has not reset after ingestion.

**fullIngestPostChecks** runs all checks listed above, with the optional argument to check the DOI data. Also runs **api_checks.postIngestAPIChecks()** on every 10th dataset (see DB API Endpoints below for details on this). This is called in **general.py** with each ingestion to Rainier.


DB API Endpoints
-----------------------

Information on the DB API endpoints can be found here: https://cmapdatavalidation.com/docs#tag/Post-Ingestion-Checks

You can test out each endpoint here: https://cmapdatavalidation.com/try#/

Running the DB API checks on each ingestion should not be necessary. There is both a monetary cost for each API check and an unneccesary load on the servers to justify running these checks on each ingestion. Currently the logic to run these checks is on every 10 datasets. Each new dataset, or metadata update, will result in a new Dataset ID. These IDs are not backfilled, so when metadata is deleted, the old Dataset ID will not be reused. 

The **fullIngestPostChecks** function will run **api_checks.postIngestAPIChecks()  ** when a Dataset ID is divisible by ten. 

.. code-block:: python

    def postIngestAPIChecks(server = 'Rossby'):
    ## Runs DB endpoint checks. Default server is Rossby
        db_name = 'Opedia'
        strandedTables()
        strandedVariables(server, db_name) ## Checks all on prem servers
        numericLeadingVariables(server, db_name)
        duplicateVarLongName(server, db_name)
        duplicateDatasetLongName()
        datasetsWithBlankSpaces(server, db_name)
        varsWithBlankSpace(server, db_name)

The default server is Rossby as it's the fastest. Only strandedVariables() checks all on prem servers.