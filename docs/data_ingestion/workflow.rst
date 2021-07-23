Workflow for User Submitted Datasets
====================================

The process for ingesting datasets into CMAP differs based on a few factors. 
The three main categories are *User Submitted Datasets*, *Outside 'Small' Datasets* and *Outside 'Large' Datasets*.
User submitted datasets that pass through the web validator must be <150MB. 
*Outside 'Small' Datasets* are datasets collected that are collected from an outside source that can generally fit in memory. An example would be an AMT or HOT dataset. 
*Outside 'Large' Datasets* are datasets collected from an outside source that have multiple data files and cannot fit into memory. Examples are satellite data, model data or large insitu collections such as ARGO or GOSHIP.


User Submitted Datasets
-----------------------
User submitted datasets are submitted through the web validator. Once the QA/QC checks are completed and a DOI is received, the dataset can be ingested into CMAP.
Starting out with the dataset in '/CMAP Data Submission Dropbox/Simons CMAP/staging/combined/{dataset_name}.xlsx, the data ingestion pipeline should work.

Using general.py, you can pass command line arguments to specify which server you wish to add the dataset to as well as including a DOI.

Where we have:

.. code-block:: python

   python general.py {table_name} {branch} {filename} {-d} {DOI link} {-S} {server}

* {**branch**}: Branch where dataset should be placed in Vault. Ex's: cruise, float, station, satellite, model, assimilation]
* {**filename**}: Base file name in vault/staging/combined/. Ex.: 'global_diazotroph_nifH.xlsx'
* {**-d**}: Optional flag for including DOI with dataset in tblReferences. DOI link string follows flag arg. 
* {**DOI link**}: String for full web address of CMAP specific DOI. Ex. "http://doi.org/10.5281/zenodo.4968554"
* {**-S**}: Required flag for specifying server choice. Server name string follows flag. 
* {**server**}: Valid server name string.  Ex. "Rainier", "Mariana" or "Rossby"


An example string would be:

.. code-block:: python

   python general.py tblglobal_diazotroph_nifH cruise 'global_diazotroph_nifH.xlsx' -d "http://doi.org/10.5281/zenodo.4968554" -S "Rainier"




general.py contains wrapper functions that will split the excel sheet into pandas dataframes, transfer the data to vault/, build a suggested SQL table, insert data, split dataset_meta_data and vars_meta_data into SQL queries and insert into SQL metadata tables, build summary statistics, match provided cruises to cruises in the database, classify the dataset into ocean regions and create maps and icons for the web catalog.

Once the dataset has been successfully ingested (appears in the web catalog and can be visualized), it should be ingested on the other servers.



Outside 'Small' Datasets
------------------------

These datasets usually need quite a bit of data munging to make them match the CMAP data format. Additionally, metadata needs to be collected and created.
To keep a record of data transformations, any processing scripts should be placed in **/process/../process_datasetname.py**. Additionally, any relevant collection information should be placed in **/collect/../collect_datasetname.py**


Outside 'Large' Datasets
------------------------

These datasets are usually composed of multiple data files (generally in netcdf or hdf5). Some features of the ingestion pipeline only work for data that can fit into memory. Because of this, special care is needed to ingest these large datasets.
Data usually starts in the /collect/ step. Depending on the source, data is downloaded using curl/wget/ftp etc. into **vault/collected_data/dataset_name/rep/*files/**. Any collection scripts should be stored in **/collect/../{collect_datasetname.py}.
Once data has been transfered, the next step is any data processing. This should be recorded in **/process/../process_datasetname.py**. 

In this data processing script, data should be read from **/collected_data/**, cleaned, sorted and inserted into the database(s). 

.. note::
   You will need to create a SQL table and add it to the databases prior to ingestion. Any SQL table creation script should be recorded in DB/ (repository is on Simons CMAP github). Adding indexes once the ingestion has completed might speed up ingestion.

After the data has been inserted and the indices successfully created, metadata will need to be created and added to the databases. A standard excel template should be used for the dataset and vars metadata sheets.

