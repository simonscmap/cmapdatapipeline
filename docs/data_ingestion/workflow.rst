Workflow
========

.. warning::
   Rainier is currently the production database and 'source of truth'. If you want to test features, use Mariana or Rossby. 

   
The process for ingesting datasets into CMAP differs based on a few factors. 
The three main categories are *User Submitted Datasets*, *Outside 'Small' Datasets* and *Outside 'Large' Datasets*.
User submitted datasets that pass through the web validator must be <150MB. 
*Outside 'Small' Datasets* are datasets collected that are collected from an outside source that can generally fit in memory. An example would be an AMT or HOT dataset. 
*Outside 'Large' Datasets* are datasets collected from an outside source that have multiple data files and cannot fit into memory. Examples are satellite data, model data or large insitu collections such as ARGO or GOSHIP.


User Submitted Datasets
-----------------------
User submitted datasets are submitted through the web validator. Once the QA/QC checks are completed and a DOI is received, the dataset can be ingested into CMAP. Details on the QC process can be found here: https://simonscmap.atlassian.net/browse/CMAP-621. An additional step of adding org_id and conversion_coefficient columns to the variable metadata sheet in the submitted template is done only for variables describing organism abundance. 


Using general.py, you can pass command line arguments to specify which server you wish to add the dataset to as well as including a DOI.

Where we have:

.. code-block:: python

   python general.py {table_name} {branch} {filename} {-d} {DOI link} {-l} {DOI download link} {-f} {DOI file name} {-S} {server}

* {**table_name**}: Table name for the dataset. Must start with prefix "tbl". Ex. tblFalkor_2018
* {**branch**}: Branch where dataset should be placed in Vault. Ex's: cruise, float, station, satellite, model, assimilation
* {**filename**}: Base file name in vault/staging/combined/. Ex.: 'global_diazotroph_nifH.xlsx'
* {**-d**}: Optional flag for including DOI with dataset in tblReferences. DOI link string follows flag arg. 
* {**DOI link**}: String for full web address of CMAP specific DOI. Ex. "https://doi.org/10.5281/zenodo.8306724"
* {**-l**}: Optional flag for including the DOI download link in tblDataset_DOI_Download. DOI dowload link string follows flag. 
* {**DOI download link**}: String for DOI download link of CMAP specific DOI. Ex. "https://zenodo.org/record/8306724/files/Gradients5_TN412_LISST_DEEP_Profiles.xlsx?download=1"
* {**-f**}:  Optional flag for DOI file name. DOI file name string follows flag. 
* {**DOI file name**}:  String for filename of CMAP specific DOI. Ex. "Gradients5_TN412_LISST_DEEP_Profiles.xlsx"
* {**-t**}: Optional flag for denoting if DOI is a web validator template. Default value is 1.
* {**DOI in CMAP template**}:  Boolean if DOI is a web validator template.
* {**-S**}: Required flag for specifying server choice. Server name string follows flag. 
* {**server**}: Valid server name string.  Ex. "Rainier", "Mariana" or "Rossby"


An example string would be:

.. code-block:: python

   python general.py tblTN412_Gradients5_LISST_DEEP_Profiles cruise 'Gradients5_TN412_LISST_DEEP_Profiles.xlsx' -S 'Rossby' -d 'https://doi.org/10.5281/zenodo.8306724' -l 'https://zenodo.org/record/8306724/files/Gradients5_TN412_LISST_DEEP_Profiles.xlsx?download=1' -f 'Gradients5_TN412_LISST_DEEP_Profiles.xlsx'




general.py contains wrapper functions that will split the excel sheet into pandas dataframes, transfer the data to vault/, build a suggested SQL table, insert data, split dataset_meta_data and vars_meta_data into SQL queries and insert into SQL metadata tables, build summary statistics, match provided cruises to cruises in the database, classify the dataset into ocean regions and create maps and icons for the web catalog.

Certain functions are only run when the server name is Rainier (creating icon map, data server alias assignment, and data ingestion tests). A suggested order for server ingestion is starting with Rossby (the fastest server), then ingesting to Mariana, and finally on Rainier. As DOIs are requirements for user submitted datasets, a function to test the data in the DOI matches the data in Rainier also runs automatically. 



Outside 'Small' Datasets
------------------------

These datasets usually need quite a bit of data munging to make them match the CMAP data format. Additionally, metadata needs to be collected and created.
To keep a record of data transformations, any processing scripts should be placed in **/process/../process_datasetname.py**. Additionally, any relevant collection information should be placed in **/collect/../collect_datasetname.py**. A text file containing a link to the process and collect scripts in GitHub should be saved in the vault to **{dataset table name}/code/**

With the addition of the QC API, it is suggested to submit the final, cleaned dataset to the validator. 


Outside 'Large' Datasets
------------------------

These datasets are usually composed of multiple data files (generally in netcdf or hdf5). Some features of the ingestion pipeline only work for data that can fit into memory. Because of this, special care is needed to ingest these large datasets.
All raw data should be saved in the vault /raw folder for the dataset. Depending on the source, data is downloaded using curl/wget/ftp etc. Any collection scripts should be stored in **/collect/../{collect_datasetname.py}.
Once data has been transfered, the next step is any data processing. This should be recorded in **/process/../process_datasetname.py**. A text file containing a link to the process and collect scripts in GitHub should be saved in the vault to **{dataset table name}/code/**

In this data processing script, data should be read from the vault /raw folder, cleaned, sorted and inserted into the database(s). 

.. note::
   You will need to create a SQL table and add it to the databases prior to ingestion. Any SQL table creation script should be recorded in DB/ (repository is on Simons CMAP github). Adding indexes once the ingestion has completed will likely speed up ingestion.

After the data has been inserted and the indices successfully created, metadata will need to be created and added to the databases. A standard excel template should be used for the dataset and vars metadata sheets. Submit a template to the validator with a dummy data sheet that holds all variables, but only needs one row of data to make it through the validator. This allows the data curation team to run the QC API checks and create the /final folder needed for ingesting the metadata. 

