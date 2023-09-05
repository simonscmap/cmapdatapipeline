User Submitted Dataset Walkthrough
==================================


This example should walk you through the steps of ingesting a user submitted dataset into the database.

For this example, we are going to be using the dataset: **Falkor_2018 - 2018 SCOPE Falkor Cruise Water Column Data**


Removal of Previously Existing Dataset 
--------------------------------------

This dataset was an early submitted dataset and has recently been revised to bring it up to line with the current CMAP data submission guidelines. The dataset was reviewed by CMAP data curators, which means the finalized updated dataset will be found in the /final folder: **Dropbox/Apps/<dataset_short_name>/final**

Because this dataset already exists in the database, we must first remove the old version.

To do this, we can use some of the functionality in cmapdata/ingest/metadata.py


By calling this function **deleteCatalogTables(tableName, db_name, server)**, we can remove any metadata and data tables from a given server. 

.. warning::
    This function has drop privileges! Make sure you want to wipe the dataset metadata and table.



.. code-block:: console

   python metadata.py 

.. code-block:: python

   >>> deleteCatalogTables('tblFalkor_2018','Rainier')

Continue this function for any other existing servers. ex. 'Mariana', 'Rossby'

If only the metadata needs updating, calling the function **deleteTableMetadata(tableName, db_name, server)** will remove all metadata associated with the dataset, but will not delete the data table. 



Specifying the Ingestion Arguments 
----------------------------------


Using ingest/general.py, you can pass command line arguments to specify which server you wish to add the dataset to as well as including a DOI. Because the DOI is a CMAP template, the optional flag -t is not necessary to include as the default is true.

Navigate to the ingest/ submodule of cmapdata. From there, run the following in the terminal. 

.. code-block:: python

   python general.py {table_name} {branch} {filename} {-d} {DOI link} {-l} {DOI download link} {-f} {DOI file name} {-S} {server}

* {**branch**}: Branch where dataset should be placed in Vault. Ex's: cruise, float, station, satellite, model, assimilation
* {**filename**}: Base file name in vault/staging/combined/. Ex.: 'Falkor_2018.xlsx'
* {**-d**}: Optional flag for including DOI with dataset in tblReferences. DOI link string follows flag arg. 
* {**DOI link**}: String for full web address of CMAP specific DOI. Ex. "https://doi.org/10.5281/zenodo.5208854"
* {**-l**}: Optional flag for including the DOI download link in tblDataset_DOI_Download. DOI dowload link string follows flag. 
* {**DOI download link**}: String for DOI download link of CMAP specific DOI. Ex. "https://zenodo.org/record/5208854/files/tblFalkor_2018%20%282%29.xlsx?download=1"
* {**-f**}:  Optional flag for DOI file name. DOI file name string follows flag. 
* {**DOI file name**}:  String for filename of CMAP specific DOI. Ex. "tblFalkor_2018 (2).xlsx"
* {**-t**}: Optional flag for denoting if DOI is a web validator template. Default value is 1.
* {**DOI in CMAP template**}:  Boolean if DOI is a web validator template.
* {**-S**}: Required flag for specifying server choice. Server name string follows flag. 
* {**server**}: Valid server name string.  Ex. "Rainier", "Mariana" or "Rossby"


An example string for full ingest would be:

.. code-block:: python

   python general.py tblFalkor_2018 cruise 'Falkor_2018.xlsx' -d 'https://doi.org/10.5281/zenodo.5208854' -l 'https://zenodo.org/record/5208854/files/tblFalkor_2018%20%282%29.xlsx?download=1' -f 'tblFalkor_2018 (2).xlsx' -S 'Rainier'

There are two additional arguments if you are only updating the metadata, and not reingesting the data table.

* {**-U**}: Optional flag for specifying updates to metadata only
* {**-F**}: Optional boolean depth flag. Default value is 0. Set to 1 if the data has a depth column

An example string for metadata update would be:

.. code-block:: python

   python general.py tblFalkor_2018 cruise 'Falkor_2018.xlsx' -d 'https://doi.org/10.5281/zenodo.5208854' -l 'https://zenodo.org/record/5208854/files/tblFalkor_2018%20%282%29.xlsx?download=1' -f 'tblFalkor_2018 (2).xlsx' -S 'Rainier' -F 1 -U

Behind the scenes, the script is doing:

 1. Parsing the user supplied arguments. 
 2. Creates **vault/** folder structure, syncs Dropbox, and transfers the files to **vault/**. 
 3. Splitting the data template into data, dataset_metadata and vars_metadata files. Saves metadata as parquet files in /metadata folder.
 4. Importing into memory the data, dataset_metadata and vars_metadata sheets as pandas dataframes. 
 5. Checks for presence of variables describing organism abundance based on units, short name, and long name. If Org_ID is present in the variable metadata sheet, checks that Conversion_Coefficient aligns with units.
 6. Checks values in data for outliers (+/- five times standard deviation) or invalid ranges for lat, lon, depth.
 7. Creating a suggested SQL table and index based on the infered data. 
 8. Insert data into newly created table. 
 9. Insert metadata into various metadata tables, match cruises and classify ocean region(s). 
 10. Create summary stats and insert into tblDataset_Stats.
 11. Add server alias to tblDataset_Servers.
 12. Create dataset icon and push to github. 


Once the ingestion completes without any errors, check the catalog to see if the table is visable.
It is also advisable to try to plot one or more variables, download the full datasets, as well as a subset. 



.. Note::
   See the future code changes section for ideas on improvements.

   





