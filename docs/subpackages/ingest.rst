ingest
======

ingest/ is the submodule that contains ingestion and data processing functions, data vault structure logic and DB connection information.
Ingesting a dataset using this submodule will be covered in data_ingestion/workflow.

ingest/ is broken into the multiple python scripts to separate ingestion logic. Descriptions and uses of each are described below.

common.py
---------

This scrip holds many commonly used simple functions for data processing/cleaning. It is a good location for generalized use functions.


credentials.py
--------------

This is a simple file hidden with .gitignore. **Make sure this is not pushed to github!**
It contains usernames, passwords, ip addresses, ports and connection strings. It is used primarilly by DB.py for database connections. Make sure this file is duplicated across machines.


cruise.py
---------

This file contains some cruise metadata helper functions and database calls along with a smattering of old cruise trajectory/metadata webscrapping stuff for r2r (rolling deck to repository).


data.py
-------

This file contains general cleaning and data processing functions specifically for the data sheet of the template.

DB.py
-----

DB contains the SQL connection logic, table insert logic, bcp (MSSQL bulk copy program) wrapper functions etc.

general.py
----------

general.py contains wrapper functions that take arguments through argparse to ingest datasets. It is the main script used in the collection script.

ingest_test.py
--------------

This is a fragment of a started post-ingestion test suite, with the aim of testing the success of the ingestion. Could be removed, rewritted or expanded upon.

mapping.py
----------

Contains the functionallity for creating .png and .html interactive maps from input datasets. These are stored in /static and used in the web catalog to give a spatial representation of a dataset. html interactive maps were never included in the catalog, but could be.

metadata.py
-----------

This contains multiple functions to format the dataset_meta_data and vars_meta_data into custom SQL queries.

region_classification.py
------------------------

This uses input dataset coordinates along with a geopackage of ocean regions to classify a dataset by spatial region.

SQL.py
------

Has functionality to suggest SQL tables and basic indices.

stats.py
--------

Functions to build summary statistics from datasets. These results are used for data size estimations in the web app.

transfer.py
-----------

A few functions to move and split excel files from /staging to /vault

vault_structure.py
------------------

vault_structure contains the relative paths of vault as well as some directory creation structure.

::

    ├── assimilation
    ├── model
    ├── observation
        ├── in-situ
        │   ├── cruise
        │   ├── drifter        
        │   ├── float
        │   ├── mixed        
        │   └── station
        └── remote
            └── satellite
    ├── r2r_cruise

