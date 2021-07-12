ingest
======

ingest/ is the submodule that contains ingestion and data processing functions, data vault strucutre logic and DB connection information.
Ingesting a dataset using this submodule will be covered in data_ingestion/workflow.

ingest/ is broken into the multiple python scripts to seperate ingestion logic. Desiciptions and uses of each are descibed below.

common.py
---------
This scrip holds many commonly used simple functions for data processing/cleaning. It is a good location for generalized use functions.


credentials.py
--------------
This is a simple file hidden with .gitignore. **Make sure this is not pushed to github!**
It contains usernames, passwords, ip addresses, ports and connection strings. It is used primarilly by DB.py for database connections. Make sure this file is duplicated across machines.


cruise.py
---------
