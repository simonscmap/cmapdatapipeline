process
=======

Process is similar to collect in some ways. It contains independent scripts for processing larger datasets into the CMAP database. 
It serves as a record for the data processing/cleaning steps.
The organization roughly mirors **vault/**.


data flow
---------
As files are processed, retain the raw, unprocessed data in **vault/../{dataset_name}/raw**.
Smaller datasets can be exported to **vault/../{dataset_name}/raw** to create a template for submitting to the web validator. 
Larger datasets such as satellite, model or ARGO float data are too large to be run through the web validator. Metadata should still go through the validator, including dummy data in the data tab with at least 1 row of placeholder data. 
These datasets can be cleaned and ingested into the database in one process script. Once the data has been ingested, a cleaned version should be exported to **vault/** as a parquet file.




