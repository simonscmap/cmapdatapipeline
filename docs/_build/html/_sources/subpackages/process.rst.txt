process
=======

Process is similar to collect in some ways. It contains independent scripts for processing larger datasets into the CMAP database. 
It serves as a record for the data processing/cleaning steps.
The organization roughly mirors **vault/**.


data flow
---------
As files are processed, they should generally pass from **collected_data/** to **vault/**.
Smaller datasets can be exported to **staging/combined/** and can be run through the web validator. 
Larger datasets such as satellite, model or ARGO float data are too large to be run through the web validator. 
These datasets can be cleaned and ingested into the database in one process script. Once the data has been ingested, a cleaned version should be exported to **vault/**.




