DB
==


The repository DB (https://github.com/simonscmap/DB) under the Simons CMAP github page stores all of the SQL Server table creation scripts.

This repo contains subdirectories for db creation, stored procedures (usp), user-defined functions(udf) and data and metadata tables. 

The structure with examples is outlined below:



::

    ├── db
    │   └── db.sql
    ├── tables
    │   ├── api
    │   │   └── tblFront_End_Errors.sql
    │   ├── core
    │   │   ├── cruise
    │   │   │   └── tblCruise_Trajectory.sql
    │   │   └── tblVariables_update.sql
    │   ├── model
    │   │   └── tblPisces_NRT.sql
    │   ├── observation
    │   │   └── tblWOA_Climatology.sql
    │   └── satellite
    │       └── tblWind_NRT.sql
    ├── udf
    │   └── udfVariableMetaData.sql
    └── usp
        └── uspWeekly.sql
