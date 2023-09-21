Table Creation and Indexing
===========================


Space-Time Index 
----------------

All datasets in CMAP share a common data format that includes columns with space a time information (time, lat, lon, depth <opt.>). 
With these, space-time indexes can be created on data tables. Creating clustered indicies on this ST index will speed up query performance on large datasets. 
Only include depth in the index if the dataset has a depth component. 


Climatology
-----------

To quickly calculate climatology for large datasets, specific climatology indicies can be created. Examples of these can be found in the tblSSS_NRT_cl1 or others. 




File Groups 
-----------

In each server, data in the database is split into File Groups. The available space can be queried with a SQL Server stored procedure. 
You can execute a stored procedure either in SSMS, Azure Data Studio or a pyodbc query. 

.. code-block:: SQL

   EXEC uspFileGroup_Volume()
