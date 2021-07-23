Database Design and Table Structure
===================================

.. figure:: ../_static/DB_diagram.png
   :alt: CMAP DB metadata specific table database diagram

   CMAP DB metadata specific table database diagram

Simons CMAP currently has three servers that contain near replicates of the SQL Server database. The names of these three are: Rainier, Mariana and Rossby. 
Rainier was the first dedicated server to host the database and currently serves as the main 'source of truth'. 

Data tables along with metadata tables are stored in the same schema. Common queries live on the database as stored procedures. 
Data tables are independent of one another. The dataset table name is stored as a column in the metadata table *tblVariables*. 
This key links the data tables to the rest of the metadata.

Metadata tables are listed below with a brief description of each:

Variable Level Metadata
-----------------------

**tblVariables** links the data tables to the metadata through the column [Table_Name]. 
Columns with an _ID suffix are linked to other metadata tables. For example, Dataset level information in tblDatasets in linked through [Dataset_ID].
Each data variable in a dataset has a row in this table, containing the following columns:

* ID
* DB
* Dataset_ID
* Table_Name
* Short_Name
* Long_Name
* Unit
* Temporal_Res_ID
* Spatial_Res_ID
* Temporal_Coverage_Begin
* Temporal_Coverage_End
* Lat_Coverage_Begin
* Lat_Coverage_End
* Lon_Coverage_Begin
* Lon_Coverage_End
* Grid_Mapping
* Make_ID
* Sensor_ID
* Process_ID
* Study_Domain_ID
* Comment
* Visualize
* Data_Type


**tblKeywords** contains user submitted keywords used in the searching of a variable. 
tblKeywords contains an ID column, where each value which corresponds to a unique variable entry in tblVariables. 

* var_ID
* keywords


**tblTemporal_Resolution**, **tblSpatial_Resolution**, **tblMake**, **tblSensor**, **tblProcess_Stages** and **tblStudy_Domains** are all 
variable level tables that contain links between the ID's in tblVariables and their respective tables. 






Dataset Level Metadata
----------------------

**tblDatasets** contains dataset level information and has links to tblVariables as well as links to cruise and region level information.


* ID
* DB
* Dataset_Name
* Dataset_Long_Name
* Variables
* Data_Source
* Distributor
* Description
* Climatology
* Acknowledgement
* Doc_URL
* Icon_URL
* Contact_Email
* Dataset_Version
* Dataset_Release_Date
* Dataset_History



**Cruise Metadata**
The metadata for cruises in CMAP is separated into multiple tables. A core **tblCruise** links 
together metadata tables for cruise trajectories, cruise keywords, cruise region links and cruise dataset links.
**tblDataset_Cruises** is the linking table to connect cruise_IDs with dataset_IDs.  

**Region Metadata**
The region tables in CMAP share a similar schema to the cruise tables layout. **tblRegions** contains the ocean regions 
(this can be expanded). 
**tblDataset_Regions** is similar to **tblDataset_Cruises** because it acts as the linking table between the region and dataset tables. 
**tblCruise_Regions** is a linking table between cruise IDs and region IDs. 

