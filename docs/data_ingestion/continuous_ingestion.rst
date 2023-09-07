Continuous Ingestion
========

There are currently 13 datasets processed and ingested continuously. For details on the project, see Jira ticket 688 (https://simonscmap.atlassian.net/browse/CMAP-688)

All near real time (NRT) datasets are only ingested to the cluster. Note that dataset replication can be done across any of our servers. See Jira ticket 582 for details on the distributed dataset project (https://simonscmap.atlassian.net/browse/CMAP-582). The following datasets are downloaded, processed, and ingested utilizing run_cont_ingestion.py

.. list-table:: Datasets Collected and Processed Daily
   :widths: 25 25 50
   :header-rows: 1

   * - Dataset
     - Target Servers
     - New Data Available
   * - Sattelite SST
     - Rainier, Mariana, Rossby, Cluster
     - Daily
   * - Satellite SSS
     - Rainier, Mariana, Rossby, Cluster
     - Daily, ~2 week lag
   * - Satellite CHL
     - Mariana, Rossby, Cluster
     - ~Weekly, ~2 month lag
   * - Satellite CHL NRT
     - Cluster
     - ~Weekly    
   * - Satellite POC
     - Mariana, Rossby, Cluster
     - ~Weekly, ~2 month lag       
   * - Satellite POC NRT
     - Cluster
     - ~Weekly   
   * - Satellite AOD 
     - Mariana, Rossby, Cluster 
     - Monthly         
   * - Satellite Altimetry NRT (Signal)
     - Cluster
     - Daily   
   * - Satellite Altimetry (Signal)
     - Mariana, Rossby, Cluster 
     - ~4x a year         
   * - Satellite PAR (Daily)
     - Cluster
     - Daily, ~1 month lag            
   * - Satellite PAR NRT (Daily) 
     - Cluster
     - Daily                 

Each dataset has a collect and process script. 

Collection Scripts
----------------------------------
Collection scripts can be found in cmapdata/collect/model and in cmapdata/collect/sat within the dataingest branch of the GitHub repository (https://github.com/simonscmap/cmapdata). Each dataset (with the exception of Argo) adds an entry per file downloaded to tblProcess_Queue. 

**tblProcess_Queue** contains information for each file downloaded for continuous ingestion. It holds the original naming convention for each file, the file's relative path in the vault, the table name it will be ingested to, the date and time it was downloaded and processed, and a column to take note of download errors.

* ID
* Original_Name
* Path
* Table_Name
* Downloaded
* Processed
* Error_Str

Each collection script does the following: 

 1. Retries download for any file previously attempted that includes and error message
 2. Get the date of the last succesful download
 3. Attempts to download the next available date range. Start and end dates are specific to each dataset's temporal resolution and new data availability (see New Data Available column in table above)
 4. Entries for each date a download is attempted for are writted to tblProcess_Queue, with successful downloads denoted by a NULL Error_Str

Dataset-specific logic:

 1. tblPisces_Forecast_cl1 updates the data on a rolling schedule. Each week new data is available, and ~2 weeks of previous data is overwritten by the data producer. GetCMEMS_NRT_MERCATOR_PISCES_continuous.py will re-download these files, and if successfully downloaded, will delete the existing data for that day from the cluster. Simply creating new parquet files and pushing to S3 does not update the data in the cluster, so it is necessary to delete the data first.
 2. The NRT datasets should not overlap with the matching REP datasets. For example, when new data is successfully ingested for tblModis_PAR_cl1, data for that date is deleted from tblModis_PAR_NRT.
 3. A successful download for one day of tblWind_NRT_hourly data results in 24 files. An additional check is in place when finding the last successful date downloaded by including there need to be 24 files for that date. If it is less than 24, it will retry downloading that date.
 4. Data for tblAltimetry_REP_Signal is updated by the data provider sporadically throughout the year. New data is checked weekly. Date range for downloads is based on latest files in the FTP server. See /collect/sat/CMEMS/GetCMEMS_REP_ALT_SIGNAL_continuous.py for details.


If a file download is attempted but fails because no data is available yet, the Original_Name will be the date where data wasn't available (ex. "2023_08_09") and the Error_Str will be populated. Each collection script has a retryError function that queries dbo.tblProcess_Queue for any entries where Error_Str is not null. If a file is successfully downloaded on a retry, tblProcess_Queue will be updated with the original file name and date of successful download. 


Process Scripts
----------------------------------

Each process script does the following:


 1. Pulls newly downloaded files from tblProcess_Queue where Error_Str is NULL. 
 2. Does a schema check on the new downloaded file against the oldest NetCDF in the vault
 3. Processes file and adds climatology fields (year, month, week, dayofyear)
 4. Does a schema check on the processed file against the oldest parquet file in the vault
 5. Ingests to on-prem servers (see Target Servers column in table above)
 6. Copies parquet file to S3 bucket for ingestion to the cluster
 7. Updates Processed column in tblProcess_Queue
 8. Adds new entry to tblIngestion_Queue


**tblIngestion_Queue** contains information for each file processed for continuous ingestion. It holds the file's relative path in the vault, the table name it will be ingested to, the date and time it was moved to S3 (Staged), and the date and time it was added to the cluster (Started and Ingested).

* ID
* Path
* Table_Name
* Staged
* Started
* Ingested


Once all new files have been processed from tblProcess_Queue and added to tblIngestion_Queue, trigger the ingestion API. The URL is saved in ingest/credentials.py as S3_ingest. It is best to only trigger the ingestion API once, which is why the snippet below is run after files for all datasets have been processed. See Jira ticket 688 for additional details: (https://simonscmap.atlassian.net/browse/CMAP-688)

.. code-block:: python

   requests.get(cr.S3_ingest)


After all files have successfully ingested to the cluster (Ingested will be filled with the date and time it was completed), each dataset will need updates to tblDataset_Stats. In run_cont_ingestion.py, updateCIStats(tbl) formats the min and max times to ensure the download subsetting and viz page works properly. In short, time must be included, along with the '.000Z' suffix.

Troubleshooting
----------------------------------
Occasionally datasets will have days missing, resulting in a date being retried on each new run of run_cont_ingestion.py. In some cases, data will never be provided for these dates. This information can be found in the documentation provided by each data provider. For example, the SMAP instrument used for SSS data experienced downtime between Aug 6 - Sept 23 2022 (see Missing Data section: https://remss.com/missions/smap/salinity/). That date range was deleted from tblProcess_Queue to prevent those dates from being rechecked each run. 

If there are known issues of data already ingested that the data producer has fixed, the entry for the impacted dates should be deleted from tblProcess_Queue and tblIngestion_Queue and redownloaded. Data should be delete from impacted on-prem servers and the cluster as applicable before reingestion. 

Each dataset's processing script has checks for changes in schema. Some data providers will change the dataset name when a new version is processed, but not all. If a processing script finds a schema change for a dataset that has the same name / ID / version number, a new dataset should be made in CMAP with a suffix denoting a new change log iteration. For example, tblModis_CHL_cl1 is a reprocessed version of tblModis_CHL. 


Batch Ingestion
----------------------------------
The following datasets are to be ingested monthly. Due to the nature of updates done by the data provider, each month of Argo is a new dataset. These datasets will be ingested via batch ingestion instead of appending to existing tables like the datasets described above. See the outside large dataset walkthrough for details on Argo processing.  

.. list-table:: Datasets Collected and Processed Monthly
   :widths: 25 25 50
   :header-rows: 1
   * - Dataset
     - Target Servers
     - New Data Available    
   * - Argo REP Core
     - Cluster 
     - Monthly      
   * - Argo REP BGC
     - Cluster 
     - Monthly                 



Continuous Ingestion Badge on Website
----------------------------------
The CMAP Catalog page includes a filter for Continuously Updated datasets and displays badges for each applicable dataset.


.. figure:: ../_static/CI_screenshot.png
   :scale: 70 %
   :alt: CMAP Catalog Continuous Ingestion Filter


The badges and filter call the uspDatasetBadges stored procedure, which in turn calls the udfDatasetBadges() function. As Argo datasets are a batch ingestion, they are not included in tblProcess_Queue. In order to have the badges display for Argo datasets, a union was done for any Argo REP table, regardless of month.

.. code-block:: SQL

  select distinct table_name, ci = 1 from tblProcess_Queue 
	union all
	select distinct table_name, ci = 1  from tblvariables where Table_Name like 'tblArgo%_REP_%'