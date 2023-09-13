Outside Large Dataset Walkthrough
=================================

Outside large datasets require similar collecting/processing methods to outside small datasets, however the ingestion strategy can differ. 
In this section, we will outline examples of collecting, processing and ingesting two large outside datasets.



Argo Float Walkthrough
----------------------

The ARGO float array is a multi-country program to deploy profiling floats across the global ocean. These floats provide a 3D insitu ocean record. 
The CORE argo floats provide physical ocean parameters, while the BGC (Biogeochemical) specific floats provide Biogeochemical specific variables (nutrients, radiation etc.).

These Argo datasets are a part of the continuous ingestion project, but differ in process as each month will create a new table for each dataset. 


Argo Data Collection 
~~~~~~~~~~~~~~~~~~~~

ARGO float data are distributed through two main DAAC's. Individual files can be accessed directly from FTP servers from each DAAC.
Alternatively, a zipped file of all float records updated monthly can be found at: https://www.seanoe.org/data/00311/42182/. These are released on the 10th of every month.



.. figure:: ../../_static/seanoe_argo.png
   :scale: 80 %
   :alt: seanoe_argo



To keep a record of the collection, we will create a collect_{dataset_name}.py file. 


.. code-block:: python

    import vault_structure as vs
    import os

    def downloadArgo(newmonth, tar_url):
        """Download Argo tar file. Creates new vault tables based on newmonth stub
        Args:
            newmonth (string): Month and year of new data used as table suffix (ex. Sep2023)
            tar_url (string): URL pointing to tar download for newest data (ex. https://www.seanoe.org/data/00311/42182/data/104707.tar.gz)
        """
        tbl_list = [f'tblArgoCore_REP_{newmonth}',f'tblArgoBGC_REP_{newmonth}']
        for tbl in tbl_list:
            vs.leafStruc(vs.float_dir+tbl)
        base_folder = f'{vs.float_dir}{tbl}/raw/'
        output_dir = base_folder.replace(" ", "\\ ")      
        os.system(f"""wget --no-check-certificate {tar_url} -P {output_dir}""")


The raw data will be saved in **dropbox/../vault/observation/in-situ/float/tblArgoBGC_REP_{newmonth}/raw**
This file will need to be unzipped, either using python or bash. The functions for doing so in Python are in process_ARGO_BGC_Sep2023.py

Once the data has been unzipped, there are four subfolders: 

::

    ├── aux
    ├── dac
    ├── doc
    └── geo


**dac** contains the data. Descriptions for the rest can be found in the argo data handbook (http://dx.doi.org/10.13155/29825).

The **dac** subfolder contains 11 daacs/distributors.  Each of these contains zipped files. 

To unzip and organize these by BGC and Core. The following scripts were run as part of **process_ARGO.py**

.. code-block:: python

    def unzip_and_organize_BGC():
        vs.makedir(argo_base_path + "BGC/")
        os.chdir(argo_base_path)
        for daac in tqdm(daac_list):
            os.system(
                f"""tar -xvf {daac}_bgc.tar.gz -C BGC/ --transform='s/.*\///' --wildcards --no-anchored '*_Sprof*'"""
            )


A similar function is then run for the Core files.



Argo Data Processing 
~~~~~~~~~~~~~~~~~~~~


Once the data collection is complete, we can start processing each argo netcdf file. To keep a record, we will create a record in the **process/** submodule of cmapdata. 



::

    
    ├── insitu
        ├── float
            ├── ARGO
                └── process_ARGO.py


Since BGC specific floats and Core floats contain different sets of variables, the processing has been split into two scripts. 


Detailed processing steps for the argo core and bgc can be found in process_ARGO_BGC_Sep2023.py and process_ARGO_Core_Sep2023. The processing is done with Pool from the multiprocessing library. The rough processing logic is outlined below:

1. Use the glob library to create a list of all netcdf files in the BGC directory. 
2. Iterate thorough list

  
   * import netcdf with xarray
   * decode binary xarray column data
   * export additional metadata cols for future unstructured metadata
   * drop unneeded metadata cols
   * checks no new columns are present this month   
   * convert xarray to dataframe and reset index   
   * add a depth specific column calculated from pressure and latitude using python seawater library
   * rename Space-Time columns
   * format datetime
   * drop any duplicates create by netcdf multilevel index
   * drop any invalid ST rows (rows missing time/lat/lon/depth)
   * sort by time/lat/lon/depth
   * add climatology columns
   * reorder columns and add any missing columns
   * replace any inf or nan string values with np.nan (will go to NULL in SQL server)
   * strips any whitespace from string col values
   * removes nan strings before setting data types
   * checks there is data in dataframe before exporting parquet file to /rep folder

Because the data will only live on the cluster, the fastest way to calculate stats for such a large dataset is to aggregate the values from each processed parquet file. Once all NetCDF files have been processed and parquet files saved to /rep, the following steps are completed:

1. Read each parquet file into a pandas dataframe
2. Query the dataframe to remove space and time data flagged as "bad" (_QC = 4)
3. Calculate min/max for each field with describe()
4. Append min/max values for each file to a stats dataframe
5. Export stats dataframe to /stats directory to be used during dataless ingestion


Before passing off for ingestion to the cluster, run through each processed parquet file to ensure the schema matches across all files. Past errors have been caused by empty parquet files and empty columns in one profile that are string data types in other profiles. Reading a parquet file into a dataframe and checking for matches is not suffient as pandas can read data types differently than the cluster will. The most successful checks to date were completed using pyarrow and pyarrow.parquet. 

.. warning::
   Any schema error in a single parquet file will cause the bulk ingestion to fail 

  
The last step for all process scripts is to copy the GitHub URL for the script to the /code folder in the vault. The example below calls the metadata.export_script_to_vault function and saves a text file named "process" in the dataset's code folder in the vault.

.. code-block:: python

    metadata.export_script_to_vault(tbl,'float_dir',f'process/insitu/float/ARGO/process_Argo_BGC_{date_string}.py','process.txt')


Bulk Ingestion to the Cluster
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Due to the size of the Argo datasets, and the monthly creation of a new dataset, both Argo Core and Argo BGC only live on the cluster. After all parquet files are created and checked for matching schemas, a bulk ingestion will be done to create the new tables on the cluster. 


Creating and Ingesting Metadata
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Once the bulk ingest is complete on the cluster, the metadata can be added. All dataset ingestion using general.py (see cruise ingestion for differences) pulls metadata from a folder named "final" within the validator folders in DropBox. For large datasets, you will still need to submit a template to the validator. In order to pass the validator tests you will need to include a minimum of one row of data in the data sheet. The values can all be placeholders, but must contain some value. 

If no new variables have been added, the data curation team does not need to re-run the QC API. Use the last month's metadata for Argo and update the **dataset_meta_data** tab with new values for dataset_short_name, dataset_long_name, dataset_version, dataset_release_date, and dataset_references. In the **vars_meta_data** tab, replace old references of dataset names in the variable keywords to current month. These keywords are usually assigned by the QC API.

After submitting through the validator, create a folder named final in **dropbox../Apps/Simons CMAP Web Data Sunmission/ARGO_BGC_Sep2023** and copy the submitted template into /final for ingestion.


To ingest the metadata only, you can use ingest/general.py 


Navigate to the ingest/ submodule of cmapdata. From there, run the following in the terminal. 

.. code-block:: python

   python general.py {table_name} {branch} {filename} {-S} {server} {-a} {data_server} {-i} {icon_filename} {-F} {-N}

* {**branch**}: Branch where dataset should be placed in Vault. Ex's: cruise, float, station, satellite, model, assimilation
* {**filename**}: Base file name in vault/staging/combined/. Ex.: 'global_diazotroph_nifH.xlsx'
* {**-S**}: Required flag for specifying server choice for metadata. Server name string follows flag. 
* {**server**}: Valid server name string.  Ex. "Rainier", "Mariana" or "Rossby"
* {**-a**}: Optional flag for specifying server name where data is located
* {**data_server**}: Valid server name string.  Ex. "Rainier", "Mariana", "Rossby", or "Cluster"
* {**-i**}: Optional flag for specifying icon name instead of creating a map thumbnail of the data
* {**icon_filename**}: Filename for icon in Github instead of creating a map thumbnail of data. Ex: argo_small.jpg
* {**-F**}: Optional flag for specifying a dataset has a valid depth column. Default value is 0
* {**-N**}: Optional flag for specifying a 'dataless' ingestion or a metadata only ingestion. 

An example string for the September 2023 BGC dataset is:

.. code-block:: python

    python general.py tblArgoBGC_REP_Sep2023 float 'ARGO_BGC_Sep2023.xlsx' -i 'argo_small.jpg' -S 'Rossby' -N -a 'cluster' -F 1


