Outside Large Dataset Walkthrough
=================================

Outside large datasets require similar collecting/processing methods to outside small datasets, however the ingestion strategy can differ. 
In this section, we will outline examples of collecting, processing and ingesting two large outside datasets.



Argo Float Walkthrough
----------------------

The ARGO float array is a multi-country program to deploy profiling floats across the global ocean. These floats provide a 3D insitu ocean record. 
The CORE argo floats provide physical ocean parameters, while the BGC (Biogeochemical) specific floats provide Biogeochemical specific variables (nutrients, radiation etc.).


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

    june_10_link = "https://www.seanoe.org/data/00311/42182/data/85023.tar.gz"

    output_dir = vs.collected_data + "insitu/float/ARGO/"

    def wget_file(fpath, output_dir):
        os.system("wget " + fpath + " -P " + output_dir)

    wget_file(june_10_link, output_dir)


This file will need to be unzipped, either using python or bash. The output files will be put in **dropbox/../collected_data/insitu/float/ARGO/**

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
        # 1559 files
        vs.makedir(argo_base_path + "BGC/")
        os.chdir(argo_base_path)
        for daac in tqdm(daac_list):
            os.system(
                f"""tar -xvf {daac}_bgc.tar.gz -C BGC/ --transform='s/.*\///' --wildcards --no-anchored '*_Sprof*'"""
            )


A similar function was fun for Core files. Details can be found in **process_ARGO.py**.



Argo Data Processing 
~~~~~~~~~~~~~~~~~~~~


Once the data collection is complete, we can start processing each argo netcdf file. To keep a record, we will create a record in the **process/** submodule of cmapdata. 



::

    
    ├── insitu
        ├── float
            ├── ARGO
                └── process_ARGO.py


Since BGC specific floats and Core floats contain different sets of variables, the processing has been split into two functions. 


Creating Tables and Indicies
^^^^^^^^^^^^^^^^^^^^^^^^^^^^


General SQL table creation and indexing strategies can be found in the **table creation and indexing** section of these docs. 
Tables and indexes have to be created on all databases before data ingestion. 


Subset Testing 
^^^^^^^^^^^^^^

When ingesting large datasets such as models or satellite products, it can be helpful to ingest a small subset of the entire dataset, then ingest the metadata. 
This should allow you to test visualizing and retrieving the dataset before committing to GB to TB of database space. 

General Processing Steps
^^^^^^^^^^^^^^^^^^^^^^^^

Detailed processing steps for the argo core and bgc can be found in process_ARGO.py. The rough processing logic is outlined below:

1. Use the glob library to create a list of all netcdf files in the BGC directory. 
2. Iterate thorough list

  
   * import netcdf with xarray
   * decode binary xarray column data
   * convert xarray to dataframe and reset index
   * drop unneeded metadata cols
   * add a depth specific column from decibars pressure column
   * rename Space-Time columns
   * format datetime
   * drop any duplicates create by netcdf multilevel index
   * drop any invalid ST rows (rows missing time/lat/lon/depth)
   * sort by time/lat/lon/depth
   * add climatology columns
   * reorder columns and add any missing columns
   * replace any inf or nan string values with np.nan (will go to NULL in SQL server)
   * strips any whitespace from string col values
   * downcasts data if possible 
   * builds summary stats (opt. can use db stats call functionality instead)
   * uses the BCP utility to insert dataframe into database table 
   * exports df to csv in vault





Ingesting Metadata
^^^^^^^^^^^^^^^^^^

-general flags, spoofing data for validator






