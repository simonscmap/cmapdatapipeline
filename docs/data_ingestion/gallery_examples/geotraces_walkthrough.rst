Geotraces Seawater Walkthrough
=================================

The Geotraces Seawater dataset is unique for two reasons:
1. The data table contains >1,024 columns which is the max allowed by SQL Server
2. The data producers requested the inclusion of unstructured metatdata


Geotraces Overview
----------------------

Geotraces IDP2021v2 contains 5 datasets: Seawater, Sensor, Precipitation, Aerosols, and Cryosphere. Only Seawater and Aerosols have updated data in v2, but the download link includes the v1 data for the remaining 3 datasets. Based on discussions with the Geotraces data team, there are no plans to update the remaining 3 datasets to v2. The second version includes additional cruises and fixes to multiple errors found in v1. The full list of fixes can be found here: https://www.bodc.ac.uk/geotraces/data/idp2021/documents/geotraces_idp2021v2_changes.pdf


Geotraces Data Collection 
~~~~~~~~~~~~~~~~~~~~~~~~~~

A zipped file of all five GEOTRACES datasets can be found on BODC: https://www.bodc.ac.uk/data/published_data_library/catalogue/10.5285/ff46f034-f47c-05f9-e053-6c86abc0dc7e/ 

The raw data will be saved in **dropbox/../vault/observation/in-situ/cruise/tblGeotraces_{dataset_name}/raw**
These files will need to be unzipped, saving to each dataset's raw folder. Copies of the included PDFs are moved to the /metadata folder. The script to download the data and unzip the raw data into each of the 5 dataset folders in the vault is here: ../cmapdata/collect/insitu/cruise/GEOTRACES/collectGeotraces_v2.py

In addition to a NetCDF for each dataset, Geotraces also includes an infos folder. Within that folder is a static html file for each cruise and variable combination that metadata was submitted for. These are scraped to populate the variable-level unstructured metadata (UM) which is described in detail below.



Geotraces Seawater Processing 
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Detailed processing steps for the Seawater dataset can be found in process/insitu/cruise/GEOTRACES/process_Geotraces_Seawater_IDP2021v2.py. The rough processing logic is outlined below:

   * import netcdf with xarray
   * loop through netcdf metadata and export to Geotraces_Seawater_Vars.xlsx to build out the vars_metadata sheet for validator 
   * decode binary xarray column data
   * change flag values from ASCII to int / string values based on Geotraces request
   * rename depth field as it contains nulls
   * build suggested SQL table based on netcdf data types
   * convert xarray to dataframe and reset index   
   * add a depth specific column calculated from pressure and latitude using python seawater library
   * rename Space-Time columns
   * format datetime
   * map longitude values from 0, 360 to -180, 180
   * drop any invalid ST rows (rows missing time/lat/lon/depth)
   * reorder columns and sort by time/lat/lon
   * create two temp tables in SQL
   * split dataframe in two, retaining the initial 17 metadata columns in both
   * ingest split dataframes into two temp tables
   * insert into final table with column set 
   * drop two temporary tables


Ingestion to the Database
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Geotraces_Seawater_IDP2021v2 includes 1186 variables which exceeds the limit SQL server allows to be held within a single table. Due to the nature of how the data is organized, much of the data is sparse, as not every cruise collected data for each variable. 

An example of the syntax to create a column set for sparse columns is shown below. Not all columns in the full create table script are show here.

.. code-block:: SQL

    
    CREATE TABLE [dbo].[tblGeotraces_Seawater_IDP2021v2](
        [time] [datetime] NOT NULL,
        [lat] [float] NOT NULL,
        [lon] [float] NOT NULL,
        [N_SAMPLES] [int] NULL,
        [N_STATIONS] [int] NULL,
        [cruise_id] [nvarchar](6) NULL,
        [station_id] [nvarchar](26) NULL,
        [station_type] [nvarchar](1) NULL,
        [Bot__Depth] [float] NULL,
        [Operator_s_Cruise_Name] [nvarchar](13) NULL,
        [Ship_Name] [nvarchar](20) NULL,
        [Period] [nvarchar](23) NULL,
        [Chief_Scientist] [nvarchar](31) NULL,
        [GEOTRACES_Scientist] [nvarchar](77) NULL,
        [Cruise_Aliases] [nvarchar](14) NULL,
        [Cruise_Information_Link] [nvarchar](75) NULL,
        [BODC_Cruise_Number] [float] NULL,
        [CSet] [xml] COLUMN_SET FOR ALL_SPARSE_COLUMNS  NULL,
        [CTDPRS_T_VALUE_SENSOR] [float] SPARSE  NULL,
        [CTDPRS_T_VALUE_SENSOR_qc] [nvarchar](1) SPARSE  NULL,
        [DEPTH_SENSOR] [float] SPARSE  NULL,
        [DEPTH_SENSOR_qc] [nvarchar](1) SPARSE  NULL,
        ...
        ) ON [FG3]


In order to successfully ingest into the column set, the data needs to be unioned from the two temp tables described above into the full table. BCP directly into the table with the column set fails.


Add New Cruises
^^^^^^^^^^^^^^^^^
The first version of Geotraces included data for multiple cruises that were not in CMAP. See the cruise ingestion for details on the template specific for adding cruise metadata and trajectories. Most US-based cruises can be found on the following websites: 

    * R2R: https://www.rvdata.us/browse_vessels
    * SAMOS: https://samos.coaps.fsu.edu/html/cruise_data_availability.php
    * BODC: https://www.bodc.ac.uk/data/bodc_database/nodb/search/
    * UNOLS: https://strs.unols.org/Public/Search/diu_ships.aspx

It is always preferred to use navigation or underway data for a cruise trajectory. In rare cases this data is not publicly available and sample locations can be used instead. Geotraces provides an API endpoint for sample locations that is "live (or close to) dynamically created data from Geotraces databases".

The collection script for the new cruises in v2 can be found here: cmapdata/collect/insitu/cruise/GEOTRACES/collectGeotraces_sample_locations_v2.py

The processing script for the the new cruises can be found here: cmapdata/process/insitu/cruise/GEOTRACES/process_Geotraces_trajectories_v2.py

The processing script creates the excel template needed for cruise trajectory ingestion. The metadata details for each cruise was pulled from IDP2021v2_Cruises.pdf, which is included in download provided by Geotraces. The final template is saved to the vault here: ../vault/r2r_cruise/{cruise_name}/raw/{cruise_name}_cruise_meta_nav_data.xlsx

An example ingestion string for a new cruise is: 

.. code-block:: console

    python general.py "SAG25_cruise_meta_nav_data.xlsx" -C SAG25 -S "Rossby" -v True

The {-v} flag tells the ingestion script to look in the raw folder of the vault, instead of pulling from Apps validator folder.


Creating and Ingesting Metadata
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The Geotraces NetCDFs contain metadata that can be used to build out the vars_meta_data sheet for validator submission. This includes variable short name (Geotraces requested we maintain their variable short names), long names, and flag values. Below is syntax used to loop through the variables and create an initial spreadsheet of provided metadata:

.. code-block:: python

    tbl = 'tblGeotraces_Seawater_IDP2021v2'
    meta_folder = f'{vs.cruise}{tbl}/metadata/'
    n = f'{vs.cruise}{tbl}/raw/GEOTRACES_IDP2021_Seawater_Discrete_Sample_Data_v2.nc' 
    x = xr.open_dataset(n)   

    d1 = {'var_name':[], 'std_name':[], 'long_name':[], 'dtype':[], 'units':[], 'comment':[], 'flag_val':[], 'flag_def':[]}
    df_varnames = pd.DataFrame(data=d1)

    for varname, da in x.data_vars.items():
        dtype = da.data.dtype
        if 'flag_values' in da.attrs.keys():
            fl_val = da.attrs['flag_values'].tolist()
            fl_def = da.attrs['flag_meanings']
        else:
            fl_val = None
            fl_def = None
        if 'long_name' in da.attrs.keys():
            long_name = da.attrs['long_name']
        else:
            long_name = None
        if 'standard_name' in da.attrs.keys():
            std_name = da.attrs['standard_name']
        else:
            std_name = None      
        if 'comment' in da.attrs.keys():
            comment = da.attrs['comment']
        else:
            comment = None   
        if 'units' in da.attrs.keys():
            units = da.attrs['units']
        else:
            units = None          

        d1 = {'var_name':[varname], 'std_name':[std_name], 'long_name':[long_name], 'dtype':[dtype], 'units':[units], 'comment':[comment], 'flag_val':[fl_val], 'flag_def':[fl_def]}
        temp_df = pd.DataFrame(data=d1)
        df_varnames = df_varnames.append(temp_df, ignore_index=True)

    df_varnames.to_excel(meta_folder +'Geotraces_Seawater_Vars.xlsx', index=False) 


With so many variable names, a significant amount of additional work is needed to clean up this initial spreadsheet. All variable long names should be title case. Specifically for Geotraces, there were many instances when the metadata in the NetCDF was truncated. Holes were filled in by referring to the relevant static HTML files included in the infos folder, later used to scrape for UM. 

All dataset ingestion using general.py (see cruise ingestion for differences) pulls metadata from a folder named "final" within the validator folders in DropBox. For large datasets, you will still need to submit a template to the validator. In order to pass the validator tests you will need to include a minimum of one row of data in the data sheet. The values can all be placeholders, but must contain some value. After the data curation team run the QC API to add the necessary keywords, they will include the finalized template to Apps/Geotraces_Seawater_IDP2021v2/final.


To ingest the metadata only, you can use ingest/general.py 


Navigate to the ingest/ submodule of cmapdata. From there, run the following in the terminal. Because the DOI for the Argo datasets is already in the references column in the **dataset_meta_data** tab of the metadata template, you do not need to use the {-d} flag with ingestion.

.. code-block:: python

   python general.py {table_name} {branch} {filename} {-S} {server} {-a} {data_server} {-i} {icon_filename} {-F} {-N}

* {**table_name**}: Table name for the dataset. Must start with prefix "tbl". Ex. tblArgoBGC_REP_Sep2023
* {**branch**}: Branch where dataset should be placed in Vault. Ex's: cruise, float, station, satellite, model, assimilation
* {**filename**}: Base file name in vault/staging/combined/. Ex.: 'global_diazotroph_nifH.xlsx'
* {**-S**}: Required flag for specifying server choice for metadata. Server name string follows flag. 
* {**server**}: Valid server name string.  Ex. "Rainier", "Mariana" or "Rossby"
* {**-i**}: Optional flag for specifying icon name instead of creating a map thumbnail of the data
* {**icon_filename**}: Filename for icon in Github instead of creating a map thumbnail of data. Ex: argo_small.jpg
* {**-F**}: Optional flag for specifying a dataset has a valid depth column. Default value is 0
* {**-N**}: Optional flag for specifying a 'dataless' ingestion or a metadata only ingestion. 

An example string for the September 2023 BGC dataset is:

.. code-block:: python

    python general.py tblGeotraces_Seawater_IDP2021v2 cruise 'Geotraces_Seawater_IDP2021v2.xlsx' -i 'tblGeotraces_Sensor.jpg' -S 'Rossby' -N 




Creating and Ingesting Unstructured Metadata
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The unstructured metadata (UM) for v1 of Geotraces Seawater was provided by Jesse McNichol. As there were additional cruises and variables in v2, a new set of UM needed to be scraped from the static HTML files included in the Geotraces data download. 

The file naming convention for the HTML files is {cruise_name}_{variable_short_name}.html

The files were scraped using BeautifulSoup. The cruise name and variable name were parsed from the file name. The Geotraces data team requested we include the BODC documentation links on methods for each variable and cruise. Additional links to cruise information are also included in the html files, but were not requested. These can be added to the future iteration of the cruise page if the new designs include UM for cruises.


For details on the unstructured metadata project see Jira the following tickets: (https://simonscmap.atlassian.net/browse/CMAP-563, https://simonscmap.atlassian.net/browse/CMAP-572). Each unstructured metadata object includes a value array and a description array. Values and descriptions are always arrays, even if empty or single values. Also, these arrays must always have identical lengths, even if descriptions are empty strings. Descriptions are meant to be human readable, short descriptions akin to alt-text for an image online. A single variable may have multiple entries in tblVariables_JSON_Metadata. An example of a variable-level unstructured metadata is:

.. code-block:: SQL

   {"cruise_names":{"values":["PS71"],"descriptions":["Operators Cruise Name"]},"meta_links":{"values":["https://www.bodc.ac.uk/data/documents/nodb/285421/"],"descriptions":["BODC documentation link"]}}


The script for creating UM for Geotraces Seawater is here: ..cmapdata/process/insitu/cruise/GEOTRACES/scrape_Geotraces_Seawater_UM.py


Only one entry was requested by the Geotraces data team for dataset level metadata: 

.. code-block:: SQL

   {"publication_link":{"values":["https://www.geotraces.org/geotraces-publications-database/"],"descriptions":["Link to database of GEOTRACES publications"]}}


The dataset-level UM is ingested in the scrape script using DB.toSQLpandas(). The variable-level UM is ingested using DB.toSQLbcp_wrapper(), though requires a final update to fix BCP including additional quotes, causing the JSON to no longer be valid:

.. code-block:: SQL

    qry = """UPDATE tblVariables_JSON_Metadata SET json_metadata = replace(replace(replace(json_metadata,'""','"'), '"{','{'), '}"','}')"""
    DB.DB_modify(qry, server)


You can check for invalid JSON in tblVariables_JSON_Metadata and tblDatasets_JSON_Metadata with the following:

.. code-block:: SQL

    SELECT * FROM tblVariables_JSON_Metadata WHERE ISJSON(JSON_Metadata) = 0