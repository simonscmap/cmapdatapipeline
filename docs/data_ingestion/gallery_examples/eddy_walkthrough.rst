Mesoscale Eddy Data Walkthrough
=================================


Mesoscale Eddy Version History
----------------------

The first version of Mesoscale Eddy data in CMAP was v2.0 provided by AVISO. Upon the release of v3.2, the version in CMAP was no longer being updated by AVISO and recommended to discontinue the use of v2 (https://ostst.aviso.altimetry.fr/fileadmin/user_upload/OSTST2022/Presentations/SC32022-A_New_Global_Mesoscale_Eddy_Trajectory_Atlas_Derived_from_Altimetry___Presentation_and_Future_Evolutions.pdf). 

A video descibing the differences in versions can be found here: https://www.youtube.com/watch?v=4Vs3ZJNMViw

When data providers reprocess historic data so that it no longer aligns with data already ingested in CMAP, a new dataset for CMAP needs to be created. In this case, since AVISO provided a new name to their dataset (v3.2), the change did not fall under CMAP's internal change log naming convention (see continuous ingestion section for more details). AVISO's documentation notes updates are done "multiple times a year", and they create a new DOI for each temporal extension. They don't change their naming convention with temporal extensions, so any updates will result in a new change log table name.

While v2.0 consisted of one dataset, v3.2 included two datasets (allsat/twosat) are different enough such that AVISO decided to create two releases. Within each of these two datasets there is data for three types of eddies (long, short, untracked) describing the lifetime of an eddy, each split into two NetCDF files (cyclonic, anticyclonic). The 12 provided NetCDF files were ingested into 6 datasets described below.



Mesoscale Eddy Data Collection 
~~~~~~~~~~~~~~~~~~~~~~~~~~

AVISO provides data download through their FTP service which requires a login. 

In order to keep all raw data in their respective folders, the first step is to create the 6 new dataset folders in the vault:

.. code-block:: python

    from ingest import vault_structure as vs

    tbl = 'tblMesoscale_Eddy_'
    sat_list = ['twosat','allsat']
    ln_list = ['untracked', 'short', 'long']
    for sat in sat_list:
        for ln in ln_list:
            vs.leafStruc(vs.satellite+tbl+sat+'s_'+ln)


Each individual NetCDF is then downloaded into the corresponding dataset's /raw folder in the vault:

.. code-block:: python

    cyc_list = ['Cyclonic', 'Anticyclonic']
    for sat in sat_list:
        for ln in ln_list:
            base_folder = f'{vs.satellite}{tbl}{sat}s_{ln}/raw/'
            print(base_folder)
            os.chdir(base_folder)
            for cyc in cyc_list:
                url_base = f"ftp://dharing@uw.edu:NBxOn4@ftp-access.aviso.altimetry.fr/value-added/eddy-trajectory/delayed-time/META3.1exp_DT_{sat}/META3.1exp_DT_{sat}_{cyc}_{ln}_19930101_20200307.nc"
                urllib.request.urlretrieve(url_base, base_folder+f'META3.1exp_DT_{sat}_{cyc}_{ln}_19930101_20200307.nc')




Mesoscale Eddy Processing 
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Each NetCDF files includes 50 samples per observation. The lat and lon of the centroid don't change per sample and most have the same values within an observation. These variables have small differences across samples in an observation: effective_contour_lat, effective_contour_longitude, speed_contour_latitude, speed_contour_longitude, uavg_profile. Due to duplication of lat/lon and small varations across a subset of variables, each dataset was subset for sample = 0. 

 The processing logic for each NetCDF is outlined below:

   * import netcdf with xarray, selecting where NbSample = 0
   * loop through netcdf metadata and export to AVISO_Eddy32_allsats_Vars.xlsx to build out the vars_metadata sheet for validator 
   * call SQL.full_SQL_suggestion_build() to create SQL tables
   * loop through both NetCDFs per dataset
   * rename lat and lon, drop obs field
   * add column eddy_polarity based on Anticyclonic vs Cyclonic file
   * add climatology fields
   * map longitude values from 0, 360 to -180, 180
   * ingest with DB.toSQLbcp_wrapper()



Creating and Ingesting Metadata
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

All dataset ingestion using general.py (see cruise ingestion for differences) pulls metadata from a folder named "final" within the validator folders in DropBox. For large datasets, you will still need to submit a template to the validator. In order to pass the validator tests you will need to include a minimum of one row of data in the data sheet. The values can all be placeholders, but must contain some value. After the data curation team run the QC API to add the necessary keywords, they will include the finalized template to Apps/Mesoscale_Eddy_*/final.


To ingest the metadata only, you can use ingest/general.py 


Navigate to the ingest/ submodule of cmapdata. From there, run the following in the terminal. Because the DOI for the Mesoscale Eddy datasets is already in the references column in the **dataset_meta_data** tab of the metadata template, you do not need to use the {-d} flag with ingestion.

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
* {**-v**}: Optional flag denoting if metadata template is present in the raw folder of the vault
* {**in_vault**}: If True, pulls template from vault. Default is False, which pulls from /final folder in Apps folder created after submitting to the validator


These datasets were ingested before the QC API was written. The use of the vault flag for datasets should no longer be used as all metadata should go through the API, at minimum for the automatic addition of all the keywords. 

An example string used for a Mesoscale Eddy dataset is:

.. code-block:: python

    python general.py tblMesoscale_Eddy_allsats_long satellite 'tblMesoscale_Eddy_allsats_long.xlsx' -i 'chelton_aviso_eddy.png' -S 'Rossby' -v True -N 



