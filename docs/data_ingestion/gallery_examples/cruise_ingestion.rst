Ingesting Cruise Metdata and Trajectory
=======================================


CMAP contains cruise trajectories and metadata stored in seperate tables (tblCruise and tblCruise_Trajectory). 
These allow us to visualize cruise tracks on map, colocalize datasets with cruise tracks and links datasets to specific cruises. 

A cruise ingestion template should contain two sheets. One for cruise metadata and another for cruise trajectory. 

Metadata Sheet 
--------------

+--------------------------------------+------------------------------------+-----------------------------------------+------------------------------------------------+-------------------------------------------------+
| Nickname                             | Name                               | Ship_Name                               | Chief_Name                                     | Cruise_Series                                   |
+--------------------------------------+------------------------------------+-----------------------------------------+------------------------------------------------+-------------------------------------------------+
| < Ship Nickname (ex. Gradients 3) >  | < UNOLS Cruise Name (ex. KM1906) > | < Official Ship Name (ex. Kilo Moana) > | < Chief Scientist Name (ex. Ginger Armbrust) > | < opt. Cruise Series (ex. Gradients/HOT/etc.) > |
+--------------------------------------+------------------------------------+-----------------------------------------+------------------------------------------------+-------------------------------------------------+

The metadata sheet contains cruise metadata that will populate tblCruise. The ST bounds will be filled in with the ingestion process.

Trajectory Sheet
----------------


+--------------------------------------------------------------------------------+--------------------------------------------------------------------------------+----------------------------------------------------------------------------------+
| time                                                                           | lat                                                                            | lon                                                                              |
+--------------------------------------------------------------------------------+--------------------------------------------------------------------------------+----------------------------------------------------------------------------------+
| < Format  %Y-%m-%dT%H:%M:%S,  Time-Zone:  UTC,  example: 2014-02-28T14:25:55 > | < Format: Decimal (not military grid system), Unit: degree, Range: [-90, 90] > | < Format: Decimal (not military grid system), Unit: degree, Range: [-180, 180] > |
+--------------------------------------------------------------------------------+--------------------------------------------------------------------------------+----------------------------------------------------------------------------------+

The trajectory sheet contains ST information of the cruise. This should have enough points to give an accurate cruise trajectory, without having too high a sampling interval. A good target might be minute scale. 



Ingesting Cruise Templates
--------------------------

Similar to how datasets are ingested into CMAP, we can use the functionallity in the **ingest** subpackage. 

Completed crusie templates should start the ingestion process in '/CMAP Data Submission Dropbox/Simons CMAP/vault/r2r_cruise/{cruise_name}/{cruise_name_template.xlsx}'



Using ingest/general.py, you can pass command line arguments to specify a cruise ingestion as well as a server.

Navigate to the ingest/ submodule of cmapdata. From there, run the following in the terminal. 

.. code-block:: python

   python general.py {filename} {-C} {cruise_name} {-S} {server}

* {**filename**}: Base file name in vault/staging/combined/. Ex.: 'TN278_cruise_meta_nav_data.xlsx'
* {**-C**}: Flag indicating for cruise ingestion. Follow with cruise_name. 
* {**cruise_name**}: String for official (UNOLS) cruise name Ex. TN278
* {**-S**}: Required flag for specifying server choice. Server name string follows flag. 
* {**server**}: Valid server name string.  Ex. "Rainier", "Mariana" or "Rossby"
* {**-v**}: Optional flag denoting metadata template is present in the raw folder of the vault
* {**in_vault**}: If True, pulls template from vault. Default is False, which pulls from /final folder in Apps folder created after submitting to the validator

An example string would be:

.. code-block:: python

    python general.py 'TN278_cruise_meta_nav_data.xlsx' -C TN278 -S "Rainier" -v True


Behind the scenes, the script is doing:

 1. parsing the user supplied arguments. 
 2. Splitting the data template into cruise_metadata and cruise_trajectory files. 
 3. Importing into memory the cruise_metadata and cruise_trajectory sheets as pandas dataframes. 
 4. Filling in the ST bounds for the cruise_metdata dataframe with min/max's from the trajectory dataframe.
 5. Inserting the metadata dataframe into tblCruise. 
 6. Inserting the trajectory dataframe into tblCruise_Trajectory. 
 7. Using the trajectory dataframe to classify the cruise by ocean region(s).
 8. Inserting the cruise_ID and region_ID's into tblCruise_Regions.






