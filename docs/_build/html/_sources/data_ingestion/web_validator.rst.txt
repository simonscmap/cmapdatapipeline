Web Validator
=============

Colaborator datasets are submitted through our online web validator. 
This system checks the integrity of the data, dataset metaddata and variable metadata sheets, 
looking for missing information and invalid data types.
The status of the dataset is available to the curation team as well as the submitter on the data submission dashboard (https://simonscmap.com/datasubmission/admindashboard)

## insert image of validator dashboard and admin status dashboard. ##

Once a colaborator has succesfully submitted a dataset through the validator, 
the process will be handed over to the data curation team for additional QA/QC checks.
Data and metadata should be checked to make sure it conforms to the data submission guide. 
At this point it is also a good idea to do some sanity checks on the data itself. 
A common pitfall from data submitters is to mix up the sign of longitude, placing the dataset in the wrong ocean.
When the first check is complete, the dataset should have secondary independent QA/QC check.

Any edits of suggestions should be sent back to the submitter through the web validator. Addtionally, 
any changes in the 'phase' should be updated through the dropdown menu in the dashboard.

Once all the dataset changes are complete, the submitter should register a DOI for their dataset and send it over via the validator.

.. note::
   Datasets submitted through the web validator are currently stored in dropbox:
   'CMAP Data Submission Dropbox/Simons CMAP/Apps/Simons CMAP Web Data Submission/{dataset_name}/{dataset_name_datestring.xlsx}
   The data ingest pipeline starts at 'CMAP Data Submission Dropbox/Simons CMAP/staging/combined/{dataset_name}.xlsx
   This path disconnect should be remedied.

