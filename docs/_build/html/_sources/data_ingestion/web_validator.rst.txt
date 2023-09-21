Web Validator
=============

Collaborator datasets are submitted through our online web validator. 
This system checks the integrity of the data, dataset metadata and variable metadata sheets, 
looking for missing information and invalid data types.
The status of the dataset is available to the curation team as well as the submitter on the data submission dashboard (https://simonscmap.com/datasubmission/admindashboard)

.. figure:: ../_static/admin_dashboard.png
   :scale: 70 %
   :alt: Web Validator Admin Dashboard




Once a collaborator has successfully submitted a dataset through the validator, 
the process will be handed over to the data curation team for additional QA/QC checks.
Data and metadata should be checked to make sure it conforms to the data submission guide. 
At this point it is also a good idea to do some sanity checks on the data itself. 
A common pitfall from data submitters is to mix up the sign of longitude, placing the dataset in the wrong ocean.
When the first check is complete, the dataset should have secondary independent QA/QC check.

.. figure:: ../_static/validator_meta_data_check.png
   :scale: 80 %
   :alt: Web Validator Dataset Metadata Check


Any edits of suggestions should be sent back to the submitter through the web validator. Additionally, 
any changes in the 'phase' should be updated through the dropdown menu in the dashboard.

.. figure:: ../_static/phase_dropdown.png
   :alt: Web Validator Phase Dropdown Menu



Once all the dataset changes are complete, the submitter should register a DOI for their dataset and send it over via the validator.

.. note::
   
   Datasets submitted through the web validator are currently stored in dropbox:
   'CMAP Data Submission Dropbox/Simons CMAP/Apps/Simons CMAP Web Data Submission/{dataset_name}/{dataset_name_datestring.xlsx}. Validator folders are currently based on dataset short name. This means it is possible a submitted dataset will be saved to a folder that already exists, if a user reuses a dataset short name. A fix for this should be implemented in the future by leveraging the unique ID in tblData_Submissions. 


