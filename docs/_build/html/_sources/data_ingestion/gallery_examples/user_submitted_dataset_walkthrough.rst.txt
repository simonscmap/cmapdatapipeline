User Submitted Dataset Walkthrough
==================================


This example should walk you through the steps of ingesting a user submitted dataset into the database.

For this example, we are going to be using the dataset: **Falkor_2018 - 2018 SCOPE Falkor Cruise Water Column Data**


Removal of Previously Existing Dataset 
--------------------------------------

This dataset was an early submitted dataset and has recently been revised to being it up to line with the current CMAP data submission guidelines.
Because this dataset already exists in the database, we must first remove the old version.

To do this, we can use some of the functionallity in cmapdata/ingest/metadata.py


By calling this function **deleteCatalogTables(tableName, server)**, we can remove any metadata and data tables from a given server. 

.. warning::
    This function has drop privileges! Make sure you want to wipe the dataset metadata and table.



.. code-block:: console

   python metadata.py 

.. code-block:: python

   >>> deleteCatalogTables('tblFalkor_2018','Rainier')

Continue this function for any other existing servers. ex. 'Mariana','Rossby'


