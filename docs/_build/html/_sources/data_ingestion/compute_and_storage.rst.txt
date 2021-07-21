Compute Resources and Data Storage
==================================


The two main computers used in the ingestion pipeline are a Dell XPS 15 laptop and a newer Exxact workstation. 
For memory intensive and multi-core data processing, the workstation is a useful resource. 
It could either be used directly from the lab or ssh'ed into to run processing jobs. 
Using VS Code's **Remote-SSH** extension, you can connect and modify files over ssh without using command line editors. 
To start a connection, click on the bottom left green icon.
The ip address is for the workstation is 128.208.238.117

.. figure:: ../_static/ssh_connect.png
   :scale: 80 %
   :alt: VS code ssh 


Data Storage
------------

Both the web application and the data ingestion pipeline share storage over dropbox. With an unlimited account, 
we can use dropbox to store all our pre-DB data. In addition to dropbox, the **vault/** also is synced on the workstation under:
**~/data/CMAP Data Submission Dropbox/Simons CMAP/vault/**

.. note::
    Ideally this path should be changed to remove any spaces. The one potential hold-up is where the web app is writing files. 

Dropbox's CLI tools are installed on the workstation. Using the selective sync feature of dropbox, the **/vault** stored on disk can be synced with the cloud.
By reading/writing to disk, IO speeds for data processing should be improved.

If dropbox has stopped syncing, you can start the CLI by typing in terminal:

.. code-block:: console

   dropbox start
   dropbox status 

