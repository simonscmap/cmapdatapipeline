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


Data Flow 
---------

There is currently a disconnect between where the web validator stores validated datasets (**Dropbox/Apps/<dataset_short_name>/<dataset_short_name_timestamp.xlsx>**)
vs where the data ingestion pipeline starts (**Dropbox/staging/combined/<dataset_name.xlsx>**). 
In addition, there is a directory **Dropbox/Collected_Data/../<table_name>/** that contains collected raw data from outside sources.

Ideally, it would be good to integrate the **collected_data/** directory within the **vault/** directory in a 'raw' or 'collected' leaf level. Additionally, the disconnect between staging/combined and Apps should be sorted out.


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




Synology NAS and Drobo Storage
------------------------------


Before storing data on Dropbox, two non-cloud storage methods were tried. Both the Drobo and Synology NAS are desktop size hard disk storage. Each contains ~40-50TB of disk space. 
There are limitations to each of these. The Drobo requires a connection through usb-c/thunderbolt. The Synology NAS can be accessed over the internet, ie ((Network Attached Storage).
They read/write speed for both is quite slow compared to the disks on the workstation. Perhaps one or both could be used as another backup?

