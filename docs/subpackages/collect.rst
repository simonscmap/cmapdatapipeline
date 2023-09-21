collect
=======


This submodule contains various scripts for collecting outside data. Methods include FTP, curl, wget and others.
Scripts are usually one-off and are a record of the method used to collect the data. 
They are organized hierarchically in a similar fashion to **/vault**.


::

    ├── assimilation
    ├── model
    ├── observation
        ├── in-situ
        │   ├── cruise
        |   |   |── cruise_name
        |   |       |── collect_{cruise_name}.py
        │   ├── drifter
        │   ├── float        
        │   ├── mixed          
        │   └── station
        └── remote
            └── satellite



collection strategies
---------------------

The oceanography data in CMAP comes from multiple sources which vary in the amount of data processing required and available metadata.
The first step of ingesting a dataset from an outside source into CMAP is collecting the data. 
This generally starts with a python collection script. This both servers to collect the data as well as leave a record. 



FTP Servers 
-----------
Some datasets, especially when there are multiple files, are available over FTP servers. 
To retrieve this data, you can either use some GUI FTP application such as FileZilla or a command line utility such as wget or curl. 
Examples of using wget are available in some of the collect.py scripts. Some FTP sites required registrations and username/passwords. 


.. figure:: ../_static/hot_ftp_site.png
   :scale: 80 %
   :alt: HOT FTP



Zipped File Links
-----------------
Some data providers such as Pangea provide datasets and metadata as zipped files. While this is very convenient, 
it is a good idea to still create a collect_datasetname.py file with the zipped file link. 



Webscrapping
------------

Some of the cruise trajectory and metadata was initially collected from R2R (Rolling Deck to Repository). Generally, webscraping is only a last resort.

