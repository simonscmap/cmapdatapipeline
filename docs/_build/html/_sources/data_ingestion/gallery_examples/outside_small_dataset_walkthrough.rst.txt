Outside Small Dataset Walkthrough
=================================

Datasets from outside sources do not follow CMAPs data guidelines and vary drastically in quality of metadata. 
Many dataset have variables that are not self explanatory and may have data flags or conventions that will take some work to understand. 
One of the best resources is the rest of the Armbrust lab, which has tons of oceanography domain knowledge and experience working with these datasets. 
Additionally, contacting the dataset owner/provider can also provide insight. 

In this example, we are going to walkthrough collecting, processing and ingesting a small outside dataset into CMAP.

Recently, we wanted to add multiple datasets from the CMORE-BULA cruise that traveled from Hawaii south to Fiji. 
These data were available on the HOT (Hawaii Ocean Time-series) FTP site. 

Collecting a small dataset from an FTP site using wget 
------------------------------------------------------

From the UH CMORE-BULA site: https://hahana.soest.hawaii.edu/cmorebula/cmorebula.html there are links for data access. 
Each of these leads you to an FTP site. 


.. figure:: ../../_static/cmore_bula_data_link.png
   :scale: 80 %
   :alt: CMORE-BULA

The FTP site for the CMORE-BULA CTD dataset contains 46 individual CTD casts. 
Instead of downloading them all by hand, we can use some functionality from the wget command line tool. 

.. figure:: ../../_static/cmore_bula_ctd_ftp.png
   :scale: 80 %
   :alt: CMORE-BULA


To keep a record of dataset processing, it is a good idea to create a collect_{dataset_name}.py script in the collect/ submodule of cmapdata. 
For this example, we have created a directory with the tree structure: 

::

    
    ├── insitu
        ├── cruise
            ├── misc_cruise
                ├── KM0704_CMORE_BULA
                    └── collect_KM0704_CMORE_BULA.py



For each dataset in this cruise (CTD, underway, underway sample, bottle  & wind), we can collect any files using wget. 

For CTD we can write a download function using wget and pass the FTP link for the CTD files. Using wget, we can specify the output directory.

.. code-block:: python

    odir = vs.cruise + "tblKM0704_CMORE_BULA/raw/"

    def download_KM0704_data(outputdir, download_link):
        wget_str = f"""wget -P '{outputdir}' -np -R "'index.html*" robots=off -nH --cut-dirs 8 -r  {download_link}"""
        os.system(wget_str)

    # #download ctd
    download_KM0704_data(
        odir + "CTD/", "https://hahana.soest.hawaii.edu/FTP/cmore/ctd/bula1/"
    )


For all the datasets in CMORE-BULA, this process would need to be repeated. 



Processing a small dataset
--------------------------

Now that the CTD files have been collected, we can begin processing them. 
Once again, we are going to create a dataset/collection specific file for a record. In this example, we will call it process_KM0704_CMORE_BULA.py
This should be placed in the **process/** submodule of cmapdata. 

::

    
    ├── insitu
        ├── cruise
            ├── misc_cruise
                ├── KM0704_CMORE_BULA
                    └── process_KM0704_CMORE_BULA.py


The full CTD processing steps can be found in this file, but in summary they are:

 1. Use the glob library to create a list of all .ctd files collected previously. 
 2. Iterate thorough list
   
    * read csv into pandas dataframe 
    * replace '-9' missing values with np.nan 
    * extract station,cast,cast direction and num_observations from filename using string splitting. 
    * create new columns of variable specific quality flags out of strange combined flag column. 
    * drop unneeded columns
    * append data 
  
 3. concatenate cleaned data into Pandas Dataframe 

Other datasets in the CMORE-BULA cruise required additional processing. Some required time and depth formatting. 
Others were missing spatio-temporal coordinates, which had to be collected using spatial or station/cast # joins to the other datasets. 



Once all the data are processed, the can be exported to dropbox/../vault/observation/in-situ/cruise/tblKM0704_CMORE_BULA/raw where dataset and variable metadata sheets can be added. Once these are complete, the dataset should run through the validator. 
From here, follow the steps outlined in the **user submitted datasets walkthrough** section to ingest the data into the database. 








