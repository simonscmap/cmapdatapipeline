cmapdata
========

cmapdata is a collection of scripts organized into dataset collection (collect), 
dataset processing (process) and dataset ingestion (ingest). 
These docs should provide a primer on data flow into CMAP's databases along with 
suggested improvements. 




Todo
----


Explain BGC and insert strat 




Add examples to collect:

collect example for satellite or eddy etc. (from cmems/argo/eddy)

discuss a bit about multiple data sources. For example argo, release schedule, etc. Reading their data dictionary



Add complete walkthough(s) of a public dataset and a colaborator dataset.
From collection, processing, validation, ingestion.

If there is a part of process we can highlight, give an example of this.




In DB structure explain:


-creating a custom table
	-FG's and indices (time/lat/lon depth) climatology etc.
	-how these relate to preformance 

-docstrings




.. toctree::
    :maxdepth: 2
    :caption: Getting Started
    :hidden:

    getting_started/installation.rst
    getting_started/database_design.rst
    getting_started/compute_and_storage.rst
    getting_started/pitfalls.rst

.. toctree::
    :maxdepth: 2
    :caption: Dataset Ingestion Examples
    :hidden:

    data_ingestion/web_validator.rst
    data_ingestion/workflow.rst
    data_ingestion/indexing_strategies.rst
    data_ingestion/gallery_examples/user_submitted_dataset_walkthrough.rst
    data_ingestion/gallery_examples/outside_small_dataset_walkthrough.rst
    data_ingestion/gallery_examples/outside_large_dataset_walkthrough.rst


.. toctree::
    :maxdepth: 2
    :caption: Package Overview
    :hidden:

    subpackages/DB.rst
    subpackages/collect.rst
    subpackages/process.rst
    subpackages/ingest.rst

.. toctree::
    :maxdepth: 2
    :caption: Future Improvements
    :hidden:

    future/code_changes.rst

.. toctree::
    :maxdepth: 2
    :caption: API Ref
    :hidden:

    API/API_common.rst
    


    