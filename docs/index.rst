cmapdata
========

cmapdata is a collection of scripts organized into dataset collection (collect), 
dataset processing (process) and dataset ingestion (ingest). 
These docs should provide a primer on data flow into CMAP's databases along with 
suggested improvements. 







.. toctree::
    :maxdepth: 2
    :caption: Getting Started
    :hidden:

    getting_started/installation.rst
    getting_started/database_design.rst
    getting_started/compute_and_storage.rst
    getting_started/pitfalls.rst
    getting_started/cmap_website.rst 

.. toctree::
    :maxdepth: 2
    :caption: Dataset Ingestion Examples
    :hidden:

    data_ingestion/web_validator.rst
    data_ingestion/workflow.rst
    data_ingestion/indexing_strategies.rst
    data_ingestion/data_validation.rst    
    data_ingestion/continuous_ingestion.rst              
    data_ingestion/gallery_examples/user_submitted_dataset_walkthrough.rst
    data_ingestion/gallery_examples/outside_small_dataset_walkthrough.rst
    data_ingestion/gallery_examples/outside_large_dataset_walkthrough.rst
    data_ingestion/gallery_examples/geotraces_walkthrough.rst    
    data_ingestion/gallery_examples/eddy_walkthrough.rst      
    data_ingestion/gallery_examples/cruise_ingestion.rst


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
    API/API_cruise.rst
    API/API_data.rst
    API/API_DB.rst
    API/API_general.rst
    API/API_mapping.rst
    API/API_metadata.rst
    API/API_region_classifcation.rst
    API/API_SQL.rst
    API/API_stats.rst
    API/API_transfer.rst
    API/API_vault_structure.rst



    