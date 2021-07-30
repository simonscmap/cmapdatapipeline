Installation and Setup
======================


The github repository **cmapdata** is on the Simons CMAP github organization page. 
To clone the repository navigate to/create the directory location where you want the cmapdata repository to live, enter in terminal:

.. code-block:: console

   git clone git@github.com:simonscmap/cmapdata.git

The DB repository that houses the SQL table creation scripts can be retrieved with:

.. code-block:: console

   git clone git@github.com:simonscmap/DB.git





Documentation 
-------------

These docs were built using Sphinx and written in re-structured text. With sphinx installed (via pip), you can build the docs by running the command:

.. code-block:: console

   make html

in the directory **docs/**

This will produce a build in **docs/**

To view these locally, you can open up the index.html with chrome/firefox etc. 


These docs are hosted on readthedocs.org. You can set up an account with your github and add a webhook so that the Documentation builds whenever you push to github. 




.. note::
   The auto generated API reference does not appear on readthedocs and will only appear locally. 