Code Changes
============


* Improve test coverage
* Update all .format() to fstring formatting
* Rename dropbox pathing to remove any spacing
* Add the ability to ingest to add DB's
* Metadata insert should all be captured in a SQL transaction, so that if a table insert fails, other tables and relations are rolled back.
* Move any r2r cruise collection remenant into collect/ 
* Build up cruise ingestion infrastructure
* Build gridded spatio-temporal dataset classifer 
* Finish up cmapsync and put on cronjob with report emailed




