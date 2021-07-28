Pitfalls
========



* Metadata ingestion fails part way
* Mariana seems to have some auto_incriment ID issues. These should be synced with Rainier/Rossby.
* kdswap0 issue






Todo
----


Explain BGC and insert strat 




Add examples to collect:

collect example for satellite or eddy etc. (from cmems/argo/eddy)

discuss a bit about multiple data sources. For example argo, release schedule, etc. Reading their data dictionary



Add complete walkthough(s) of a public dataset and a colaborator dataset.
From collection, processing, validation, ingestion.

If there is a part of process we can highlight, give an example of this.




Why argo_core in vault is missing metadata, wasn't split?


remove any server default = eg rainer

**Be very careful of pushing to Rainier! Not a good place to TEST**

In future not that initial ingestion server should not be main/default server SOT




explain the role of apps vs vault vs collected data. how data moves









In DB structure explain:


-creating a custom table
	-FG's and indices (time/lat/lon depth) climatology etc.
	-how these relate to preformance 

-docstrings