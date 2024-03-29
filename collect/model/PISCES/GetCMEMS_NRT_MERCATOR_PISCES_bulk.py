import sys
import os
from tqdm import tqdm
import datetime
import copernicusmarine

sys.path.append("../../../ingest")
sys.path.append("ingest")

import vault_structure as vs
import credentials as cr


# https://data.marine.copernicus.eu/product/GLOBAL_ANALYSISFORECAST_BGC_001_028/description


##### pick which dataset to download

# tbl = "tblPisces_Forecast_Nutrien"
# dataset_id = "cmems_mod_glo_bgc-nut_anfc_0.25deg_P1D-m"

# tbl = "tblPisces_Forecast_Bio"
# dataset_id = "cmems_mod_glo_bgc-bio_anfc_0.25deg_P1D-m"

# tbl = "tblPisces_Forecast_Car"
# dataset_id = "cmems_mod_glo_bgc-car_anfc_0.25deg_P1D-m"

# tbl = "tblPisces_Forecast_Co2"
# dataset_id = "cmems_mod_glo_bgc-co2_anfc_0.25deg_P1D-m"

# tbl = "tblPisces_Forecast_Optics"
# dataset_id = "cmems_mod_glo_bgc-optics_anfc_0.25deg_P1D-m"

tbl = "tblPisces_Forecast_Pft"
dataset_id = "cmems_mod_glo_bgc-pft_anfc_0.25deg_P1D-m"

###################



base_folder = f'{vs.model}{tbl}/raw/'
vs.leafStruc(vs.model+tbl)

copernicusmarine.get( 
                    dataset_id=dataset_id, 
                    output_directory=os.path.normpath(base_folder),
                    username=cr.usr_cmem,
                    password=cr.psw_cmem,
                    no_directories=True,
                    show_outputnames=True,
                    overwrite_output_data=True,
                    force_download=True,
                    #filter=f"*{d.strftime("%Y%m%d")}.nc"
                    )



# ds = xr.open_dataset()
# ds
# ds.attrs
# ds.data_vars
# ds.chl