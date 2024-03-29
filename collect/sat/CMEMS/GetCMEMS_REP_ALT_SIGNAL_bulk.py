
import os, sys
import copernicusmarine
sys.path.append("../../../ingest")
sys.path.append("ingest")
import vault_structure as vs
import credentials as cr


tbl = 'tblAltimetry_REP_Signal_cl1'
base_folder = f'{vs.satellite}{tbl}/raw/'
vs.leafStruc(vs.satellite+tbl)
output_dir = os.path.normpath(base_folder)


# https://data.marine.copernicus.eu/product/SEALEVEL_GLO_PHY_L4_MY_008_047/description

copernicusmarine.get( 
                    dataset_id="cmems_obs-sl_glo_phy-ssh_my_allsat-l4-duacs-0.25deg_P1D",
                    output_directory=output_dir,
                    username=cr.usr_cmem,
                    password=cr.psw_cmem,
                    no_directories=True,
                    show_outputnames=True,
                    overwrite_output_data=True,
                    force_download=True,
                    #filter=f"*{d.strftime("%Y%m%d")}.nc"
                    )






# cat = copernicusmarine.describe(contains=["cmems_obs-sl_glo_phy-ssh_my_allsat-l4-duacs-0.25deg_P1D"], 
#                                 include_datasets=True, 
#                                 include_description=False, 
#                                 include_keywords=False
#                                 )
# coor = cat['products'][0]['datasets'][0]['versions'][-1]['parts'][0]['services'][0]['variables'][0]['coordinates']