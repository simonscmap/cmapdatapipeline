import pandas as pd
import numpy as np
import xarray as xr
from cmapingest import vault_structure as vs
from cmapingest import common as cmn
from cmapingest import data 

# xdf = xr.open_dataset(vs.collected_data + "EN532_EN538_flow_cytometry/FCM.nc")
# df = xdf.to_dataframe().reset_index(drop=True)
# df = df.applymap(lambda x: x.decode() if isinstance(x, bytes) else x)
# df.rename(columns={'ISO_DateTime_UTC':'time'}, inplace=True)
# df = df[['time','lat','lon','depth','cruise_id','cast','Syn_biomass_nMC','Syn_cells_per_ml','Syn_cells_per_ml_flag','picoEuk_biomass_nMC','picoEuk_cells_per_ml','picoEuk_cells_per_ml_flag','PE_Euk_biomass_nMC','PE_Euk_cells_per_ml','PE_Euk_cells_per_ml_flag','totnanoEuk_biomass_nMC','totnanoEuk_cells_per_ml','totnanoEuk_cells_per_ml_flag','cocco_biomass_nMC','cocco_cells_per_ml','cocco_cells_per_ml_flag','Prochlor_biomass_nMC','Prochlor_cells_per_ml','Prochlor_cells_per_ml_flag','total_FC_phyto_biomass_nMC','total_FC_phyto_cells_per_ml','total_FC_phyto_cells_per_ml_flag','total_het_bact_cells_per_ml','total_het_bact_cells_per_ml_flag','comment']]
# for col in ['Syn_biomass_nMC','Syn_cells_per_ml','Syn_cells_per_ml_flag','picoEuk_biomass_nMC','picoEuk_cells_per_ml','picoEuk_cells_per_ml_flag','PE_Euk_biomass_nMC','PE_Euk_cells_per_ml','PE_Euk_cells_per_ml_flag','totnanoEuk_biomass_nMC','totnanoEuk_cells_per_ml','totnanoEuk_cells_per_ml_flag','cocco_biomass_nMC','cocco_cells_per_ml','cocco_cells_per_ml_flag','Prochlor_biomass_nMC','Prochlor_cells_per_ml','Prochlor_cells_per_ml_flag','total_FC_phyto_biomass_nMC','total_FC_phyto_cells_per_ml','total_FC_phyto_cells_per_ml_flag','total_het_bact_cells_per_ml','total_het_bact_cells_per_ml_flag']:
#     df[col].values[df[col].values > 1.0*10**36] = np.nan
# df = data.mapTo180180(df)
# df = data.clean_data_df(df)
# df.to_excel(vs.staging + 'combined/' +"EN532_EN538_flow_cytometry.xlsx",index=False )



df = pd.read_excel("/home/nrhagen/CMAP Data Submission Dropbox/Simons CMAP/staging/combined/EN532_EN538_flow_cytometry.xlsx",sheet_name='data')


df1.to_excel("/home/nrhagen/CMAP Data Submission Dropbox/Simons CMAP/staging/combined/EN532_EN538_flow_cytometry_fixedlon.xlsx")


Flag                          Description
   0                   no quality control
   1                           good value
   2                  probably good value
   3                   probably bad value
   4                            bad value
   5                        changed value
   6                value below detection
   7                      value in excess
   8                   interpolated value
   9                        missing value
   A           value phenomenon uncertain
   Q  value below limit of quantification