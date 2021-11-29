import sys
import os
import urllib.request as req
from datetime import datetime
import glob
import requests

sys.path.append("../../../ingest")
from ingest import vault_structure as vs

vs.leafStruc(vs.float_dir + "tblWOA_2018_1deg_Climatology")
vs.leafStruc(vs.float_dir + "tblWOA_2018_qrtdeg_Climatology")

raw_save = vs.float_dir + "tblWOA_2018_qrtdeg_Climatology/raw"
base_url = 'https://www.ncei.noaa.gov/data/oceans/ncei/woa'


doc_pdfs = ['woa18_vol1.pdf','woa18_vol2.pdf','woa18_vol3.pdf','woa18_vol4.pdf','woa18documentation.pdf']
month_list = ['01','02','03','04','05','06','07','08','09','10','11','12']

d_dict = {}
d_dict['AOU'] = {}
d_dict['AOU']['timespan'] = 'all'
d_dict['AOU']['onedeg'] = True 
d_dict['AOU']['qdeg'] = False #'1.00' #woa18_all_A00_01.nc
d_dict['AOU']['prefix'] = 'A'
d_dict['conductivity'] = {}
d_dict['conductivity']['timespan'] = 'A5B7'
d_dict['conductivity']['onedeg'] = True
d_dict['conductivity']['qdeg'] = True # '0.25' woa18_A5B7_C00_04.nc
d_dict['conductivity']['prefix'] = 'C'
d_dict['density'] = {}
d_dict['density']['timespan'] = 'A5B7' #woa18_A5B7_I00_04.nc	
d_dict['density']['onedeg'] = True
d_dict['density']['qdeg'] = True
d_dict['density']['prefix'] = 'I'
d_dict['mld'] = {}
d_dict['mld']['timespan'] = 'A5B7' 
d_dict['mld']['onedeg'] = True #woa18_A5B7_M0200_01.nc	
d_dict['mld']['qdeg'] = True
d_dict['mld']['prefix'] = 'M'
d_dict['nitrate'] = {}
d_dict['nitrate']['timespan'] = 'all' 
d_dict['nitrate']['onedeg'] = True #woa18_all_n01_01.nc	
d_dict['nitrate']['qdeg'] = False
d_dict['nitrate']['prefix'] = 'n'
d_dict['o2sat'] = {}
d_dict['o2sat']['timespan'] = 'all' 
d_dict['o2sat']['onedeg'] = True #woa18_all_O00_01.nc	
d_dict['o2sat']['qdeg'] = False
d_dict['o2sat']['prefix'] = 'O'
d_dict['oxygen'] = {}
d_dict['oxygen']['timespan'] = 'all' 
d_dict['oxygen']['onedeg'] = True #woa18_all_o01_01.nc	
d_dict['oxygen']['qdeg'] = False
d_dict['oxygen']['prefix'] = 'o'
d_dict['phosphate'] = {}
d_dict['phosphate']['timespan'] = 'all' 
d_dict['phosphate']['onedeg'] = True #woa18_all_p01_01.nc
d_dict['phosphate']['qdeg'] = False
d_dict['phosphate']['prefix'] = 'p'
d_dict['salinity'] = {}
d_dict['salinity']['timespan'] = 'A5B7' 
d_dict['salinity']['onedeg'] = True #woa18_A5B7_s01_01.nc
d_dict['salinity']['qdeg'] = True
d_dict['salinity']['prefix'] = 's'
d_dict['silicate'] = {}
d_dict['silicate']['timespan'] = 'all' 
d_dict['silicate']['onedeg'] = True #woa18_all_i01_01.nc
d_dict['silicate']['qdeg'] = False
d_dict['silicate']['prefix'] = 'i'
d_dict['temperature'] = {}
d_dict['temperature']['timespan'] = 'A5B7' 
d_dict['temperature']['onedeg'] = True #woa18_A5B7_t01_01.nc
d_dict['temperature']['qdeg'] = True
d_dict['temperature']['prefix'] = 't'

for d in d_dict:
    for m in month_list:
        file_name = 'woa18_' + d_dict[d]['timespan'] +'_'+ d_dict[d]['prefix'] + m +'01.nc'
        file_name = f"woa18_{d_dict[d]['timespan']}_{d_dict[d]['prefix']}{m}01.nc"
        dl_url = os.path.join(base_url, d, d_dict[d]['timespan'], '1.00', file_name)
        wget_str = f'wget --no-check-certificate "{dl_url}" -O {vs.float_dir}/tblWOA_2018_1deg_Climatology/raw/{file_name}'


# wget_str = 'wget --no-check-certificate "https://docs.google.com/uc?export=download&id=179tQ24Wn_BD_HZiraQoyOI674-RIhQmm" -O C:/Scripts/CMAP/test_data/loc.tsv'
# os.system(wget_str)

