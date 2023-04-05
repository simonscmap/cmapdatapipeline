import sys
import os
from tqdm import tqdm 

sys.path.append("../../../ingest")
from ingest import vault_structure as vs


tbl_names = ['tblWOA_2018_1deg_Climatology', 'tblWOA_2018_qrtdeg_Climatology']
for tbl in tbl_names:
    vs.leafStruc(vs.float_dir + tbl)
#vs.leafStruc(vs.float_dir + "tblWOA_2018_qrtdeg_Climatology")


base_url = 'https://www.ncei.noaa.gov/data/oceans/ncei/woa'


doc_pdfs = ['woa18_vol1.pdf','woa18_vol2.pdf','woa18_vol3.pdf','woa18_vol4.pdf','woa18documentation.pdf']
month_list = ['01','02','03','04','05','06','07','08','09','10','11','12']


d_dict = {}
d_dict['AOU'] = {}
d_dict['AOU']['timespan'] = 'all'
d_dict['AOU']['qdeg'] = False #'1.00' #woa18_all_A00_01.nc
d_dict['AOU']['prefix'] = 'A'
d_dict['conductivity'] = {}
d_dict['conductivity']['timespan'] = 'A5B7'
d_dict['conductivity']['qdeg'] = True # '0.25' woa18_A5B7_C00_04.nc
d_dict['conductivity']['prefix'] = 'C'
d_dict['density'] = {}
d_dict['density']['timespan'] = 'A5B7' #woa18_A5B7_I00_04.nc	
d_dict['density']['qdeg'] = True
d_dict['density']['prefix'] = 'I'
d_dict['mld'] = {}
d_dict['mld']['timespan'] = 'A5B7' 
d_dict['mld']['qdeg'] = True #woa18_A5B7_M0200_01.nc	
d_dict['mld']['prefix'] = 'M02'
d_dict['nitrate'] = {}
d_dict['nitrate']['timespan'] = 'all' 
d_dict['nitrate']['qdeg'] = False #woa18_all_n01_01.nc	
d_dict['nitrate']['prefix'] = 'n'
d_dict['o2sat'] = {}
d_dict['o2sat']['timespan'] = 'all' 
d_dict['o2sat']['qdeg'] = False #woa18_all_O00_01.nc
d_dict['o2sat']['prefix'] = 'O'
d_dict['oxygen'] = {}
d_dict['oxygen']['timespan'] = 'all' 
d_dict['oxygen']['qdeg'] = False #woa18_all_o01_01.nc	
d_dict['oxygen']['prefix'] = 'o'
d_dict['phosphate'] = {}
d_dict['phosphate']['timespan'] = 'all' 
d_dict['phosphate']['qdeg'] = False #woa18_all_p01_01.nc
d_dict['phosphate']['prefix'] = 'p'
d_dict['salinity'] = {}
d_dict['salinity']['timespan'] = 'A5B7' 
d_dict['salinity']['qdeg'] = True #woa18_A5B7_s01_01.nc
d_dict['salinity']['prefix'] = 's'
d_dict['silicate'] = {}
d_dict['silicate']['timespan'] = 'all' 
d_dict['silicate']['qdeg'] = False #woa18_all_i01_01.nc
d_dict['silicate']['prefix'] = 'i'
d_dict['temperature'] = {}
d_dict['temperature']['timespan'] = 'A5B7' 
d_dict['temperature']['qdeg'] = True #woa18_A5B7_t01_01.nc
d_dict['temperature']['prefix'] = 't'

#d='temperature'
#m='01'
for d in tqdm(d_dict):
    for m in month_list:
        file_name = f"woa18_{d_dict[d]['timespan']}_{d_dict[d]['prefix']}{m}_01.nc"
        dl_url = os.path.join(base_url, d, d_dict[d]['timespan'], '1.00', file_name)
        save_path = f'{vs.float_dir}tblWOA_2018_1deg_Climatology/raw/{file_name}'
        wget_str = f'wget --no-check-certificate "{dl_url}" -O "{save_path}"'
        os.system(wget_str)
        if d_dict[d]['qdeg']:
            file_name_q = f"woa18_{d_dict[d]['timespan']}_{d_dict[d]['prefix']}{m}_04.nc"
            dl_url_q = os.path.join(base_url, d, d_dict[d]['timespan'], '0.25', file_name_q)
            save_path_q = f'{vs.float_dir}tblWOA_2018_qrtdeg_Climatology/raw/{file_name_q}'
            wget_str_q = f'wget --no-check-certificate "{dl_url_q}" -O "{save_path_q}"'
            os.system(wget_str_q)

for tbl in tbl_names:
    for doc in doc_pdfs:
        dl_url_doc = os.path.join(base_url, 'DOC', doc)
        save_path_doc = f'{vs.float_dir}{tbl}/doc/{doc}'
        wget_str_doc = f'wget --no-check-certificate "{dl_url_doc}" -O "{save_path_doc}"'
        os.system(wget_str_doc)


