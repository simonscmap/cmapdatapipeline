import sys
import os
import pandas as pd
import glob
from bs4 import BeautifulSoup


sys.path.append("ingest")
import vault_structure as vs
import common as cmn
import DB

tbl = 'tblGeotraces_Seawater_IDP2021v2'
scrape_folder = f'{vs.cruise}{tbl}/raw/infos/'

flist = glob.glob(scrape_folder+'*.html')

### Variable names changed from v1 to v2
d1 = {'cruise':[], 'var_name':[], 'url':[], 'url_desc':[]}
df_um = pd.DataFrame(data=d1)
fil = '/data/CMAP Data Submission Dropbox/Simons CMAP/vault/observation/in-situ/cruise/tblGeotraces_Seawater_IDP2021v2/raw/infos/IN2016_V02_NH4_D_CONC_BOTTLE_0.html'
fil = flist[0]
for fil in flist:
    fil_name = fil.rsplit("/",1)[1].split('.')[0]
    ## These cruises have underscores in their names
    if fil_name.split("_",1)[0] in ['IN2016','IN2018','M81','SS01']:
        cruise_name = "_".join(fil_name.split("_", 2)[:2])
        var_name = fil_name.split(cruise_name+"_",1)[1].rsplit("_",1)[0]
    else:    
        cruise_name = fil_name.split("_",1)[0]
        var_name = fil_name.split("_",1)[1].rsplit("_",1)[0]
    # var_it = fil_name.split("_",1)[1].rsplit("_",1)[1]
    html_page = open(fil, "r")
    contents = html_page.read()
    soup = BeautifulSoup(contents, "html.parser")
    for link in soup.findAll('a'):
        url=link.get('href')
        url_desc =link.text.strip()
        d1 = {'cruise':[cruise_name], 'var_name':[var_name], 'url':[url], 'url_desc':[url_desc]}
        temp_df = pd.DataFrame(data=d1)
        df_um = df_um.append(temp_df, ignore_index=True)
    del url, url_desc

df_um_u = df_um.drop_duplicates()
df_um_u.to_excel(f'{vs.cruise}{tbl}/raw/Geotraces_Seawater_var_cruise_links.xlsx', index=False)
# df_um_u = pd.read_excel(f'{vs.cruise}{tbl}/raw/Geotraces_Seawater_var_cruise_links.xlsx')

df_cruise_var = df_um_u[['cruise','var_name']].drop_duplicates()
df_cruise_var.replace({'KN192-5':'KN192-05'}, inplace=True)

server = 'rainier'
# var_list = df_um_u['var_name'].unique().tolist()
cruise_list = df_cruise_var['cruise'].unique().tolist()
cmap_cruise = DB.dbRead("select * from tblcruise where cruise_series=4",server) 
cmap_cruise_list = cmap_cruise['Name'].unique().tolist()

new_cruises = [x for x in cruise_list if not x in cmap_cruise_list]
## Update to cruise_series = 4: PANDORA 
## 7 new cruises, none in R2R
# ['SAG25', 'IN2016', 'IN2018', 'M81', 'AU1602', 'REDMAST', 'SS01']


var_ids = DB.dbRead(f"select Dataset_ID, id, short_name from tblvariables where table_name = '{tbl}'",server)
dataset_id = var_ids['Dataset_ID'][0]

d = {'Var_ID':[],'JSON_Metadata':[]}
df_Vars_JSON = pd.DataFrame(data=d)

# df_um_u[(df_um_u.var_name=='Nd_D_CONC_BOTTLE') & (df_um_u.cruise=='SAG25')]
# df_um_u[(df_um_u.var_name=='10_Cu_D_CONC_BOTTLE') & (df_um_u.cruise=='SS01')]

# df_um_u[ (df_um_u.cruise=='REDMAST')& (df_um_u.url_desc=='Link to cruise information')]

for row in df_cruise_var.itertuples():
    ## Geotraces names the cruise differently than CMAP and R2R
    if row.cruise == 'KN192-05':
        c_name = 'KN192-5'
    else:
        c_name = row.cruise
    ## Some short names have dashes, spaces, or characters instead of underscores
    var_name = row.var_name.replace('-','_').replace(' ','_').replace('+','_').replace("'","_")
    ###### ADD LOGIC FOR REDMAST CRUISE VARS STARTING WITH Pb, ONLY ONE METHODS URL ALSO ASSIGNED TO CTD VARS
    #### CORRECT URL FOR REDMAST CRUISE INFO: https://www.bodc.ac.uk/resources/inventories/cruise_inventory/report/17647/
    var_id = var_ids.loc[var_ids['short_name']==var_name]['id'].item()
    ## Static HTML files contain 2 to 3 urls
    ## V1 scraping done by Jesse only grabbed methods links in lui of finding accurate Sensor name for each variable
    ## Most HTML files have the following url descriptions:
    ## Link to detailed originator and methods information, Link to cruise information, Link to publications asociated with these data 
    var_links = df_um_u[(df_um_u.var_name==row.var_name) & (df_um_u.cruise==c_name) & (df_um_u.url_desc.str.contains('methods'))]['url'].values.tolist()
    if len(var_links) == 0:
        print(f"{c_name}, {var_name}")
    v_meta_desc = ['BODC documentation link'] * len(var_links)
    json_str = f"""{{"cruise_names":{{"values":["{c_name}"],"descriptions":["Operators Cruise Name"]}},"meta_links":{{"values":{var_links},"descriptions":{v_meta_desc}}}}}"""
    d2 = {'Var_ID':[var_id],'JSON_Metadata':[json_str]}
    temp_df = pd.DataFrame(data=d2)
    ## Replace single with double quotes for valid JSON
    temp_df.replace({'\'':'"'}, inplace=True, regex=True)
    df_Vars_JSON = df_Vars_JSON.append(temp_df, ignore_index=True)    
# df_Vars_JSON.to_excel(f'{vs.cruise}{tbl}/raw/Geotraces_Seawater_Rossby_var_UM_JSON.xlsx', index=False)
# df_Vars_JSON = pd.read_excel(f'{vs.cruise}{tbl}/raw/Geotraces_Seawater_Rossby_var_UM_JSON.xlsx')

### Dataset UM
## Insert into db, use SQLpandas to avoid BCP splitting on commas
cl_ds_json = f"""{{"publication_link":{{"values":["https://www.geotraces.org/geotraces-publications-database/"],"descriptions":["Link to database of GEOTRACES publications"]}}}}"""
d = {'Dataset_ID':[dataset_id],'JSON_Metadata':[cl_ds_json]}
df_Datasets_JSON = pd.DataFrame(data=d)
DB.toSQLpandas(df_Datasets_JSON,'tblDatasets_JSON_Metadata',server)

## Insert into db, SQLpandas errors out
DB.toSQLbcp_wrapper(df_Vars_JSON,'tblVariables_JSON_Metadata',server)

## Fix bcp additional quotes in sql
qry = """UPDATE tblVariables_JSON_Metadata SET json_metadata = replace(replace(replace(json_metadata,'""','"'), '"{','{'), '}"','}')"""
DB.DB_modify(qry, server)