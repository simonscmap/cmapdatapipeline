import sys
import os

import pandas as pd
from tqdm import tqdm
sys.path.append("cmapdata/ingest")
sys.path.append("ingest")
from ingest import vault_structure as vs
from ingest import DB

server = 'Rainier'
server = 'Rossby'
server = 'Mariana'

tbl = 'tblGeotraces_Seawater'
# df_Vars_JSON = pd.read_excel(f'{vs.cruise}{tbl}/metadata/Seawater_Vars_JSON.xlsx')
# df_Datasets_JSON = pd.read_excel(f'{vs.cruise}{tbl}/metadata/Seawater_Datasets_JSON.xlsx')

df_excel_var_links = pd.read_excel(f'{vs.cruise}{tbl}/raw/Geotraces_Seawater_Metadata_DHedits.xlsx', sheet_name='cruise_sensor_methodological_da')
df_excel_var_links.replace({'KN192-5':'KN192-05'}, inplace=True)

cruise_column_name = 'Operators_Cruise_Name'

qry = "SELECT Dataset_ID, ID, short_name  FROM tblVariables where table_name = 'tblGeotraces_Seawater'"
var_id  = DB.dbRead(qry, server)

dataset_id = var_id['Dataset_ID'][0]

qry = f"SELECT Cruise_ID, Name, Nickname FROM tblDataset_Cruises dc inner join tblcruise c on dc.Cruise_ID = c.ID where Dataset_ID = {dataset_id}"
cruise_ids = DB.dbRead(qry, server)

cols = var_id['short_name'].to_list()
cruise_list = cruise_ids['Name'].to_list()

# d = {'cruise':[],'column':[]}
# df_cruise_vars = pd.DataFrame(data=d)

# for cruise in tqdm(cruise_list):
#     if cruise == 'KN192-05':
#         cruise = 'KN192-5'
#     for col in cols:
#         qry = f"SELECT count(*) as cnt from dbo.{tbl} where {cruise_column_name} = '{cruise}' and [{col}] is not null"
#         check_count = DB.dbRead(qry, 'Rainier')
#         if check_count['cnt'][0] > 0:
#             d2 = {'cruise':[cruise],'column':[col]}
#             temp_df = pd.DataFrame(data=d2)
#             df_cruise_vars = df_cruise_vars.append(temp_df, ignore_index=True)  

# df_cruise_vars_id = pd.merge(df_cruise_vars, cruise_ids[['Cruise_ID','Name','Nickname']], how = 'left', left_on = 'cruise', right_on='Name' )
# df_cruise_vars_ids = pd.merge(df_cruise_vars_id, var_id[['ID','short_name']], how = 'left', left_on = 'column', right_on='short_name' )
# df_cruise_vars_ids.replace({'KN192-5':'KN192-05'}, inplace=True)

# df_cruise_vars_ids.to_excel(f'{vs.cruise}{tbl}/raw/cruise_vars_ids.xlsx', index=False)

# cruise_list_db = [x if x != 'KN192-5' else 'KN192-05' for x in cruise_list]
### tbDatasets_JSON_Metadata
# { "values": [], "descriptions": [] }
# { "values": [100, 'varname'], "descriptions": ['cruise_id','var_shortname'] }
# d = {'Dataset_ID':[],'JSON_Values':[],'JSON_Desc':[]}

# d = {'Dataset_ID':[],'JSON_Metadata':[]}
# df_Datasets_JSON = pd.DataFrame(data=d)
# for cruise in cruise_list:
#     cl = df_cruise_vars_ids.loc[df_cruise_vars_ids['cruise']==cruise]
#     cruise_str = cl['cruise'].iloc[0]
#     cruise_id = cl['Cruise_ID'].iloc[0].astype(int)
#     var_str = "', '".join(cl['column'])
#     var_id_str = ", ".join(cl['ID'].astype(str)) 
#     desc_list = ['cruise_name'] * len(var_str)
#     v_ds_json = f"""{{"values":['{cruise_str}','{var_str}']}}"""
#     d_ds_json = f"""{{"descriptions":[{desc_list},'{var_str}']}}"""

#     cl_ds_json = f"""{{"Cruise_id":{{"values":[{cruise_id}],"descriptions":['{cruise_str}']}},"Var_id":{{"values":[{var_id_str}],"descriptions":['{var_str}']}}}}"""
#     ## insert into db values (dataset_id , cl_ds_json)
#     # d2 = {'Dataset_ID':[dataset_id],'JSON_Values':[v_ds_json],'JSON_Desc':[d_ds_json]}
#     d2 = {'Dataset_ID':[dataset_id],'JSON_Metadata':[cl_ds_json]}
#     temp_df = pd.DataFrame(data=d2)
#     temp_df.replace({'\'':'"'}, inplace=True, regex=True)
#     df_Datasets_JSON = df_Datasets_JSON.append(temp_df, ignore_index=True)


cl_ds_json = f"""{{"publication_link":{{"values":["https://www.geotraces.org/geotraces-publications-database/"],"descriptions":["Link to database of GEOTRACES publications"]}}}}"""
d = {'Dataset_ID':[dataset_id],'JSON_Metadata':[cl_ds_json]}
df_Datasets_JSON = pd.DataFrame(data=d)


### tblVariabes_JSON_Metadata
# { "Cruise_id": { "values": [], "descriptions": [] }, "meta_links: { "values": [], "descriptions": [] }
# { "values": [], "descriptions": [] }
# { "values": [100, 'url'], "descriptions": ['cruise_id','Meta link'] }
df_var_links_id = pd.merge(df_excel_var_links, cruise_ids[['Cruise_ID','Name','Nickname']], how = 'left', left_on = 'cruise_name', right_on='Name' )    
df_var_links_ids = pd.merge(df_var_links_id,var_id[['ID','short_name']], how = 'left', left_on = 'var_short_name', right_on='short_name')

len(df_var_links_ids[df_var_links_ids.duplicated(keep=False)])
### Drop duplicates
df_var_links_ids.drop_duplicates(inplace=True)
    

####### ?? JOIN VAR IDS based on if short name like to get _qc and _err

# d = {'Var_ID':[],'JSON_Values':[],'JSON_Desc':[]}
d = {'Var_ID':[],'JSON_Metadata':[]}
df_Vars_JSON = pd.DataFrame(data=d)
for c in cols:
    cl = df_var_links_ids.loc[df_var_links_ids['short_name']==c]
    if len(cl) > 0:
        for row in cl.itertuples():
            ## no data in seawater table for these cruises
            if row.cruise_name == 'M135' or row.cruise_name == 'M81' or row.cruise_name == 'SS01':
                continue
            c_id = int(row.Cruise_ID)
            c_name = row.cruise_name
            c_nickname = row.Nickname
            v_id = int(row.ID)
            v_meta = row.link
            v_meta_list = v_meta.replace(';',', ')
            v_meta_desc = ['BODC documentation link'] * len(v_meta_list.split(', '))
            # v_meta_d = "', '".join(v_meta_desc)
            # v_ds_json = f"""{{"values":['{c_name}','{v_meta_list}']}}"""
            # d_ds_json = f"""{{"descriptions":['cruise_name','{v_meta_d}']}}"""
            json_str = f"""{{"cruise_names":{{"values":['{c_name}'],"descriptions":["Operators Cruise Name"]}},"meta_links":{{"values":['{v_meta_list}'],"descriptions":{v_meta_desc}}}}}"""
            # json_str = f"""{{"cruise_names":{{"values":['{c_name}'],"descriptions":['{c_nickname}']}},"meta_links":{{"values":['{v_meta_list}'],"descriptions":{v_meta_desc}}}}}"""
            d2 = {'Var_ID':[v_id],'JSON_Metadata':[json_str]}
            # d2 = {'Var_ID':[v_id],'JSON_Values':[v_ds_json],'JSON_Desc':[d_ds_json]}
            temp_df = pd.DataFrame(data=d2)
            temp_df.replace({'\'':'"'}, inplace=True, regex=True)
            df_Vars_JSON = df_Vars_JSON.append(temp_df, ignore_index=True)

df_Vars_JSON.to_excel(vs.download_transfer+'geo_var_um.xlsx', index = False)
df_Vars_JSON['Var_ID'] = df_Vars_JSON['Var_ID'].astype(int)
# server_list = ['Rainier','Rossby','Mariana']
# for server in server_list:
server = 'mariana'
# qry = f"delete from dbo.tblDatasets_JSON_Metadata"
# DB.DB_modify(qry,server)
qry = f"delete from dbo.tblVariables_JSON_Metadata"
DB.DB_modify(qry,server)
# DB.toSQLpandas(df_Datasets_JSON,'tblDatasets_JSON_Metadata',server)
DB.toSQLpandas(df_Vars_JSON,'tblVariables_JSON_Metadata',server)

DB.toSQLbcp_wrapper(df_Vars_JSON,'tblVariables_JSON_Metadata',server)

## Fix bcp additional quotes in sql

#   update tblVariables_JSON_Metadata set JSON_Metadata = replace(JSON_Metadata,'""','"') 

#     update tblVariables_JSON_Metadata
#   set JSON_Metadata = replace(JSON_Metadata,'"{','{')

#       update tblVariables_JSON_Metadata
#   set JSON_Metadata = replace(JSON_Metadata,'}"','}')
