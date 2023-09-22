import requests
import json
import pandas as pd

from ingest import vault_structure as vs

tbl = 'tblGeotraces_Seawater_IDP2021v2'

def try_catch(val):
    try:
        x = station[val]
    except:
        x = None
    return x

url = 'https://api.linked-systems.uk/api/metadata/geotraces/v1/cruises/json/'
cruise_list =['SAG25', 'AU1602', 'REDMAST']


for cruise in cruise_list:
    vs.cruise_leaf_structure(vs.r2r_cruise+cruise)
    raw_folder = f'{vs.r2r_cruise+cruise}/raw/'
    resp = requests.get(url+cruise).json()
    resp_json = json.dumps(resp)
    resp_info = json.loads(resp_json)
    station_list = []
    for station in resp_info['items']:
        station_list.append([
            try_catch('site'), try_catch('cruise_station_ref'), try_catch('start_date'), try_catch('end_date'), try_catch('latitude'), try_catch('longitude')
        ])    
    df = pd.DataFrame(data = station_list, columns = ['site', 'cruise_station_ref', 'start_date', 'end_date', 'latitude', 'longitude'])
    df.drop_duplicates(inplace=True)
    df.sort_values(by=['start_date'], inplace=True)
    df.reset_index(drop=True)
    df.to_excel(raw_folder+cruise+'station_locations.xlsx',index=False)
    print(f'Exported locations for {cruise}')