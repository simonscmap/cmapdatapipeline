import requests
import json
import pandas as pd

from ingest import vault_structure as vs

tbl = 'tblGeotraces_Sensor'
tbl = 'tblGeotraces_Seawater'
raw_folder = f'{vs.cruise}{tbl}/raw/'
def try_catch(val):
    try:
        x = station[val]
    except:
        x = None
    return x

url = 'https://api.linked-systems.uk/api/metadata/geotraces/v1/cruises/json/'
cruise_list = ['ArcticNet1502','ArcticNet1503','AU1603','D357','JC068','KH14-06','KH17-03','SK304','SK311','SK312']

cruise_list = ['JC156', 'ISSS-08', 'PS70', 'AU0703', 'MD166', 'PS71', 'AU0806', '2012-13', '2014-19', 'GEOVIDE', 'HLY1502', 'RR1815', 'RR1814 | RR1815', 'IN2016_V01', 'IN2016_V02', 'JC057', 'KH09-05', 'KH11-07', '2013-18', 'KH12-04', 'KH15-03', 'KN199', 'KN204', 'M81_1', 'MD188', 'MedSeA2013', 'PE319', 'PE321', 'PE370', 'PE373', 'PE374', 'PEACETIME', 'PS100', 'PS94', 'SK324', 'SS01_10', 'SS2011', 'TAN1109', 'RR1814']

cruise_list = ['PE358','M121','SK338','TAN0609','D361','KH05-02','TAN0811','SO223T','TAN1212','PS1718']

cruise_list = ['nbp1409',  'so245']

for cruise in cruise_list:
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