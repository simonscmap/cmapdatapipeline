import sys
import os
import pandas as pd
import glob
import requests
import re

sys.path.append("ingest")
import vault_structure as vs
import cruise


cruise_list = [
'AE1822',
'AE1825',
'AE1828',
'AE1829',
'AE1831',
'AE1836',
'AE1902',
'EN629',
'EN631',
'AE1905',
'AE1906',
'AE1909',
'AE1912',
'AE1913',
'AE1917',
'AE1921',
'AE1923',
'AE1925',
'AE1927',
'AE1930',
'AE1932',
'AE2002',
'AE2005',
'AE2009',
'AE2010',
'AE2012',
'AE2014',
'AE2015',
'AE2016',
'AE2017',
'AE2019',
'AE2021',
'AE2022',
'AE2101',
'AE2102',
'AE2103',
'AE2104',
'AE2105',
'AE2107',
'AE2109',
'AE2110',
'AE2112',
'AE2116',
'AE2117',
'AE2120',
'AE2121',
'AE2124',
'AE2201',
'AE2204',
'AE2205',
'AE2208',
'AE2210',
'AE2211',
'AE2212',
'AE2214',
'AE2217',
'AE2219',
'AE2222',
'AE2224',
'AE2226']
## Geo CSV processing
cruise_list=['AR43', 'AR62', 'AR29', 'EN668']
for cruise_name in cruise_list:
    cruise_raw = f"{vs.r2r_cruise}{cruise_name}/raw/"
    # flist = glob.glob(f"{cruise_raw}*.geoCSV")
    geoCSV=''
    for root, dirs, files in os.walk(cruise_raw):
        for filename in files:
            # print(filename)
            if re.search(r'min.geoCSV$', filename):
                geoCSV = os.path.join(root, filename)
    if len(geoCSV) ==0:
        continue
    df_nav = pd.read_csv(geoCSV, skiprows=16)
    # df_nav = pd.read_csv( glob.glob(cruise_raw+'*.geoCSV')[0], skiprows=16)
    df_nav['time'] = pd.to_datetime(df_nav['iso_time']).dt.tz_localize(None)
    df_nav.rename(columns={'ship_longitude':'lon','ship_latitude':'lat'}, inplace=True)
    df_nav=df_nav[['time','lat','lon']]
    df_len = len(df_nav)
    df_nav.dropna(subset=['time','lat','lon'], inplace=True)
    df_nav['time'] = df_nav['time'].dt.strftime('%Y-%m-%dT%H:%M:%S')
    df_nav.to_excel(cruise_raw+f'{cruise_name}_trajectory.xlsx', index=False)
    (df_len - len(df_nav))/ df_len
    del geoCSV
    # df_nav.index = pd.to_datetime(df_nav.time)
    # rs_df = df_nav.resample('1min').mean()
    # rs_df = rs_df.dropna()
    # rs_df.reset_index(inplace=True)
    # rs_df = rs_df.sort_values(['time','lat','lon'], ascending=[True] * 3)
    # rs_df['time'] = rs_df['time'].dt.strftime('%Y-%m-%dT%H:%M:%S')
    # (df_len - len(rs_df))/ df_len
    # rs_df.to_excel(cruise_raw+f'{cruise_name}_trajectory.xlsx', index=False)
    # del df_nav
    # del rs_df

cruise_name = 'AE1726'
cruise_list = ['AE1726','AE1729']
for cruise_name in cruise_list:
    cruise_raw = f"{vs.r2r_cruise}{cruise_name}/raw/"
    for root, dirs, files in os.walk(cruise_raw):
        for filename in files:
            # print(filename)
            if re.search(r'a_ae.gps$', filename):
                traj = os.path.join(root, filename)
                ## DOY, lon, lat
                ## sep irregular number of spaces
                df = pd.read_csv(traj, sep='\s+',header=None, names=['doy','lat','lon'])
                yr = 2017
                df['year']=yr
                if calendar.isleap(yr-1):
                    df['doy_leap']= df['doy']+1



from datetime import datetime, timedelta
import calendar
calendar.isleap(2017)
calendar.isleap(2016)
df['date'] = pd.to_datetime(df['year']*1000 + df['doy'], format='%Y%j')
epoch = datetime(2017 - 1, 11, 15)
result = epoch + timedelta(days=365.630244)
df['time'] = df['date'] + timedelta(days=365.630244)
if calendar.isleap(yr-1):
    result+=1

def dm(x):
    degrees = int(x) // 100
    minutes = x - 100*degrees
    return degrees, minutes

def decimal_degrees(degrees, minutes):
    return degrees + minutes/60 


col_list = ['date_utc','time_utc','GGA','fix_time','lat_deg','lat_dir','lon_deg','lon_dir','fix_qual','sat_count','hdop','alt','alt_unit','sea_level','sea_level_unit','dgps_update_time','dgps_stationID','checksum']

col_list_17 = ['date_utc','time_utc','GGA','fix_time','lat_str','lat_dir','lon_str','lon_dir','fix_qual','sat_count','hdop','alt','alt_unit','sea_level','sea_level_unit','dgps','checksum']

cruise_name = 'AE1529'#'AE0827'
flist = glob.glob(f"{cruise_raw}*.Raw")


combined_df_list = []
for fil in flist:
    ## Read all as string to catch leading zeros in lon
    df = pd.read_csv(fil,names=col_list_17, dtype = str)
    df_len = len(df)
    ## Using errors coerce due to SUB (substitute control character) and VT (vertical tab) in raw file
    df['time'] = pd.to_datetime(df['date_utc']+df['time_utc'], format="%m/%d/%Y%H:%M:%S.%f",errors='coerce')
    df = df.loc[~df['time'].isnull()]
    ## Question mark in place of decimal point in lat
    df['lat_str'] = df['lat_str'].str.replace('?','.',regex=False)
    df['lon_str'] = df['lon_str'].str.replace('?','.',regex=False)    
    ## Question mark also present at and of lat string. remove trailing .
    df['lat_str'] = df['lat_str'].str.replace('\.$','',regex=True)
    df['lon_str'] = df['lon_str'].str.replace('\.$','',regex=True)    
    ## ! in place of values in lat
    df['lat_str'] = df['lat_str'].str.replace('!','',regex=False)
    ## Drop rows with bad delim (Direction in with lon)
    df = df.loc[df['lon_str']!='W']
    ## Convert to decimal deg
    df['lon'] =  df['lon_str'].astype(float).map(lambda x: decimal_degrees(*dm(x)))
    df['lat'] =  df['lat_str'].astype(float).map(lambda x: decimal_degrees(*dm(x)))
    ## multiply by -1 if lon is W
    dict_lon = {'E':1, 'W':-1}
    df['lon'] =df['lon'].mul(df['lon_dir'].map(dict_lon)).fillna(df['lon'])
    ## Drop where position fix not available or invalid
    df = df.loc[df['fix_time']!='0']
    df = df[['time','lat','lon']]
    df.dropna(subset=['time','lat','lon'], inplace=True)
    df_len_new = len(df)
    print(f"Dropped rows: {df_len - df_len_new}")
    combined_df_list.append(df)

combined_df = pd.concat(combined_df_list, axis=0, ignore_index=True)
combined_df = combined_df.sort_values(['time','lat','lon'], ascending=[True] * 3)
combined_df['time']=combined_df['time'].dt.strftime('%Y-%m-%dT%H:%M:%S')

## Downsample for trajectory
combined_df.index = pd.to_datetime(combined_df.time)
rs_df = combined_df.resample('1min').mean()
rs_df = rs_df.dropna()
rs_df.reset_index(inplace=True)
rs_df['time'] = rs_df['time'].dt.strftime('%Y-%m-%dT%H:%M:%S')
df_len_new - len(rs_df)

rs_df.to_excel(cruise_raw+f'{cruise_name}_trajectory.xlsx', index=False)

bats = pd.read_excel(vs.download_transfer+'BATS_traj.xlsx')
cruise_list = ['AE0813','AE0816','AE0827','AE0829','AE1119','AE1528','AE1529']
## Geo CSV processing
for cruise_name in cruise_list:
    if cruise_name not in ['AE1318', 'AE1704','AE1726','AE1729', 'AE1802']:
        cruise_raw = f"{vs.r2r_cruise}{cruise_name}/raw/"
        df_traj = pd.read_excel(cruise_raw+f"{cruise_name}_trajectory.xlsx")
        ## Build cruise_metadata sheet
        print(f"Cruise name: {cruise_name}")
        df_cruise_meta = pd.DataFrame(
            {
                "Nickname": ['BATS ' + str(bats.loc[bats['Name']==cruise_name,'Nickname'].iloc[0])],
                "Name": [cruise_name],
                "Ship_Name": [bats.loc[bats['Name']==cruise_name,'Ship_Name'].iloc[0]],
                "Chief_Name": [bats.loc[bats['Name']==cruise_name,'Chief_Name'].iloc[0]],
            }
        )
        df_cruise_meta['Cruise_Series'] = None
        ## Export cruise metadata and trajectory template
        with pd.ExcelWriter(f"{cruise_raw}{cruise_name}_cruise_meta_nav_data.xlsx") as writer:  
            df_cruise_meta.to_excel(writer, sheet_name='cruise_metadata', index=False)
            df_traj[['time','lat','lon']].to_excel(writer, sheet_name='cruise_trajectory', index=False)
import credentials as cr
for cruise_name in cruise_list:
    print(f"python general.py '{cruise_name}_cruise_meta_nav_data.xlsx' -C {cruise_name} -S 'rossby' -v True")