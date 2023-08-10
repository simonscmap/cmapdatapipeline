import sys

import pandas as pd

sys.path.append("ingest")
import vault_structure as vs
import DB
import cruise
import common as cmn
import metadata

## Missing cruises from sosik import
cruise_list = ['ar43', 'ar62', 'ar29', 'en668', 'ar34b', 'rb1904', 'en661', 'ar61b', 'ar63', 'en657', 'ar31a', 'tn368', 'ar28b', 'ar39b', 'en655', 'en644', 'at46', 'en627', 'en649']
cruise_list = [x.upper() for x in cruise_list]

for cruise_name in cruise_list:
    vs.cruise_leaf_structure(vs.r2r_cruise+cruise_name)

cruise_name='AR28-B'  #EN608 , EN617, EN668
missing_nav = []
for cruise_name in cruise_list:
    try:
        cruise.download_single_cruise(cruise_name)
        df_cruise_meta = pd.read_parquet(f"{vs.r2r_cruise}{cruise_name}/raw/{cruise_name}_cruise_metadata.parquet")
        df_traj = pd.read_parquet(f"{vs.r2r_cruise}{cruise_name}/raw/{cruise_name}_trajectory.parquet")
        cruise_raw = f"{vs.r2r_cruise}{cruise_name}/raw/"
        with pd.ExcelWriter(f"{cruise_raw}{cruise_name}_cruise_meta_nav_data.xlsx") as writer:  
            df_cruise_meta.to_excel(writer, sheet_name='cruise_metadata', index=False)
            df_traj[['time','lat','lon']].to_excel(writer, sheet_name='cruise_trajectory', index=False)
    except:
        missing_nav.append(cruise_name)
        continue

misnamed = ['AR61-B', 'AR31-A', 'AR28-B']
for cruise_name in misnamed:
    try:
        cruise.download_single_cruise(cruise_name)
        df_cruise_meta = pd.read_parquet(f"{vs.r2r_cruise}{cruise_name}/raw/{cruise_name}_cruise_metadata.parquet")
        df_cruise_meta['Nickname'] = cruise_name.replace('-','')
        df_traj = pd.read_parquet(f"{vs.r2r_cruise}{cruise_name}/raw/{cruise_name}_trajectory.parquet")
        cruise_raw = f"{vs.r2r_cruise}{cruise_name}/raw/"
        with pd.ExcelWriter(f"{cruise_raw}{cruise_name}_cruise_meta_nav_data.xlsx") as writer:  
            df_cruise_meta.to_excel(writer, sheet_name='cruise_metadata', index=False)
            df_traj[['time','lat','lon']].to_excel(writer, sheet_name='cruise_trajectory', index=False)
    except:
        missing_nav.append(cruise_name)
        continue

### Data for RB1904 not on R2R. Cruise info found here: https://www.ncei.noaa.gov/data/oceans/ncei/ocads/data/0204022/33RO20190512_PI_OME.pdf
cruise_name = 'RB1904'
cruise_nickname = '33RO20190512'
cruise_shipname = 'Ronald H. Brown'
chief_sci='Wanninkhof R.; Pierrot D.'
df_cruise_meta = pd.DataFrame(
    {
        "Nickname": [cruise_nickname],
        "Name": [cruise_name],
        "Ship_Name": [cruise_shipname],
        "Start_Time": "",
        "End_Time": "",
        "Lat_Min": "",
        "Lat_Max": "",
        "Lon_Min": "",
        "Lon_Max": "",
        "Chief_Name": [chief_sci],
        "Cruise_Series":""
    }
)
df_traj = DB.dbRead("SELECT time, lat, lon from [tblSosik_flow_cytometry_NES_LTER]  where cruise = 'RB1904'",'Rossby')
df_cruise_meta =cruise.add_ST_cols_to_metadata_df(df_cruise_meta, df_traj)
cruise_raw = f"{vs.r2r_cruise}{cruise_name}/raw/"
with pd.ExcelWriter(f"{cruise_raw}{cruise_name}_cruise_meta_nav_data.xlsx") as writer:  
    df_cruise_meta.to_excel(writer, sheet_name='cruise_metadata', index=False)
    df_traj[['time','lat','lon']].to_excel(writer, sheet_name='cruise_trajectory', index=False)


### Check that new trajectories match data in Sosik
qry = "SELECT cruise, min(time) mntime, max(time) mxtime, min(lat) mnlat, max(lat) mxlat, min(lon) mnlon, max(lon) mxlon from tblSosik_flow_cytometry_NES_LTER group by cruise"
df = DB.dbRead(qry, 'Rossby')
cruise_list = df['cruise'].to_list()
recheck = []
for cruise_name in recheck:
    if cruise_name in ['AR61B', 'AR31A', 'AR28B']:
        cruise_name_f = cruise_name[:4]+'-'+cruise_name[4:]
    else:
        cruise_name_f = cruise_name
    ps = 1
    cruise_raw = f"{vs.r2r_cruise}{cruise_name_f}/raw/"
    df_traj = pd.read_excel(f"{cruise_raw}{cruise_name_f}_cruise_meta_nav_data.xlsx", sheet_name='cruise_trajectory')
    df_traj_desc = df_traj.describe()
    if ((df.loc[df['cruise'] == cruise_name]['mntime'] < df_traj['time'].min()) | (df.loc[df['cruise'] == cruise_name]['mxtime'] > df_traj['time'].max())).any():
        print(f'Time out of bounds for {cruise_name}')
        ps = 0          
    if ps == 0:
        recheck.append(cruise_name)

### ['EN644', 'EN617', 'AR34B', 'EN655', 'EN608', 'EN668', 'EN657']
for cruise_name in recheck:
    if cruise_name in ['AR61B', 'AR31A', 'AR28B']:
        cruise_name_f = cruise_name[:4]+'-'+cruise_name[4:]
    else:
        cruise_name_f = cruise_name
    cruise_raw = f"{vs.r2r_cruise}{cruise_name_f}/raw/"
    df_traj = pd.read_excel(f"{cruise_raw}{cruise_name_f}_cruise_meta_nav_data.xlsx", sheet_name='cruise_trajectory')
    mn_time = df_traj['time'].min().strftime(format='%Y-%m-%d %H:%M:%S')
    mx_time = df_traj['time'].max().strftime(format='%Y-%m-%d %H:%M:%S')
    qry = f"select time, lat, lon from tblSosik_flow_cytometry_NES_LTER where cruise = '{cruise_name}' and (time < '{mn_time}' or time > '{mx_time}')"
    traj_add = DB.dbRead(qry, 'rossby')
    df_traj_new = pd.concat([df_traj, traj_add], ignore_index=True)
    df_traj_new = df_traj_new.sort_values(['time','lat','lon'],ascending=[True]*3)
    ## Overwrite with new data appended from Sosik
    ## Don't need to add space time to metadata as that's done on ingest
    df_cruise_meta = pd.read_excel(f"{cruise_raw}{cruise_name_f}_cruise_meta_nav_data.xlsx", sheet_name='cruise_metadata')
    with pd.ExcelWriter(f"{cruise_raw}{cruise_name}_cruise_meta_nav_data.xlsx") as writer:  
        df_cruise_meta.to_excel(writer, sheet_name='cruise_metadata', index=False)
        df_traj_new[['time','lat','lon']].to_excel(writer, sheet_name='cruise_trajectory', index=False)


### Print ingest if new, or update if already existing
### AR28-B already in db and correct
for crs in cruise_list:
    qry = f"select name, nickname, cruise_id from tblcruise c left join tblcruise_trajectory t on c.id = t.cruise_id where name = '{crs}' or nickname = '{crs}'"
    df_check = DB.dbRead(qry,'Rossby')
    if len(df_check)==0:
        if crs in ['AR61B', 'AR31A', 'AR28B']:
            crs = crs[:4]+'-'+crs[4:]
        print(f'python general.py "{crs}_cruise_meta_nav_data.xlsx" -C {crs} -S "Rossby" -v True')
    elif df_check['cruise_id'].isna().any():
        print(f"Import trajectory only for {crs}")
    else:
        cruise_raw = f"{vs.r2r_cruise}{crs}/raw/"
        df_traj = pd.read_excel(f"{cruise_raw}{crs}_cruise_meta_nav_data.xlsx", sheet_name='cruise_trajectory')
        df_traj['time'] =df_traj['time'].dt.strftime('%Y-%m-%d %H:%M:%S')
        cnt = input(f"delete {crs}? [y/n] ")
        if cnt == 'y':
            for server in ['rossby','mariana','rainier']:
                cruise.delete_cruise_trajectory(crs,'opedia',server)
                cruise.insert_cruise_trajectory(df_traj,crs,'opedia',server)
        else:
            continue

### After ingest, associate cruises to Sosik dataset
for server in ['rossby','mariana','rainier']:
    ds_id = cmn.getDatasetID_Tbl_Name('tblSosik_flow_cytometry_NES_LTER','Opedia',server)
    for cruise_name in cruise_list:
        if cruise_name in ['AR61B', 'AR31A', 'AR28B']:
            cruise_name = cruise_name[:4]+'-'+cruise_name[4:]
        if cruise_name not in ['EN617','EN608']:
            cruise_ID = cmn.getCruiseID_Cruise_Name(cruise_name, server)
            try:
                metadata.tblDataset_Cruises_Line_Insert(ds_id, cruise_ID, 'Opedia', server)
            except:
                print(f"{cruise_name} already in {server}")


metadata.export_script_to_vault('tblSosik_flow_cytometry_NES_LTER','cruise','process/insitu/cruise/Sosik/process_Sosik_trajectories.py','process_traj.txt')

