import pandas as pd

import vault_structure as vs
import DB
import metadata
import common as cmn

bats = pd.read_excel(vs.download_transfer+"BATS_traj.xlsx")


tbl_list = ['tblBATS_Bottle','tblBATS_Bottle_Validation','tblBATS_Pigment','tblBATS_Pigment_Validation','tblBATS_Primary_Production','tblBATS_Sediment_Trap','tblBATS_Zooplankton_Biomass']
tbl = 'tblBATS_Bacteria_Production'
tbl = 'tblBATS_CTD'
# qry = f"select min(time) mntime, max(time) mxtime, left(cast(cruise_ID as varchar(20)), 5) bats_id FROM {tbl} group by left(cast(cruise_ID as varchar(20)), 5) order by left(cast(cruise_ID as varchar(20)), 5)"

# ### Find bats cruises not yet in CMAP
# bats_names = bats['Name'].to_list()
# missing = []
# for nm in bats_names:
#     try:
#         cruise_id = cmn.getCruiseID_Cruise_Name(nm, server)
#         # print(cruise_id)
#         # metadata.tblDataset_Cruises_Line_Insert(ds_id, cruise_id, 'Opedia', server)
#     except:
#         missing.append(nm)
#         continue

# for m in missing:
#     print(bats[['Nickname', 'Name', 'Ship_Name', 'Chief_Name']].loc[bats['Name']==m])
for tbl in tbl_list:
    for server in ['rainier','rossby','mariana']:
        qry = f"select distinct left(cast(cruise_ID as varchar(20)), 5) bats_id FROM {tbl} order by left(cast(cruise_ID as varchar(20)), 5)"
        tbl_df = DB.dbRead(qry, server)
        bats_id = tbl_df['bats_id'].to_list()

        ds_id = cmn.getDatasetID_Tbl_Name(tbl, 'Opedia', server)
        no_id = []
        for id in bats_id:
            cruises = bats.loc[bats['Nickname']==int(id),'Name'].to_list()
            if len(cruises)>0:
                for cruisename in cruises:
                    try:
                        cruise_id = cmn.getCruiseID_Cruise_Name(cruisename, server)
                        # print(cruise_id)
                        metadata.tblDataset_Cruises_Line_Insert(ds_id, cruise_id, 'Opedia', server)
                    except:
                        print(f"No cruise name for {cruisename}")
                        continue
            else:
                # print(f"No cruise name for {id}")
                no_id.append(id)
    print(server)
    print(f"{tbl} unmatched cruise numbers {len(no_id)}/{len(bats_id)}")

### tblBATS_CTD unmatched cruise numbers 325/447; tblBATS_Sediment_Trap unmatched cruise numbers 225/ ; tblBATS_Primary_Production unmatched cruise numbers 266/
### tblBATS_Zooplankton_Biomass unmatched cruise numbers 178/289; tblBATS_Bottle unmatched cruise numbers 293/
# ### tblBATS_Pigment_Validation unmatched cruise numbers 3/9; tblBATS_Bottle_Validation unmatched cruise numbers 39/
### tblBATS_Pigment unmatched cruise numbers 287/
### tblBATS_Bacteria_Production unmatched BATS cruise numbers (226)
# ['10025', '10026', '10027', '10028', '10030', '10031', '10032', '10033', '10034', '10035', '10037', '10038', '10039', '10040', '10041', '10042', '10043', '10046', '10047', '10048', '10049', '10050', '10051', '10052', '10053', '10054', '10055', '10056', '10057', '10058', '10059', '10060', '10062', '10063', '10064', '10065', '10066', '10067', '10068', '10069', '10070', '10071', '10072', '10073', '10074', '10075', '10076', '10077', '10078', '10079', '10080', '10081', '10082', '10083', '10084', '10085', '10086', '10087', '10088', '10090', '10091', '10092', '10093', '10094', '10095', '10096', '10097', '10098', '10099', '10100', '10101', '10102', '10104', '10105', '10106', '10107', '10108', '10109', '10110', '10111', '10112', '10113', '10114', '10116', '10117', '10118', '10119', '10120', '10121', '10122', '10123', '10124', '10125', '10126', '10127', '10128', '10129', '10130', '10131', '10132', '10133', '10134', '10135', '10136', '10137', '10138', '10139', '10140', '10141', '10142', '10143', '10144', '10145', '10146', '10147', '10148', '10149', '10150', '10151', '10152', '10154', '10155', '10156', '10157', '10158', '10159', '10161', '10162', '10163', '10164', '10165', '10166', '10167', '10168', '10169', '10170', '10171', '10173', '10175', '10176', '10177', '10178', '10179', '10180', '10182', '10184', '10185', '10186', '10187', '10188', '10189', '10190', '10191', '10192', '10193', '10194', '10195', '10196', '10197', '10198', '10199', '10200', '10201', '10202', '10203', '10204', '10205', '10207', '10209', '10210', '10211', '10212', '10213', '10214', '10215', '10216', '10217', '10219', '10220', '10221', '10222', '10226', '10227', '10228', '10229', '10230', '10231', '10232', '10233', '20028', '20029', '20040', '20041', '20053', '20054', '20055', '20065', '20066', '20067', '20076', '20077', '20078', '20079', '20089', '20090', '20091', '20100', '20101', '20102', '20114', '20124', '20125', '20160', '20161', '20173', '20174', '20184', '20186', '20196', '20197', '20198', '20208', '20221', '20232', '30030', '30042']        