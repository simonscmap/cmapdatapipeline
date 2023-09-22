import os
import sys
import pandas as pd
from pandas.util import hash_pandas_object
import numpy as np
import math
from tqdm import tqdm

import pycmap
# from pycmap import viz

sys.path.append("cmapdata/ingest")
sys.path.append("ingest")

import common as cmn
import metadata
import api_checks
import credentials as cr
import vault_structure as vs
import DB

api = pycmap.API(token=cr.api_key)


def checkServerAlias(tbl):
    """Check Server Alias is correct on all servers
    Args:
        tbl (str): CMAP table name
    """
    for server in cr.server_alias_list:
        print("Checking tblDataset_Servers")
        ds_id = cmn.getDatasetID_Tbl_Name(tbl,'Opedia',server)
        ## Check server alias list is accurate on all servers
        qry = f"SELECT server_alias FROM dbo.tblDataset_Servers WHERE Dataset_ID = {ds_id}"
        df_server = DB.dbRead(qry, server)
        alias_list = df_server['server_alias'].to_list()
        qry_tbl = f"SELECT table_name FROM [INFORMATION_SCHEMA].[TABLES] WHERE table_name = '{tbl}'"
        for a in alias_list:
            if a.lower() != "cluster":
                df_alias = DB.dbRead(qry_tbl, a)
                if len(df_alias) !=1:
                    print(f"{tbl} listed in {a} but not present on {a}")
        ## Check server alias list isn't missing a server
        if len(alias_list) < 3:
            df_table = DB.dbRead(qry_tbl, server)
            if len(df_table) != 0 and server not in alias_list:
                print(f"{tbl} not listed in alias list but not present on {server}")
    return alias_list

def checkRegion(tbl, alias_list):
    print("Checking tblDataset_Regions")
    for server in cr.server_alias_list:
        ds_id = cmn.getDatasetID_Tbl_Name(tbl,'Opedia',server)
        qry_n = f"select dataset_name from tbldatasets where id = {ds_id}"
        dataset_name = DB.dbRead(qry_n,server)['dataset_name'][0]
        qry = f"SELECT [Dataset_ID] ,Region_Name FROM [Opedia].[dbo].[tblDataset_Regions] d inner join tblRegions r on d.Region_ID=r.Region_ID where Dataset_ID = {ds_id}" 
        df_region = DB.dbRead(qry, server)  
        if len(df_region) == 0:
            if alias_list[0].lower() == 'cluster':
                argo = input("Add regions for Argo Core and BGC? If no, enter regions manually. [y or n] ")
                if argo == 'y':
                    argo_regions =  ['North Atlantic Ocean', 'North Pacific Ocean', 'Caribbean Sea', 'Indian Ocean', 'Gulf of Mexico', 'Mediterranean Sea', 'South Pacific Ocean', 'South Atlantic Ocean', 'Southern Ocean', 'Arctic Ocean']
                    metadata.ocean_region_insert(argo_regions, dataset_name, 'Opedia', server)
                else:
                    manual_regions = input("Add list of regions for manual update. Eg ['Global'] or ['North Atlantic Ocean', 'North Pacific Ocean'] ")
                    metadata.ocean_region_insert(manual_regions, dataset_name, 'Opedia', server)
            else:
                qry_r = f"SELECT DISTINCT lat, lon from {tbl}"
                data_df = DB.dbRead(qry_r, alias_list[0])
                region_list = metadata.ocean_region_names(data_df)
                try:
                    metadata.ocean_region_insert(region_list, dataset_name, 'Opedia', server)
                except:
                    print(f"{dataset_name} already in {server}")
                continue

        else:
            print(f"Region check complete for {dataset_name} on {server}")

def checkHasDepth(tbl,server):
    print("Checking tblVariables for Has_Depth")
    qry = f"SELECT column_name FROM [INFORMATION_SCHEMA].[COLUMNS] WHERE table_name = '{tbl}'"
    df_cols = DB.dbRead(qry, server)
    col_list = df_cols['column_name'].to_list()
    if 'depth' in col_list:
        qry = f"Select distinct has_depth from tblvariables where table_name = '{tbl}'"
        has_depth = DB.dbRead(qry,'Rainier')
        if len(has_depth)==1:
            if not has_depth['has_depth'][0]:
                print(f'######## Fix has_depth flag for {tbl} on {server}')



def compareDOI(tbl,server):
    base_folder = vs.download_transfer
    dataset_id = cmn.getDatasetID_Tbl_Name(tbl,'Opedia', server)
    qry = f"""
    SELECT  r.[Reference_ID]
        ,[Dataset_ID]
        ,[Reference]
        ,[Data_DOI]
        ,[DOI_Download_Link]
        ,[Entity_Name]
        ,[CMAP_Format]  
    FROM [Opedia].[dbo].[tblDataset_References] r inner join 
    [dbo].[tblDataset_DOI_Download] d on r.reference_id = d.reference_id
    where Dataset_ID = {dataset_id}
    """
    df_ref = DB.dbRead(qry, server)
    
    wget_str = f'wget --no-check-certificate "{df_ref["DOI_Download_Link"][0]}" -O "{base_folder + df_ref["Entity_Name"][0]}"'
    os.system(wget_str)

    df_doi_data = pd.read_excel(base_folder + df_ref["Entity_Name"][0], sheet_name='data')
    # df_doi_data['time_check'] =df_doi_data['time']
    for c in df_doi_data.columns.to_list():
        if 'Unnamed: ' in c:
            df_doi_data.drop(columns=c, inplace=True)
    df_doi_data['time'] =pd.to_datetime(df_doi_data['time'])
    df_cmap_data = DB.dbRead(f"Select * from {tbl}",server)
    df_cmap_data['time'] =pd.to_datetime(df_cmap_data['time'])
    # df_cmap_data['time_check'] = df_cmap_data['time'].dt.strftime("%Y-%m-%dT%H:%M:%S")

    df_doi_data.describe() ==df_cmap_data.describe()
    df_cmap_data_desc = df_cmap_data.describe()
    doi_hash = hash_pandas_object(df_doi_data).sum()
    cmap_hash = hash_pandas_object(df_cmap_data).sum()

    if doi_hash != cmap_hash:
        print(f"### DOI and CMAP differ for {tbl}")
        for ca in df_cmap_data.columns.to_list():
            if ca in df_cmap_data_desc.columns.to_list():
                if not math.isclose(df_doi_data[ca].sum(), df_cmap_data[ca].sum(), abs_tol = 0.0001):
                    print(f"### Mismatch in column {ca}")
            elif hash_pandas_object(df_doi_data[ca]).sum()!= hash_pandas_object(df_cmap_data[ca]).sum():
                print(f"### Mismatch in non-numeric column {ca}")
                print(set(df_doi_data[ca]).symmetric_difference(df_cmap_data[ca]))
    else:
        print(f"### DOI check complete for {tbl}")
    os.remove(base_folder + df_ref["Entity_Name"][0]) 


def pycmapChecks(tbl):
    print("Starting pycmap checks...")    
    df_api_head = api.head(tbl) ## default is top 5
    df_qry_head = api.query(f'SELECT TOP 5 * FROM {tbl}')

    if api.is_climatology(tbl):
        print("Set as climatology")

    if not df_api_head.equals(df_qry_head):
        print('######## Check top 5')

    col_list =  api.columns(tbl) ## columns in sql table
    # col_list = col_df['Columns'].to_list() ## old pycmap format

    df_meta = api.get_dataset_metadata(tbl)
    exclude_list = ['time', 'lat', 'lon', 'depth']
    col_check_list = [x for x in col_list if x not in exclude_list]

    missing_vars = [x for x in df_meta.Variable.to_list() if x not in col_check_list]
    if len(missing_vars) > 0:
        print(f'######## Missing variables from metadata: {missing_vars}')

    ### Cluster datasets too large select * 
    ### Check table size before downloading full dataset
    ### get_var_coverage validates var is in tblVariables, errors for lat/lon/time/depth
    ds_id = max(df_meta['Dataset_ID'])
    qry = f"select JSON_VALUE(JSON_stats,'$.lat.count') lat_count from tblDataset_Stats where dataset_id = {ds_id}"
    stats_lat_count = api.query(qry)
    if stats_lat_count['lat_count'][0] < 2000000:
        qry = f'select * from {tbl}'
        df = api.query(qry)
        df_desc = df.describe()

        stat_col = ['Variable_Min', 'Variable_Max', 'Variable_Mean', 'Variable_Std', 'Variable_Count', 'Variable_25th', 'Variable_50th', 'Variable_75th']
        desc_index = ['min', 'max', 'mean', 'std','count', '25%', '50%', '75%']
        stats_dict = dict(zip(stat_col, desc_index))
        
        for row in tqdm(df_meta.itertuples()):
            var_cov = api.get_var_coverage(tbl, row.Variable)
            var_res = api.get_var_resolution(tbl, row.Variable)
            api.is_grid(tbl, row.Variable)
            pass_list = []
            pass_list.append(row.Time_Min == var_cov.Time_Min[0])
            pass_list.append(row.Time_Max == var_cov.Time_Max[0])
            pass_list.append(row.Lat_Min == var_cov.Lat_Min[0])
            pass_list.append(row.Lat_Max == var_cov.Lat_Max[0])
            pass_list.append(row.Lon_Min == var_cov.Lon_Min[0])
            pass_list.append(row.Lon_Max == var_cov.Lon_Max[0])
            pass_list.append(row.Depth_Min == var_cov.Depth_Min[0])
            pass_list.append(row.Depth_Max == var_cov.Depth_Max[0])
            pass_list.append(api.has_field(tbl, row.Variable))
            var_stats = api.get_var_stat(tbl, row.Variable)
            stats_pass = []
            if row.Variable in df_desc.columns.to_list():
                for s,d in stats_dict.items():
                    ## Check that floats are close
                    stats_pass.append(math.isclose(df_desc[row.Variable][d], var_stats[s][0], abs_tol = 0.0001))
            if not any(pass_list):
                print(f'####Coverage test failed for {row.Variable}')
            if not any(stats_pass) and len(stats_pass)>0:
                print(f'#####Stats test failed for {row.Variable}')
    else:
        print(f"Pycmap stats and coverage tests skipped. Dataset > 2million rows")  

# def pycmapViz(tbl):
#     df_meta = api.get_dataset_metadata(tbl)
#     for row in df_meta.itertuples():
#         var_cov = api.get_var_coverage(tbl, row.Variable)
#         viz.plot_hist([tbl], [row.Variable], var_cov.Time_Min[0], var_cov.Time_Max[0], var_cov.Lat_Min[0], var_cov.Lat_Max[0], var_cov.Lon_Min[0], var_cov.Lon_Max[0], var_cov.Depth_Min[0], var_cov.Depth_Max[0], exportDataFlag=False, show=True)
#         df_space_time = api.space_time(tbl, row.Variable, var_cov.Time_Min[0], var_cov.Time_Max[0], var_cov.Lat_Min[0], var_cov.Lat_Max[0], var_cov.Lon_Min[0], var_cov.Lon_Max[0], var_cov.Depth_Min[0], var_cov.Depth_Max[0])
#         viz.plot_map([tbl], [row.Variable], var_cov.Time_Min[0], var_cov.Time_Max[0], var_cov.Lat_Min[0], var_cov.Lat_Max[0], var_cov.Lon_Min[0], var_cov.Lon_Max[0], var_cov.Depth_Min[0], var_cov.Depth_Max[0])
#         viz.plot_timeseries([tbl], [row.Variable], var_cov.Time_Min[0], var_cov.Time_Max[0], var_cov.Lat_Min[0], var_cov.Lat_Max[0], var_cov.Lon_Min[0], var_cov.Lon_Max[0], var_cov.Depth_Min[0], var_cov.Depth_Max[0], exportDataFlag=False, show=True, interval=None)
#         ## plot_section() function only applies to gridded datasets with depth component (e.g. model outputs) and does not apply to sparse datasets
#         viz.plot_section([tbl], [row.Variable], var_cov.Time_Min[0], var_cov.Time_Max[0], var_cov.Lat_Min[0], var_cov.Lat_Max[0], var_cov.Lon_Min[0], var_cov.Lon_Max[0], var_cov.Depth_Min[0], var_cov.Depth_Max[0], exportDataFlag=False, show=True, levels=0)
        

def fullIngestPostChecks(tbl, doi_check = True):  
    db_id = DB.dbRead("Select max(ID) mxID from tblDatasets","Rainier")
    ## Run DB endpoint checks every 10 new datasets or updates
    if db_id['mxID'][0] % 10 ==0:
        print("Running DB API checks. This may take some time.")
        api_checks.postIngestAPIChecks()  
    alias_list = checkServerAlias(tbl)
    checkRegion(tbl, alias_list)
    for server in alias_list:
        if server != 'cluster':
            checkHasDepth(tbl,server)
    pycmapChecks(tbl)
    if doi_check:
        compareDOI(tbl,'rossby')



# tbl = 'tblHOT_AmoA_gene_abundances'
# tbl = 'tblTN397_Gradients4_15N13C'
# tbl = 'tblKM1906_Gradients3_Organic_Inorganic_Nutrients'

# api_checks.postIngestAPIChecks()
# server = 'rossby'
# compareDOI(tbl,'Rossby')

# pycmapChecks(tbl)
# checkHasDepth(tbl,'Rossby')
# checkServerAlias(tbl)