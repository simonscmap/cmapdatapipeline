##new version as of june 16th, 2021.
# strategy is download all, unzip, then use index to choose files for processing

from cmapingest import vault_structure as vs
import os


june_10_link = "https://www.seanoe.org/data/00311/42182/data/85023.tar.gz"

output_dir = vs.collected_data + "insitu/float/ARGO/"


def wget_file(fpath, output_dir):
    os.system("wget " + fpath + " -P " + output_dir)


# wget_file(june_10_link, output_dir)


# # DEV NOTe JAN 21ST: from https://argo.ucsd.edu/data/data-faq/#RorD, only use
# # The _prof suffix contains *all* profile data. In the future, use wget? to get not all profiles


# import urllib
# import requests
# import shutil
# import glob
# import os
# import ftplib
# import pandas as pd
# from cmapingest import vault_structure as vs
# from tqdm import tqdm


# def download_zipped_argo(link,outputdir):
#     local_filename = link.split('/')[-1]
#     with requests.get(link, stream=True) as r:
#         with open(outputdir + local_filename, 'wb') as f:
#             shutil.copyfileobj(r.raw, f)
#     return local_filename


# may_2021_argo_link = 'https://www.seanoe.org/data/00311/42182/data/83717.tar.gz'


# base_argo_ftp = "ftp://ftp.ifremer.fr/ifremer/argo/"
# base_argo_collected_data = vs.collected_data + "ARGO/"
# argo_bgc_collected_data = vs.collected_data + "ARGO/BGC/"
# argo_core_collected_data = vs.collected_data + "ARGO/Core/"
# argo_index_dir = vs.collected_data + "ARGO/Index/"

# arglobal = "ar_index_global_prof.txt"
# arsyn = "argo_synthetic-profile_index.txt"


# # download_zipped_argo(may_2021_argo_link,base_argo_collected_data)

# def gather_index_from_ftp():
#     wget_file(base_argo_ftp + arglobal, argo_index_dir)
#     wget_file(base_argo_ftp + arbio, argo_index_dir)
#     wget_file(base_argo_ftp + arsyn, argo_index_dir)


# def return_basename_from_filepath_list(filepath_list):
#     basenamelist = [os.path.basename(x) for x in filepath_list]
#     return basenamelist


# def get_float_ID_from_vault(directory):
#     """returns list of all files in directory"""
#     filelist = glob.glob(directory + "*.nc")
#     basenamelist = return_basename_from_filepath_list(filelist)
#     vault_float_ids = pd.Series(basenamelist).str.split("_", expand=True)[0].to_list()
#     return vault_float_ids


# def get_float_ID_and_daac_from_vault(directory):
#     """returns list of all files in directory"""
#     filelist = glob.glob(directory + "**/*.nc", recursive=True)
#     if len(filelist) > 0:
#         filelist_df = pd.DataFrame({"filepath": filelist})
#         daac_float_id_df = (
#             filelist_df["filepath"].str.rsplit("/", n=2, expand=True).iloc[:, 1:3]
#         )
#         daac_and_id = (
#             daac_float_id_df.iloc[:, 0].astype(str)
#             + "/"
#             + daac_float_id_df.iloc[:, 1]
#             .str.split("_", n=2, expand=True)
#             .iloc[:, 0]
#             .astype(str)
#         )
#     else:
#         daac_and_id = []

#     return daac_and_id


# def get_float_ID_from_index(index_filepath):
#     df = pd.read_csv(index_filepath, skiprows=8, sep=",")
#     # splits the file column to only get the float ID
#     filelist = df["file"].str.split("/", n=3, expand=True)[1]
#     basenamelist = return_basename_from_filepath_list(filelist)
#     return basenamelist


# def get_float_ID_and_daac_from_index(index_filepath):
#     df = pd.read_csv(index_filepath, skiprows=8, sep=",")
#     # splits the file column to get the float ID and DAAC
#     daac_float_id_df = df["file"].str.split("/", n=3, expand=True).iloc[:, 0:2]
#     daac_and_id = (
#         daac_float_id_df.iloc[:, 0].astype(str)
#         + "/"
#         + daac_float_id_df.iloc[:, 1].astype(str)
#     )
#     return daac_and_id


# def wget_file(fpath, output_dir):
#     os.system("wget " + fpath + " -P " + output_dir)

# def diff_index_vault(argo_dir, index_filepath):
#     """This function takes in either BGC or Core and checks if there are mismatches/missing files between the index file and the files in vault"""
#     # index_ids = get_float_ID_from_index(index_filepath)
#     # vault_ids = get_float_ID_from_vault(argo_dir)
#     index_ids = get_float_ID_and_daac_from_index(index_filepath)
#     vault_ids = get_float_ID_and_daac_from_vault(argo_dir)
#     diff_set = list(set(index_ids) ^ set(vault_ids))

#     return diff_set


# # def format_ids_to_download_str():

# # syn_diff = diff_index_vault(argo_bgc_collected_data, argo_index_dir + arsyn)
# # core_diff = diff_index_vault(argo_core_collected_data, argo_index_dir + arglobal)


# # argo_dir = argo_bgc_collected_data
# # index_filepath = argo_index_dir + arsyn


# def wget_file_diff(file_diff, argo_dir, float_prefix):
#     for fil in tqdm(file_diff):
#         daac_name = fil.split("/")[0]
#         float_id = fil.split("/")[1]
#         fpath = (
#             base_argo_ftp
#             + "dac/"
#             + daac_name
#             + "/"
#             + float_id
#             + "/"
#             + float_id
#             + "_"
#             + float_prefix
#             + "prof.nc"
#         )
#         output_dir = argo_dir + daac_name + "/"
#         print(fpath)
#         wget_file(fpath, output_dir)


# def main_download(argo_float_type):
#     """ Main download function. Checks through relevent index to retrieve float_ID's, checks against files that exist. Downloads any missing"""
#     if argo_float_type.lower() == "core":
#         float_prefix = ""
#         index_fname = "ar_index_global_prof.txt"
#         argo_dir = base_argo_collected_data + "Core/"
#     elif argo_float_type.lower() == "bgc":
#         float_prefix = "S"
#         index_fname = "argo_synthetic-profile_index.txt"
#         argo_dir = base_argo_collected_data + "BGC/"
#     else:
#         print("ARGO input float type not found. exiting... ")
#         sys.exit()
#     file_diff = diff_index_vault(argo_dir, argo_index_dir + index_fname)

#     wget_file_diff(file_diff, argo_dir, float_prefix)


# """
# FTP_directory = "ftp://usgodae.org/pub/outgoing/argo/dac/"
# output_directory = "Vault/collected_data/ARGO/"
# FTP transfer used: filezilla
# """
