"""strategy:
two tables:
BGC and Core
two main functions
    -read index file:
    -list of files from index:
        -iterate through and grab

process all files with gnu paralell or?
"""


"""
multiprocessing ex:
import multiprocessing as mp

def do_the_work(filename):
    pass

master_list = [file1, file2, fil3, ...filen]
with mp.Pool() as pool:
    results = pool.map(do_the_work, master_list, chunksize=1)
"""

from cmapingest import DB
from cmapingest import vault_structure as vs
import pandas as pd
import xarray as xr



def get_argo_core()):
    """Returns a list of argo float IDs from the core index

    Returns:
        list: list of argo float ID's that are core specific
    """
    df = pd.read_csv("argo_synthetic-profile_index.txt",sep=',',skiprows=8)
    #splits float name column into only float ID. aoml/1900722/profiles/SD1900722_001.nc -> daac/1900722/
    core_file_names = df['file'].str.split('profiles',expand=True)[0].unique()
    return core_file_names

def get_argo_synthetic():
    """Returns a list of argo float IDs from the synthetic index

    Returns:
        list: list of argo float ID's that are synthetic
    """
    df = pd.read_csv("argo_synthetic-profile_index.txt",sep=',',skiprows=8)
    #splits float name column into only float ID. aoml/1900722/profiles/SD1900722_001.nc -> daac/1900722/
    syn_file_names = df['file'].str.split('profiles',expand=True)[0].unique()
    return syn_file_names