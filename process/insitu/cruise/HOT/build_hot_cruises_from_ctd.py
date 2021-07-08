import pandas as pd
from cmapingest import DB


def gather_hot_cruise_traj_from_HOT_CTD():
    dbRead("""SELECT cruise_number, time, lat, lon FROM tblHOT_CTD""", server="Mariana")
