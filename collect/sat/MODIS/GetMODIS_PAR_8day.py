import sys
import os
import urllib.request
import requests

from cmapingest import vault_structure as vs
from cmapingest import transfer


def get_PAR(year, day):
    start_index = day / 8
    if day % 8 == 0:
        start_index = start_index - 1
    start_index = (start_index * 8) + 1

    end_index = start_index + 7
    if start_index == 361:
        end_index = 365

    if (year % 4 == 0) and (start_index == 361):
        end_index = 366
    base_folder = vs.collected_data + "/MODIS_PAR_8_day_data/"
    start_index = int(start_index) - 1
    end_index = int(end_index) - 1

    url = (
        "https://oceandata.sci.gsfc.nasa.gov/cgi/getfile/A"
        + str(year)
        + str(start_index).zfill(3)
        + str(year)
        + str(end_index).zfill(3)
        + ".L3m_8D_PAR_par_9km.nc"
    )
    print(url)
    path = base_folder + "MODIS_PAR_8_day_" + str(year) + str(day).zfill(3) + ".nc"
    print("Downloading: " + url)
    try:
        urllib.request.urlretrieve(url, path)
    except:
        print(
            "No file found for date: "
            + str(year)
            + str(start_index).zfill(3)
            + str(year)
            + str(end_index)
        )


year_list = range(2004, 2010, 1)
startDay = int(1)
endDay = int(365)


for year in year_list:
    for day in range(startDay, endDay + 1):
        pass
        get_PAR(year, day)
