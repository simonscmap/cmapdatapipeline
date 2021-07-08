import sys
import os
import urllib.request
import requests

from cmapingest import vault_structure as vs


def get_POC(year, day):
    start_index = day / 8
    if day % 8 == 0:
        start_index = start_index - 1
    start_index = (start_index * 8) + 1

    end_index = start_index + 7
    if start_index == 361:
        end_index = 365
    if (year % 4 == 0) and (start_index == 361):
        end_index = 366
    base_folder = vs.collected_data + "MODIS_POC_8_day_data/"
    start_index = int(start_index) - 1
    end_index = int(end_index) - 1

    url = (
        "https://oceandata.sci.gsfc.nasa.gov/ob/getfile/A"
        + str(year)
        + str(start_index).zfill(3)
        + str(year)
        + str(end_index).zfill(3)
        + ".L3m_8D_POC_poc_9km.nc"
    )
    path = base_folder + "MODIS_POC_8_day_" + str(year) + str(day).zfill(3) + ".nc"
    print("Downloading: " + url)
    result = requests.get(url)
    try:
        result.raise_for_status()
        f = open(path, "wb")
        f.write(result.content)
        f.close()
        # print('contents of URL written to '+path)
    except:
        # print('requests.get() returned an error code '+str(result.status_code))
        print(
            "No file found for date: "
            + str(year)
            + str(start_index).zfill(3)
            + str(year)
            + str(end_index)
        )


year_list = range(2002, 2020, 1)
startDay = int(1)
endDay = int(365)

for year in year_list:
    for day in range(startDay, endDay + 1):
        get_POC(year, day)
