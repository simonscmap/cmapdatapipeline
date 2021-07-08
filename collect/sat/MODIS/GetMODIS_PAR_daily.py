import sys
import os
import urllib.request
import requests

from cmapingest import vault_structure as vs
from cmapingest import transfer

vs.makedir(vs.collected_data + "MODIS_PAR_daily_data")


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
    base_folder = vs.collected_data + "/MODIS_PAR_daily_data/"
    start_index = int(start_index) - 1
    end_index = int(end_index) - 1

    wget_str = (
        "wget --load-cookies ~/.urs_cookies --save-cookies ~/.urs_cookies --auth-no-challenge=on --keep-session-cookies --content-disposition https://oceandata.sci.gsfc.nasa.gov/cgi/getfile/A"
        + str(year)
        + str(start_index).zfill(3)
        + ".L3m_DAY_PAR_par_9km.nc"
    )

    os.chdir(base_folder)
    try:
        os.system(wget_str)
        os.rename(
            "A" + str(year) + str(start_index).zfill(3) + ".L3m_DAY_PAR_par_9km.nc.1",
            "A" + str(year) + str(start_index).zfill(3) + ".L3m_DAY_PAR_par_9km.nc",
        )
    except:
        print("No file found for date: " + str(year) + str(start_index).zfill(3))

    # url = (
    #     "https://oceandata.sci.gsfc.nasa.gov/cgi/getfile/A"
    #     + str(year)
    #     + str(start_index).zfill(3)
    #     + ".L3m_DAY_PAR_par_9km.nc"
    # )
    # path = base_folder + "MODIS_PAR_daily_" + str(year) + str(day).zfill(3) + ".nc"
    # print("Downloading: " + url)
    # print(path)
    # try:
    #     urllib.request.urlretrieve(url, path)
    # except:
    #     print(
    #         "No file found for date: "
    #         + str(year)
    #         + str(start_index).zfill(3)
    #         + str(year)
    #         + str(end_index)
    #     )


# year_list = range(2010, 2014, 1)
year_list = range(2014, 2015, 1)

startDay = int(1)
endDay = int(365)


for year in year_list:
    for day in range(startDay, endDay + 1):
        get_PAR(year, day)


# import sys
# import os
# import urllib.request
# import requests

# sys.path.append("../../config")
# import config_vault as cfgv

# sys.path.append("../../DBIngest/login")
# import credentials as cr

# """ 9km spatial resolution """

# def get_PAR(year, day):
#     start_index = day / 8
#     if day % 8 == 0:
#         start_index = start_index - 1
#     start_index = (start_index * 8) + 1

#     end_index = start_index + 7
#     if start_index == 361:
#         end_index = 365

#     if (year % 4 == 0) and (start_index == 361):
#         end_index = 366
#     base_folder = cfgv.rep_MODIS_PAR_daily_raw
#     start_index = int(start_index) - 1
#     end_index = int(end_index) - 1

#     wget_str = (
#         "wget --load-cookies ~/.urs_cookies --save-cookies ~/.urs_cookies --auth-no-challenge=on --keep-session-cookies --content-disposition https://oceandata.sci.gsfc.nasa.gov/cgi/getfile/A"
#         + str(year)
#         + str(start_index).zfill(3)
#         + ".L3m_DAY_PAR_par_9km.nc"
#     )

#     os.chdir(base_folder)
#     print("A" + str(year) + str(start_index).zfill(3) + ".L3m_DAY_PAR_par_9km.nc.1")
#     try:
#         os.system(wget_str)
#         os.rename(
#             "A" + str(year) + str(start_index).zfill(3) + ".L3m_DAY_PAR_par_9km.nc.1",
#             "A" + str(year) + str(start_index).zfill(3) + ".L3m_DAY_PAR_par_9km.nc",
#         )
#     except:
#         print(
#             "No file found for date: "
#             + str(year)
#             + str(start_index).zfill(3)
#             + str(year)
#             + str(end_index)
#         )


# # year_list = range(2015,2020, 1)
# year_list = range(2020, 2021, 1)

# startDay = int(1)
# endDay = int(365)


# for year in year_list:
#     for day in range(startDay, endDay + 1):
#         get_PAR(year, day)
