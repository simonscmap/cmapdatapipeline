"""
Author: Norland Raphael Hagen <norlandrhagen@gmail.com>
Date: 07-23-2021

cmapdata - mapping - dataset mapping functionallity. Using folium, adapted from pycmap.
"""


from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium import webdriver
import numpy as np

import folium
from folium.plugins import HeatMap, MarkerCluster


import time
import os

import DB
import common as cmn
import vault_structure as vs


def addLayers(m):
    """Adds webtiles to created folium map. Uses World_Imagery ESRI tile data."""
    tiles = "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}"
    folium.TileLayer(tiles=tiles, attr=(" ")).add_to(m)
    return m


def addMarkers(m, df):
    """Adds CircleMarker points to folium map"""

    mc = MarkerCluster(
        options={"spiderfyOnMaxZoom": "False", "disableClusteringAtZoom": "1"}
    )
    for i in range(len(df)):
        folium.CircleMarker(
            location=[df.lat[i], df.lon[i]],
            radius=5,
            color="darkOrange",
            opacity=0.5,
            fill=False,
        ).add_to(mc)
    mc.add_to(m)
    return m


def html_to_static(m, tableName):
    """Outputs folium map to html and static map"""
    m.save(vs.mission_icons + tableName + ".html")
    options = FirefoxOptions()
    options.add_argument("--headless")
    driver = webdriver.Firefox(options=options)
    fpath = os.getcwd()
    driver.get("file://" + vs.static + "mission_icons/" + tableName + ".html")
    container = driver.find_element_by_class_name("leaflet-control-attribution")
    driver.execute_script(
        "document.getElementsByClassName('leaflet-control-attribution')[0].style.display = 'none';"
    )
    driver.execute_script(
        "document.getElementsByClassName('leaflet-control-container')[0].style.display = 'none';"
    )
    driver.execute_script(
        "document.getElementsByClassName('leaflet-bottom leaflet-left')[0].style.display = 'none';"
    )
    time.sleep(4)
    driver.save_screenshot(vs.mission_icons + tableName + ".png")
    driver.close()


def folium_map(df, tableName):
    """Creates folium map object from input DataFrame"""
    print("Creating Icon and Map..")
    if len(df) > 5000:
        df = df.sample(5000)
    df.reset_index(drop=True, inplace=True)
    mapdata = list(zip(df.lat, df.lon))
    m = folium.Map(
        [df.lat.mean(), df.lon.mean()],
        tiles=None,
        min_zoom=4,
        max_zoom=10,
        zoom_start=7,
        prefer_canvas=True,
    )
    sw = df[["lat", "lon"]].min().values.tolist()
    ne = df[["lat", "lon"]].max().values.tolist()

    lat_abs = np.abs(np.max(df["lat"]) - np.min(df["lat"]))
    lon_abs = np.abs(np.max(df["lon"]) - np.min(df["lon"]))
    if lat_abs > 1 or lon_abs > 1:
        m.fit_bounds([sw, ne])

    m = addLayers(m)
    HeatMap(mapdata, gradient={0.65: "#0A8A9F", 1: "#5F9EA0"}).add_to(m)
    m = addMarkers(m, df)
    html_to_static(m, tableName)
