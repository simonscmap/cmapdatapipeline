import pandas as pd
import numpy as np
import xarray as xr
from cmapingest import vault_structure as vs
from cmapingest import common as cmn
from cmapingest import data

xdf = xr.open_dataset(vs.collected_data + "KNOX22RR_flow_cytometry/picoplankton.nc")
df = xdf.to_dataframe().reset_index(drop=True)
df = df.applymap(lambda x: x.decode() if isinstance(x, bytes) else x)
df["time"] = pd.to_datetime(
    df["date"].astype(int).astype(str).str.zfill(8)
    + " "
    + df["time"].astype(int).astype(str),
    format="%Y%m%d %H%M%S",
)
df.rename(columns={"Depth": "depth"}, inplace=True)

df = df[
    [
        "time",
        "lat",
        "lon",
        "depth",
        "station",
        "Syn",
        "Pro",
        "Pico_Euk",
        "Total_Cyano",
        "Total_picophytoplankton",
        "HB",
    ]
]
df = data.clean_data_df(df)
df.to_excel(vs.staging + "combined/" + "KNOX22RR_flow_cytometry.xlsx", index=False)
