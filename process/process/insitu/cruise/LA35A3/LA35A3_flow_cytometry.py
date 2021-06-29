import pandas as pd
import numpy as np
import xarray as xr
from cmapingest import vault_structure as vs
from cmapingest import common as cmn
from cmapingest import data

xdf = xr.open_dataset(vs.collected_data + "LA35A3_flow_cytometry/flowCyto.netcdf")
df = xdf.to_dataframe().reset_index(drop=True)
df = df.applymap(lambda x: x.decode() if isinstance(x, bytes) else x)
df.rename(columns={"ISO_DateTime_UTC": "time", "depth_w": "depth"}, inplace=True)
df = df[
    [
        "time",
        "lat",
        "lon",
        "depth",
        "cast",
        "bottle",
        "pro",
        "syn",
        "peuk",
        "bacteria",
        "protists",
    ]
]
for col in ["pro", "syn", "peuk", "bacteria", "protists"]:
    df[col].values[df[col].values > 1.0 * 10 ** 36] = np.nan
df = data.clean_data_df(df)
df.to_excel(vs.staging + "combined/" + "LA35A3_flow_cytometry.xlsx", index=False)
