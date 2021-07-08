import pandas as pd
import numpy as np
import xarray as xr
from cmapingest import vault_structure as vs
from cmapingest import common as cmn
from cmapingest import data

xdf = xr.open_dataset(
    vs.collected_data + "MV1008_flow_cytometry_picoeuk/picophyto_fcm_ship.netcdf"
)
df = xdf.to_dataframe().reset_index(drop=True)
df = df.applymap(lambda x: x.decode() if isinstance(x, bytes) else x)
df["time"] = pd.to_datetime(
    df["date_local"].astype(int).astype(str).str.zfill(8), format="%m%d%Y"
)
df = df[
    [
        "time",
        "lat",
        "lon",
        "depth",
        "event",
        "cast",
        "cycle",
        "niskin",
        "coccus_s",
        "peuk",
    ]
]
df = data.clean_data_df(df)
df.to_excel(
    vs.staging + "combined/" + "MV1008_flow_cytometry_picoeuk.xlsx", index=False
)
