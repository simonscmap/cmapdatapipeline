import pandas as pd
import os
import glob
import xarray as xr
from cmapingest import vault_structure as vs


# os.system("""wget -erobots=off -nv -m -np -nH --cut-dirs=100 --reject "index.html*,fileList.txt" https://www.nodc.noaa.gov/archive/arc0099/0156894/4.4/data/0-data/RR1604_605907_r2rctd/data/""")

xdf = xr.open_dataset(
    vs.collected_data + "RR1604_Phytoplankton/" + "IO_Phytoplankton.nc"
)
df = xdf.to_dataframe().reset_index()
# df["station"] = df["station"].astype(int).astype(str).str.zfill(3)
for col in list(df):
    try:
        df[col] = df[col].str.decode("utf-8")
    except:
        print(col)

df["station"] = df["station"].astype(int).astype(str).str.zfill(3)
df = df.drop(columns=["unlimited"])


ctd_list = glob.glob(vs.collected_data + "RR1604_Phytoplankton/" + "*.cnv*")


time_list = []
station_list = []
for ctd_fil in ctd_list:
    with open(ctd_fil, "r") as fil:
        data = fil.read()
        time = pd.to_datetime(
            data.split("start_time")[1]
            .split("bad_flag")[0]
            .split("= ")[1]
            .split(" [")[0]
        )
        station = data.split("** Station: ")[1].split("** Operator")[0].split("\n")[0]
        time_list.append(time)
        station_list.append(station)
umstdf = pd.DataFrame({"time": time_list, "station": station_list})

stdf["station"] = (
    stdf["station"].str.split("/", expand=True)[0].str.split("-", expand=True)[0]
)
stdf["station"] = stdf["station"].apply(lambda x: x[0:3])
stdf = stdf.drop_duplicates(subset=["station"], keep="first")

ndf = pd.merge(df, stdf, how="left", left_on="station", right_on="station")
ndf = pd.merge(df, stdf, how="inner", left_on="station", right_on="station")
ndf = pd.merge(df, stdf, how="inner")

ndf.to_parquet(
    vs.collected_data
    + "RR1604_Phytoplankton/"
    + "merged_ctd_station_ST_phytoplankton.parquet"
)

# for remove_file in ctd_list:
#     os.remove(remove_file)
