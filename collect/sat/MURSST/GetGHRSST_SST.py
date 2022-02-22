import sys
import os
from tqdm import tqdm 
import datetime
import xarray as xr

sys.path.append("ingest")
from ingest import vault_structure as vs

tbl = 'tblSST_AVHRR_OI_NRT'
vs.leafStruc(vs.satellite + tbl)

## https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/AVHRR_OI-NCEI-L4-GLOB-v2.1/20190428120000-NCEI-L4_GHRSST-SSTblend-AVHRR_OI-GLOB-v02.0-fv02.1.nc
## https://podaac.jpl.nasa.gov/dataset/AVHRR_OI-NCEI-L4-GLOB-v2.1


start_date = datetime.date(2019, 4, 28)
end_date = datetime.date(2022, 1, 31)
delta = datetime.timedelta(days=1)

while start_date <= end_date:
    yr = start_date.year
    month = f"{start_date:%m}"
    day = f"{start_date:%d}"

    file_url = f'https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/AVHRR_OI-NCEI-L4-GLOB-v2.1/{yr}{month}{day}120000-NCEI-L4_GHRSST-SSTblend-AVHRR_OI-GLOB-v02.0-fv02.1.nc'
    save_path = f'{vs.satellite + tbl}/raw/{yr}{month}{day}120000-NCEI-L4_GHRSST-SSTblend-AVHRR_OI-GLOB-v02.0-fv02.1.nc'
    wget_str = f'wget --no-check-certificate "{file_url}" -O "{save_path}"'
    os.system(wget_str)
    ## Remove empty downloads
    if os.path.getsize(save_path) == 0:
        print(f'empty download for {yr}{month}{day}')
        os.remove(save_path)
    start_date += delta

## Download documentation
file_url='https://podaac-tools.jpl.nasa.gov/drive/files/OceanTemperature/ghrsst/docs/GDS20r5.pdf'
save_path = f'{vs.satellite + tbl}/doc/GDS20r5.pdf'
wget_str = f'wget --no-check-certificate "{file_url}" -O "{save_path}"'
os.system(wget_str)

file_url='https://podaac-tools.jpl.nasa.gov/drive/files/allData/ghrsst/docs/NCEI_AVHRR-OI_SST_ATBD.pdf'
save_path = f'{vs.satellite + tbl}/doc/NCEI_AVHRR-OI_SST_ATBD.pdf'
wget_str = f'wget --no-check-certificate "{file_url}" -O "{save_path}"'
os.system(wget_str)


## Testing
# n = f'{vs.satellite}{tbl}/raw/{yr}{month}{day}120000-NCEI-L4_GHRSST-SSTblend-AVHRR_OI-GLOB-v02.0-fv02.1.nc' 

# x = xr.open_dataset(n)
# x.dims
# x.data_vars
# x.attrs



