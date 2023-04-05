import sys
import os
from tqdm import tqdm
import datetime

sys.path.append("ingest")

from ingest import vault_structure as vs

tbl = 'tblPisces_DailyForecast'

base_folder = f'{vs.model}{tbl}/raw/'

vs.leafStruc(vs.model+tbl)

output_dir = base_folder.replace(" ", "\\ ")


def wget_file(output_dir, yr, usr, psw):
    fpath = f"ftp://nrt.cmems-du.eu/Core/GLOBAL_ANALYSIS_FORECAST_BIO_001_028/global-analysis-forecast-bio-001-028-daily/{yr}/*"
    os.system(
        f"""wget --no-parent -nd -r -m --ftp-user={usr} --ftp-password={psw} {fpath}  -P  {output_dir}"""
    )

user ='' 
pw = ''


end_date = datetime.date.today()
end_date = datetime.date(2022, 2, 5)
delta = datetime.timedelta(days=1)
max_date += delta

year_list = range(1993, 2023, 1)

for year in tqdm(year_list):
    yr = str(year)
    wget_file(output_dir, yr, user, pw)
        



