import os
from cmapingest import vault_structure as vs


hot_tsg_ftp_link = "https://hahana.soest.hawaii.edu/FTP/hot/tsg/"
wget_str = f"wget -e robots=off -r --no-parent {vs.collected_data + 'HOT_TSG/'} {hot_tsg_ftp_link}"
os.system(wget_str)
