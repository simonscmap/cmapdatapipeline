import os
from cmapingest import vault_structure as vs


odir = vs.collected_data + "insitu/cruise/misc_cruise/KM0704_CMORE_BULA/"


def download_KM0704_data(outputdir, download_link):
    wget_str = f"""wget -P '{outputdir}' -np -R "'index.html*" robots=off -nH --cut-dirs 8 -r  {download_link}"""
    os.system(wget_str)


# #download ctd
download_KM0704_data(
    odir + "CTD/", "https://hahana.soest.hawaii.edu/FTP/cmore/ctd/bula1/"
)

# download bottle
download_KM0704_data(
    odir + "bottle/", "https://hahana.soest.hawaii.edu/FTP/cmore/water/bula1/"
)

# download underway
download_KM0704_data(
    odir + "underway/", "https://hahana.soest.hawaii.edu/FTP/cmore/underway/bula1/"
)

# download wind
download_KM0704_data(
    odir + "wind/", "https://hahana.soest.hawaii.edu/FTP/cmore/winds/bula1"
)
