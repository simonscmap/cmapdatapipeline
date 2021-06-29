import os

wget_str = "wget -r --no-parent --reject "index.html*" http://batsftp.bios.edu/BATS/ctd/Excel/"


def download_BATS_CTD(wget_str):
  os.system(wget_str)


download_BATS_CTD(wget_str)
