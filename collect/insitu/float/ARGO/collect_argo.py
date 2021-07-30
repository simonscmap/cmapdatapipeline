import vault_structure as vs
import os


june_10_link = "https://www.seanoe.org/data/00311/42182/data/85023.tar.gz"

output_dir = vs.collected_data + "insitu/float/ARGO/"


def wget_file(fpath, output_dir):
    os.system("wget " + fpath + " -P " + output_dir)

