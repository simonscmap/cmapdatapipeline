import os
from pathlib import Path


def makedir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)
    return


def leafStruc(base):
    makedir(base)
    if base[-1] != "/":
        base = base + "/"
    nrt = base + "nrt/"
    makedir(nrt)
    rep = base + "rep/"
    makedir(rep)
    metadata = base + "metadata/"
    makedir(metadata)
    stats = base + "stats/"
    makedir(stats)
    doc = base + "doc/"
    makedir(doc)
    code = base + "code/"
    makedir(code)
    return nrt, rep, metadata, stats, doc, code


"""synced dropbox path"""
vault = r"/data/CMAP Data Submission Dropbox/Simons CMAP/vault/"
staging = r"/data/CMAP Data Submission Dropbox/Simons CMAP/staging/"
collected_data = r"/data/CMAP Data Submission Dropbox/Simons CMAP/collected_data/"
spatial_data = r"/data/CMAP Data Submission Dropbox/Simons CMAP/spatial_data/"
dataset_audit = r"/data/CMAP Data Submission Dropbox/Simons CMAP/dataset_audit/"
download_transfer = r"/data/CMAP Data Submission Dropbox/Simons CMAP/download_transfer/"


################# Static Mission Icon Directory ##################
static = r"/data/CMAP Data Submission Dropbox/Simons CMAP/static/"
mission_icons = static + r"mission_icons/"

################# Download Transfer Structure   ##################
makedir(download_transfer)

################# Collected Data Structure  ##################
makedir(collected_data)


################# Staging Area Structure  ##################
makedir(staging)

#########  data  #########
data = staging + "data/"
makedir(data)

########  metadata  ########
metadata = staging + "metadata/"
makedir(metadata)

########  combined (temporary)  ########
combined = staging + "combined/"
makedir(combined)


################## Simons CMAP vault structure ##################
makedir(vault)

#########  BCP  #########
BCP = vault + "BCP/"
makedir(BCP)

#########  assimilation  #########
assimilation = vault + "assimilation/"
makedir(assimilation)


###########  models  ##########
model = vault + "model/"
makedir(model)

#########  r2r2 cruise #########
r2r_cruise = vault + "r2r_cruise/"
makedir(r2r_cruise)

#########  observations  #########
obs = vault + "observation/"
makedir(obs)
in_situ = obs + "in-situ/"
makedir(in_situ)
remote = obs + "remote/"
makedir(remote)

#########  obs/in-situ/cruise  #########
cruise = in_situ + "cruise/"
makedir(cruise)

#########  obs/in-situ/station  #########
station = in_situ + "station/"
makedir(station)

#########  obs/in-situ/float  #########
float_dir = in_situ + "float/"
makedir(float_dir)

########  obs/remote/satellite  ########
satellite = remote + "satellite/"
makedir(satellite)
