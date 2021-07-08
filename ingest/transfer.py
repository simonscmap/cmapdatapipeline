import os
import sys
import glob
import shutil
import pandas as pd
import requests
from tqdm import tqdm
import dropbox
import credentials as cr

import vault_structure as vs


def requests_Download(download_str, filename, path):
    r = requests.get(download_str, stream=True)
    with open(path + filename, "wb") as f:
        f.write(r.content)


def clear_directory(directory):
    try:
        flist = glob.glob(directory + "*")
        [os.remove(fil) for fil in flist]
    except:
        pass


def Zenodo_DOI_Formatter(DOI, filename):
    doi_record = DOI.split("zenodo.")[1]
    doi_download_str = "https://zenodo.org/record/{doi_record}}/files/{filename}}?download=1".format(
        doi_record=doi_record, filename=filename
    )
    return doi_download_str


def staging_to_vault(
    filename, branch, tableName, remove_file_flag=True, process_level="REP"
):

    """
    Transfers a file from staging to vault rep or nrt.
    removes file from staging on successful transfer

    Parameters
    ----------
    filename : string
        Filename and extension to be transfered.
    branch : string
        Vault organization path: ex: vs.cruise
    tableName : string
        SQL tableName
    remove_file_flag : bool, default True, optional
        Flag option for removing input file from staging
    process_level : str, default REP, optional
        Place the data in the REP or the NRT


    """
    nrt_tree, rep_tree, metadata_tree, stats_tree, doc_tree, code_tree = vs.leafStruc(
        branch + tableName
    )
    base_filename = os.path.splitext(os.path.basename(filename))[0]

    clear_directory(rep_tree)
    clear_directory(nrt_tree)
    clear_directory(metadata_tree)

    data_fname = vs.staging + "data/" + base_filename + "_data.csv"
    dataset_metadata_fname = (
        vs.staging + "metadata/" + base_filename + "_dataset_metadata.csv"
    )
    vars_metadata_fname = (
        vs.staging + "metadata/" + base_filename + "_vars_metadata.csv"
    )

    if process_level.lower() == "nrt":
        shutil.copyfile(data_fname, nrt_tree + base_filename + "_data.csv")
    else:
        shutil.copyfile(data_fname, rep_tree + base_filename + "_data.csv")

    shutil.copyfile(
        dataset_metadata_fname, metadata_tree + base_filename + "_dataset_metadata.csv"
    )
    shutil.copyfile(
        vars_metadata_fname, metadata_tree + base_filename + "_vars_metadata.csv"
    )

    if remove_file_flag == True:
        os.remove(data_fname)
        os.remove(dataset_metadata_fname)
        os.remove(vars_metadata_fname)


def single_file_split(filename, metadata_filename):
    """
    #If metadata_filename is provided, ds,vars split, data file just transfered.
    else:
        filename is split into all three files.

    Splits an excel file containing data, dataset_metadata and vars_metadata sheets
    into three seperate files in the staging file strucutre.
    If additional metadata filename is provided, data is split.

    Parameters
    ----------
    filename : string
        Filename and extension to be split.
    metadata_filename : string (optional)
        Filename of metadata specific file.
    """

    base_filename = os.path.splitext(os.path.basename(filename))[0]
    if metadata_filename == None:
        metadata_filename = filename
        data_df = pd.read_excel(vs.combined + filename, sheet_name=0)

    else:
        data_df = pd.read_csv(vs.data + filename)
    #
    # print(vs.metadata + metadata_filename)
    dataset_metadata_df = pd.read_excel(
        vs.combined + metadata_filename, sheet_name="dataset_meta_data"
    )
    vars_metadata_df = pd.read_excel(
        vs.combined + metadata_filename, sheet_name="vars_meta_data"
    )

    dataset_metadata_df.to_csv(
        vs.metadata + base_filename + "_dataset_metadata.csv", sep=",", index=False
    )
    vars_metadata_df.to_csv(
        vs.metadata + base_filename + "_vars_metadata.csv", sep=",", index=False
    )
    data_df.to_csv(vs.data + base_filename + "_data.csv", sep=",", index=False)

    # os.remove(vs.combined + filename)


def remove_data_metadata_fnames_staging(staging_sep_flag="combined"):
    if staging_sep_flag == "combined":
        for base_filename in os.listdir(vs.combined):
            os.rename(
                vs.combined + base_filename,
                vs.combined + base_filename.replace("data", ""),
            )
            os.rename(
                vs.combined + base_filename,
                vs.combined + base_filename.replace("metadata", ""),
            )
            os.rename(
                vs.combined + base_filename,
                vs.combined + base_filename.replace("meta_data", ""),
            )
    else:
        for base_filename in os.listdir(vs.data):
            os.rename(
                vs.data + base_filename, vs.data + base_filename.replace("data", "")
            )
        for base_filename in os.listdir(vs.metadata):
            os.rename(
                vs.metadata + base_filename,
                vs.metadata + base_filename.replace("metadata", ""),
            )


def dropbox_file_transfer(input_file_path, output_file_path):

    """
    Transfers a file to dropbox using the dropbox v2 python api

    Parameters
    ----------
    input_file_path : string
        Input filepath, filename and extension to be transfered.
    output_file_path : string
        Output filepath, filename and extension to be transfered.
    """
    dbx = dropbox.Dropbox(cr.dropbox_api_key, timeout=900)
    chunk_size = 1024 * 1024
    with open(input_file_path, "rb") as f:
        file_size = os.path.getsize(input_file_path)
        if file_size <= chunk_size:
            dbx.files_upload(
                f.read(), output_file_path, mode=dropbox.files.WriteMode.overwrite
            )
        else:
            with tqdm(total=file_size, desc="%transfer") as pbar:
                upload_session_start_result = dbx.files_upload_session_start(
                    f.read(chunk_size)
                )
                pbar.update(chunk_size)
                cursor = dropbox.files.UploadSessionCursor(
                    session_id=upload_session_start_result.session_id, offset=f.tell(),
                )
                commit = dropbox.files.CommitInfo(path=output_file_path)

                while f.tell() < file_size:
                    if (file_size - f.tell()) <= chunk_size:
                        dbx.files_upload_session_finish(
                            f.read(chunk_size), cursor, commit
                        )

                    else:
                        dbx.files_upload_session_append(
                            f.read(chunk_size), cursor.session_id, cursor.offset,
                        )
                        cursor.offset = f.tell()
                    pbar.update(chunk_size)
