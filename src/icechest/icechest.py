import argparse
import dataclasses
from dataclasses import dataclass
import hashlib
import json
import logging
import os
from pathlib import Path
import sys
import shutil
import time
from typing import Optional
from zipfile import ZipFile

import boto3
import botocore

JOB_HISTORY_FILE = "icebox.json"

@dataclass
class ArchiveFile:
    path: Path
    checksum: str

@dataclass
class GlacierArchive:
    local_path: Path
    checksum: str
    vault_name: str
    id: str

@dataclass
class GlacierJob:
    type: str
    id: str
    timestamp: int
    vault_name: str
    status: str 
    data: str = ""

    def set_data(self, obj: object) -> None:
        self.data = str(object)

def thaw(vault:str, archive:str) -> str:
    pass

def create_archive(source:str) -> ArchiveFile:
    """create ZIP file from source file or directory.

    Args:
        source (str): path to be added to archive file

    Returns:
        ArchiveFile: path to archive file and checksum
    """
    src = Path(source)
    archive_path = src.parent / f"{src.name}.zip"
    logging.debug(f"{archive_path = }")
    if src.is_dir():
        shutil.make_archive(archive_path.with_suffix(''), 'zip', src)
    else:
        start_dir = os.getcwd()
        os.chdir(src.parent)
        with ZipFile(archive_path, mode='w') as zip_file:
            zip_file.write(src.name)
        os.chdir(start_dir)

    hash_md5 = hashlib.md5()
    with open(archive_path, 'rb') as fp:
        for buffer in iter(lambda: fp.read(4096), b""):
            hash_md5.update(buffer)

    digest = hash_md5.hexdigest()
    logging.debug(f"{digest = }") 
    return ArchiveFile(archive_path, digest) 

def get_resource(region_name: str=None) -> object:
    """get glacier resource

    Args:
        region_name (str, optional): AWS region. Defaults to None.

    Returns:
        object: glacier.ServiceResource
    """
    if region_name is None:
        region_name = os.getenv('AWS_DEFAULT_REGION', default='us-east-1')

    logging.debug(f"{region_name =}")
    return boto3.resource("glacier", region_name=region_name)

def get_vault_resource(vault_name: str, region_name: str=None) -> object:
    resource = get_resource(region_name)
    return resource.Vault('-', vault_name)

def save_job(job: GlacierJob, hist_file_path: Path=None) -> str:
    if hist_file_path is None:
        hist_file_path = Path().expanduser() / JOB_HISTORY_FILE

    logging.debug(f"Updating {hist_file_path} with {job}")
    try:
        with open(hist_file_path, 'r') as jf:
            history = json.load(jf)
    except FileNotFoundError:
        history = [] 

    history.append(dataclasses.asdict(job))
    with open(hist_file_path, 'w') as jf:
        jf.write(json.dumps(history))

    return history

def get_inprogress_jobs(job_type: str) -> list[str]:
    return []

def freeze(source:str, vault_name:str, aws_region: Optional[str]=None) -> str:
    """Create Zip file containing file(s) located at source and save 
       Zip in AWS Glacier vault.

    Args:
        source (str): path to file or directory
        vault (str): name of Glacier vault

    Returns:
        str: zip file path
    """
    archive = create_archive(source)
    vault = get_vault_resource(vault_name, aws_region)
    frozen = vault.upload_archive(archiveDescription=f"", body=str(archive.path))
    logging.info(f"Uploaded {archive.path} with ID: {frozen.id} to {vault.name}")
    return GlacierArchive(archive.path, archive.checksum, vault_name, frozen.id)
    
def get_vaults(region_name=None):
    resource = get_resource("glacier", region_name=region_name) 
    return resource.vaults.all()

def print_vault_names(region_name=None):
    for vault in get_vaults(region_name):
        print(f"Vault name: {vault.name}")

def save_item(item):
    pass

def retrieve_items(item):
    pass


def main(args_in):
    """process command line arguments.

    Args:
        args_in (list[str]): list of command line arguments
    """
    parser = argparse.ArgumentParser(description="Parse arguments")
    parser.add_argument('-v', '--vault', help='Glacier vault name')
    parser.add_argument('-s', '--source', help='File or directory to upload')
    args = parser.parse_args(args_in)
    logging.basicConfig(filename="icechest.log",  
                        format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s',
                        datefmt='%H:%M:%S')
    try:
        archive = freeze(args.source, args.vault)
    except botocore.exceptions.NoCredentialsError as ex:
        print(f"Unable to find AWS credentials, set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables or run `aws configure` command.")
    except Exception as ex:
        logging.exception(f"{source =} {vault_name =}")

if __name__ == "__main__":
    main(sys.argv[:1])