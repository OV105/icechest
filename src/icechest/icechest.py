import argparse
from dataclasses import dataclass
import hashlib
from pathlib import Path
import logging
import os
import sys
import shutil
from zipfile import ZipFile

@dataclass
class ArchiveFile:
    path: Path
    checksum: str

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

def freeze(source:str, vault:str) -> str:
    """Create Zip file containing file(s) located at source and save 
       Zip in AWS Glacier vault.

    Args:
        source (str): path to file or directory
        vault (str): name of Glacier vault

    Returns:
        str: zip file path
    """
    archive = create_archive(source)
    return archive
    

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
    freeze(args.source, args.vault)

if __name__ == "__main__":
    main(sys.argv[:1])