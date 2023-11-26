import dataclasses  
import json
import os
import pytest
from pathlib import Path
import sys

from icechest import icechest
import botocore

CONTENT = "MY CONTENT"

VAULT_NAME = "TestOne"

def test_main():
    icechest.main()

@pytest.fixture
def test_file(tmp_path):
    test = tmp_path / "foo"
    test.write_text(CONTENT)
    return test

@pytest.fixture
def hist_file(tmp_path):
    hist_file = tmp_path / "hist.json"
    history = [dataclasses.asdict(icechest.GlacierJob("test", "123abc", 654321, VAULT_NAME, "STARTED"))]
    with open(hist_file, 'w') as hf:
        json.dump(history, hf)

    return hist_file

def set_aws_env():
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
    os.environ['AWS_ACCESS_KEY_ID'] = 'FOO'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'BAR'

def test_create_archive_file(test_file):
    archive = icechest.create_archive(str(test_file))
    archive_path = Path(f"{test_file}.zip")
    assert archive.path == archive_path
    assert archive_path.exists()
    assert archive.checksum is not None

def test_create_archive_no_file():
    with pytest.raises(FileNotFoundError):
        icechest.create_archive("FooBar")

def test_create_archive_dir(test_file):
    archive = icechest.create_archive(str(test_file.parent))
    archive_path = Path(f"{test_file.parent}.zip")
    assert archive.path == archive_path
    assert archive_path.exists()
    assert archive.checksum is not None
    
#@pytest.mark.skipif(os.getenv("AWS_SECRET_ACCESS_KEY") is not None, reason="AWS credentials set")
@pytest.mark.skip("")
def test_freeze_no_creds(tmp_path):
    src = tmp_path / "foo"
    src.write_text(CONTENT)
    with pytest.raises(botocore.exceptions.NoCredentialsError):
        icechest.freeze(str(src), VAULT_NAME)

#@pytest.mark.skipif(os.getenv("AWS_SECRET_ACCESS_KEY") is None, reason="AWS credentials NOT set")
def test_freeze(tmp_path):
    src = tmp_path / "foo"
    src.write_text(CONTENT)
    icechest.freeze(str(src), VAULT_NAME)

@pytest.mark.skipif(os.getenv("AWS_SECRET_ACCESS_KEY") is None, reason="AWS credentials NOT set")
def test_get_vaults():
    vaults = icechest.get_vaults()
    assert len(vaults) > 0

def test_get_resource():
    set_aws_env()
    res = icechest.get_resource()
    assert str(res) == 'glacier.ServiceResource()'

def test_save_job_existing_file(hist_file):
    job = icechest.GlacierJob("test_type", "def456", 123456, "my_vault", "STARTED")
    rtn = icechest.save_job(job, hist_file)
    assert rtn is not None
    assert len(rtn) == 2
    assert rtn[0]['timestamp'] == 654321
    assert rtn[1]['timestamp'] == 123456
    assert hist_file.exists()
    with open(hist_file, 'r') as jf:
        history = json.load(jf)
    assert len(history) == 2
    assert history[1]['timestamp'] == 123456
    assert history[0]['timestamp'] == 654321

def test_save_job_no_file(tmp_path):
    job = icechest.GlacierJob("test_type", "abc123", 123456, "my_vault", "STARTED")
    hist_file = tmp_path / "hist.json"
    assert not hist_file.exists()
    rtn = icechest.save_job(job, hist_file)
    assert rtn is not None
    assert len(rtn) == 1
    assert rtn[0]['timestamp'] == 123456
    assert hist_file.exists()
    with open(hist_file, 'r') as jf:
        history = json.load(jf)
    assert len(history) == 1
    assert history[0]['timestamp'] == 123456

@pytest.mark.skip(reason="not implemented")
def test_main():
    args = ["--vault", "foo", "--source", "bar"]
    icechest.main(args)