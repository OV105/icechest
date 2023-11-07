import pytest
from pathlib import Path
import sys

from icechest import icechest
CONTENT = "MY CONTENT"
def test_main():
    icechest.main()

@pytest.fixture
def test_file(tmp_path):
    test = tmp_path / "foo"
    test.write_text(CONTENT)
    return test

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
    
def test_freeze(tmp_path):
    src = tmp_path / "foo"
    src.write_text(CONTENT)
    vault = "bar"
    icechest.freeze(str(src), vault)

@pytest.mark.skip(reason="not implemented")
def test_main():
    args = ["--vault", "foo", "--source", "bar"]
    icechest.main(args)