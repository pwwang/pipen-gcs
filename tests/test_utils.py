import os
from datetime import datetime

import pytest
from pipen_gcs.utils import (
    _mtime,
    parse_gcs_uri,
    get_gs_type,
    update_plugin_data,
    get_plugin_data,
    get_gs_mtime,
    download_gs_file,
    download_gs_dir,
    clear_gs_file,
    clear_gs_dir,
    gs_dir_exists,
    gs_file_exists,
    upload_gs_file,
    upload_gs_dir,
    create_gs_dir,
)
from .conftest import BUCKET, dt


def test_mtime(bucket):
    blob = bucket.blob("localfile")
    assert _mtime(blob) == 0.0

    blob = bucket.get_blob("test.txt")
    assert _mtime(blob) == dt(2021, 1, 1)


@pytest.mark.parametrize(
    "uri, bckt, path",
    [
        ("gs://bucket/path/to/file", "bucket", "path/to/file"),
        ("gs://bucket/path/to/dir/", "bucket", "path/to/dir/"),
        ("gs://bucket/", "bucket", ""),
    ],
)
def test_parse_gs_uri(uri, bckt, path):
    assert parse_gcs_uri(uri) == (bckt, path)


@pytest.mark.parametrize(
    "uri, out",
    [
        (f"gs://{BUCKET}/", "bucket"),
        (f"gs://{BUCKET}/testdir/", "dir"),
        (f"gs://{BUCKET}/testdir/test1.txt", "file"),
        (f"gs://{BUCKET}/not/exist", "none"),
    ],
)
def test_get_gs_type(bucket, uri, out):
    assert get_gs_type(bucket.client, uri) == out


def test_update_get_plugin_data():
    class P:
        ...

    class J:
        def __init__(self, proc):
            self.proc = proc

    job = J(P())
    update_plugin_data(job, "localpath", "gs://bucket/path")
    assert get_plugin_data(job, "localpath") == "gs://bucket/path"
    assert P.__meta__["plugin_data"]["gcs"]["localpath"] == "gs://bucket/path"


def test_download_gs_file(bucket, tmp_path):
    tmpfile = tmp_path / "testx.txt"
    download_gs_file(bucket.client, f"gs://{BUCKET}/test.txt", tmpfile)
    assert tmpfile.read_text() == "test1"

    blob = bucket.get_blob("test.txt")
    assert tmpfile.stat().st_mtime == _mtime(blob)


def test_download_gs_file_cache(bucket, tmp_path):
    tmpfile = tmp_path / "testx.txt"
    tmpfile.write_text("testx")
    upload_gs_file(bucket.client, tmpfile, f"gs://{BUCKET}/testx.txt")
    download_gs_file(bucket.client, f"gs://{BUCKET}/testx.txt", tmpfile)
    assert tmpfile.read_text() == "testx"
    mtime = tmpfile.stat().st_mtime
    blob = bucket.get_blob("testx.txt")
    assert mtime == _mtime(blob)

    tmpfile2 = tmp_path / "new" / "testx.txt"
    tmpfile2.parent.mkdir()
    tmpfile2.write_text("testx")
    os.utime(tmpfile2, (mtime - 100, mtime - 100))
    # update mtime on cloud
    upload_gs_file(bucket.client, tmpfile2, f"gs://{BUCKET}/testx.txt")
    download_gs_file(bucket.client, f"gs://{BUCKET}/testx.txt", tmpfile, force=False)
    # mtime should not change
    assert mtime == tmpfile.stat().st_mtime

    download_gs_file(bucket.client, f"gs://{BUCKET}/testx.txt", tmpfile, force=True)
    # mtime should change
    assert mtime != tmpfile.stat().st_mtime


def test_download_gs_file_cache_naive_file(bucket, tmp_path):
    """No meta data on the file"""
    tmpfile = tmp_path / "naive.txt"
    download_gs_file(bucket.client, f"gs://{BUCKET}/naive.txt", tmpfile)
    mtime = tmpfile.stat().st_mtime
    assert mtime > 0

    # make local file newer
    os.utime(tmpfile, (mtime + 100, mtime + 100))
    # without force, it should not download
    download_gs_file(bucket.client, f"gs://{BUCKET}/naive.txt", tmpfile, force=False)
    assert mtime + 100 == tmpfile.stat().st_mtime


def test_download_gs_dir(bucket, tmp_path):
    tmpdir = tmp_path / "testdir"
    download_gs_dir(bucket.client, f"gs://{BUCKET}/testdir2/", tmpdir)

    t9 = tmpdir / "test9.txt"
    t21 = tmpdir / "test2" / "test1.txt"
    t22 = tmpdir / "test2" / "test2.txt"
    assert t9.read_text() == "test9"
    assert t21.read_text() == "test4"
    assert t22.read_text() == "test5"

    bucket = bucket

    test2_blob = bucket.get_blob("testdir2/test2/")
    assert (tmpdir / "test2").stat().st_mtime == _mtime(test2_blob)

    test9_blob = bucket.get_blob("testdir2/test9.txt")
    assert t9.stat().st_mtime == _mtime(test9_blob)

    test21_blob = bucket.get_blob("testdir2/test2/test1.txt")
    assert t21.stat().st_mtime == _mtime(test21_blob)

    test22_blob = bucket.get_blob("testdir2/test2/test2.txt")
    assert t22.stat().st_mtime == _mtime(test22_blob)


def test_download_gs_dir_cache(bucket, tmp_path):
    tmpdir = tmp_path / "testdirx"
    tmpdir.mkdir()
    (tmpdir / "test1.txt").write_text("test1")
    (tmpdir / "test2").mkdir()
    (tmpdir / "test2" / "test2.txt").write_text("test2")
    (tmpdir / "test2" / "test3.txt").write_text("test3")
    upload_gs_dir(bucket.client, tmpdir, f"gs://{BUCKET}/testdirx/")
    download_gs_dir(bucket.client, f"gs://{BUCKET}/testdirx/", tmpdir)
    assert (tmpdir / "test1.txt").read_text() == "test1"
    assert (tmpdir / "test2" / "test2.txt").read_text() == "test2"
    assert (tmpdir / "test2" / "test3.txt").read_text() == "test3"
    mtime = (tmpdir / "test2" / "test3.txt").stat().st_mtime
    blob = bucket.get_blob("testdirx/test2/test3.txt")
    assert mtime == _mtime(blob)

    tmpdir2 = tmp_path / "new" / "testdirx"
    tmpdir2.mkdir(parents=True)
    (tmpdir2 / "test1.txt").write_text("test1")
    (tmpdir2 / "test2").mkdir()
    (tmpdir2 / "test2" / "test2.txt").write_text("test2")
    (tmpdir2 / "test2" / "test3.txt").write_text("test3")
    os.utime((tmpdir2 / "test2" / "test3.txt"), (mtime - 100, mtime - 100))
    upload_gs_dir(bucket.client, tmpdir2, f"gs://{BUCKET}/testdirx/")
    download_gs_dir(bucket.client, f"gs://{BUCKET}/testdirx/", tmpdir, force=False)
    assert (tmpdir / "test2" / "test3.txt").stat().st_mtime == mtime

    download_gs_dir(bucket.client, f"gs://{BUCKET}/testdirx/", tmpdir, force=True)
    assert (tmpdir / "test2" / "test3.txt").stat().st_mtime != mtime


def test_get_gs_mtime(bucket):
    assert get_gs_mtime(bucket.client, f"gs://{BUCKET}/test.txt", 1) == dt(2021, 1, 1)
    assert get_gs_mtime(bucket.client, f"gs://{BUCKET}/testdir/", 1) == dt(2021, 1, 3)
    assert get_gs_mtime(bucket.client, f"gs://{BUCKET}/testdir2/", 1) == dt(2021, 1, 5)
    assert get_gs_mtime(bucket.client, f"gs://{BUCKET}/testdir2/", 3) == dt(2021, 1, 7)


def test_clear_gs_file(bucket):
    bucket = bucket
    blob = bucket.blob("test_to_clear.txt")
    blob.upload_from_string("test")
    assert clear_gs_file(bucket.client, f"gs://{BUCKET}/test_to_clear.txt")
    assert not gs_file_exists(bucket.client, f"gs://{BUCKET}/test_to_clear.txt")


def test_clear_gs_dir(bucket):
    bucket = bucket
    blob = bucket.blob("testdir_to_clear/test.txt")
    blob.upload_from_string("test")
    assert clear_gs_dir(bucket.client, f"gs://{BUCKET}/testdir_to_clear/")
    assert not gs_file_exists(bucket.client, f"gs://{BUCKET}/testdir_to_clear/test.txt")
    assert not gs_dir_exists(bucket.client, f"gs://{BUCKET}/testdir_to_clear/")


def test_upload_gs_file(bucket, tmp_path):
    tmpfile = tmp_path / "testx.txt"
    tmpfile.write_text("test12")
    mtime = dt(2021, 1, 1)
    os.utime(tmpfile, (mtime, mtime))

    upload_gs_file(bucket.client, tmpfile, f"gs://{BUCKET}/test_to_upload.txt")
    blob = bucket.get_blob("test_to_upload.txt")
    assert blob.download_as_bytes() == b"test12"
    assert tmpfile.stat().st_mtime == mtime
    assert clear_gs_file(bucket.client, f"gs://{BUCKET}/test_to_upload.txt")


def test_upload_gs_file2(bucket, tmp_path):
    """file should exist locally"""
    tmpfile = tmp_path / "test_upload_gs_file2" / "test.txt"
    tmpfile.parent.mkdir()
    tmpfile.write_text("gs_file")

    upload_gs_file(
        bucket.client,
        tmpfile,
        f"gs://{BUCKET}/test_upload_gs_file2/test.txt",
    )

    tmpfile2 = tmp_path / "test_upload_gs_file2" / "test2.txt"
    tmpfile2.write_text("gs_file2")
    upload_gs_dir(
        bucket.client,
        tmpfile2.parent,
        f"gs://{BUCKET}/test_upload_gs_file2",
    )
    assert tmpfile.exists()
    assert tmpfile2.exists()
    assert clear_gs_dir(bucket.client, f"gs://{BUCKET}/test_upload_gs_file2")


def test_upload_gs_dir(bucket, tmp_path):
    tmpdir = tmp_path / "testdir"
    tmpdir.mkdir()
    (tmpdir / "test1.txt").write_text("test1")
    (tmpdir / "test2").mkdir()
    (tmpdir / "test2" / "test2.txt").write_text("test2")
    (tmpdir / "test2" / "test3.txt").write_text("test3")
    mtime = datetime.now().timestamp() + 86400
    os.utime((tmpdir / "test2" / "test3.txt"), (mtime, mtime))

    upload_gs_dir(bucket.client, tmpdir, f"gs://{BUCKET}/testdir_to_upload/")
    blob = bucket.get_blob("testdir_to_upload/test1.txt")
    assert blob.download_as_bytes() == b"test1"
    assert (tmpdir / "test1.txt").stat().st_mtime == _mtime(blob)

    blob = bucket.get_blob("testdir_to_upload/test2/test2.txt")
    assert blob.download_as_bytes() == b"test2"
    assert (tmpdir / "test2" / "test2.txt").stat().st_mtime == _mtime(blob)

    blob = bucket.get_blob("testdir_to_upload/test2/test3.txt")
    assert blob.download_as_bytes() == b"test3"
    assert (tmpdir / "test2" / "test3.txt").stat().st_mtime == _mtime(blob)

    assert get_gs_mtime(bucket.client, f"gs://{BUCKET}/testdir_to_upload/", 3) == mtime
    assert clear_gs_dir(bucket.client, f"gs://{BUCKET}/testdir_to_upload/")


def test_create_gs_dir(bucket):
    # already exists
    create_gs_dir(bucket.client, f"gs://{BUCKET}/testdir/")

    create_gs_dir(bucket.client, f"gs://{BUCKET}/testdir_to_create/")
    blob = bucket.get_blob("testdir_to_create/")
    assert blob.download_as_bytes() == b""
    assert blob.content_type == "application/x-directory"
    assert get_gs_type(bucket.client, f"gs://{BUCKET}/testdir_to_create/") == "dir"
    assert clear_gs_dir(bucket.client, f"gs://{BUCKET}/testdir_to_create/")
