import os
import sys
import pytest

from pipen import Proc, Pipen
from pipen_gcs import InvalidGoogleStorageURIError
from pipen_gcs.utils import gs_file_exists
from .conftest import BUCKET


@pytest.mark.forked
def test_pipeline_localize(bucket, tmp_path, caplog):
    infile = f"gs://{BUCKET}/test.txt"
    indir = f"gs://{BUCKET}/testdir"
    outfile = f"gs://{BUCKET}/out/out.txt"

    class TestProc(Proc):
        input = "infile:file,indir:dir"
        input_data = [(infile, indir)]
        output = f"outfile:file:{outfile}"
        script = "cp {{in.infile}} {{out.outfile}}"

    class TestProc2(Proc):
        """Mixed gs input and local output"""

        requires = TestProc
        input = "infile:file"
        output = "outfile:file:out2.txt"
        script = "cp {{in.infile}} {{out.outfile}}"

    class TestProc3(Proc):
        """Mixed local input and gs output"""

        requires = TestProc2
        input = "infile:file"
        output = f"outdir:dir:gs://{BUCKET}/out3"
        script = "cp {{in.infile}} {{out.outdir}}/out3.txt"

    p = Pipen(
        name="GcsLocalize",
        workdir=tmp_path / "pipen",
        outdir=tmp_path / "outdir",
        loglevel="DEBUG",
        plugin_opts={
            "gcs_credentials": os.environ["GOOGLE_APPLICATION_CREDENTIALS"],
            "gs_localize": tmp_path / "gslocal",
        },
    ).set_start(TestProc)
    p.run()
    assert gs_file_exists(bucket.client, outfile)

    blob = bucket.get_blob("out/out.txt")
    assert blob.download_as_bytes() == b"test1"

    assert TestProc2.workdir.joinpath("0", "output", "out2.txt").read_text() == "test1"

    blob = bucket.get_blob("out3/out3.txt")
    assert blob.download_as_bytes() == b"test1"

    caplog.clear()
    p.set_start(TestProc).run()
    assert "[cyan]TestProc:[/cyan] Cached jobs: [0]" in caplog.text
    assert "[cyan]TestProc2:[/cyan] Cached jobs: [0]" in caplog.text
    assert "[cyan]TestProc3:[/cyan] Cached jobs: [0]" in caplog.text


@pytest.mark.forked
def test_pipeline_in_nonexist_gsfile(tmp_path):
    infile = f"gs://{BUCKET}/not/exist"

    class TestProc(Proc):
        input = "infile:file"
        input_data = [infile]
        output = "outfile:file:out.txt"
        script = "cp {{in.infile}} {{out.outfile}}"
        plugin_opts = {"gs_localize": tmp_path / "gslocal"}

    with pytest.raises(InvalidGoogleStorageURIError, match="Input path not exists"):
        Pipen(
            workdir=tmp_path / "pipen",
            outdir=tmp_path / "outdir",
        ).set_start(TestProc).run()


@pytest.mark.forked
def test_pipeline_in_bucket_gsfile(tmp_path):
    infile = f"gs://{BUCKET}"

    class TestProc(Proc):
        input = "infile:file"
        input_data = [infile]
        output = "outfile:file:out.txt"
        script = "cp {{in.infile}} {{out.outfile}}"
        plugin_opts = {"gs_localize": tmp_path / "gslocal"}

    with pytest.raises(
        InvalidGoogleStorageURIError,
        match="Input path expected instead of a bare bucket",
    ):
        Pipen(
            workdir=tmp_path / "pipen",
            outdir=tmp_path / "outdir",
        ).set_start(TestProc).run()


@pytest.mark.forked
def test_pipeline_out_bucket_gsfile(tmp_path):
    outfile = f"gs://{BUCKET}"

    class TestProc(Proc):
        input = "in"
        input_data = [1]
        output = f"outfile:file:{outfile}"
        script = "cp {{in.infile}} {{out.outfile}}"

    with pytest.raises(
        InvalidGoogleStorageURIError,
        match="Output path expected instead of a bare bucket",
    ):
        Pipen(
            workdir=tmp_path / "pipen",
            outdir=tmp_path / "outdir",
        ).set_start(TestProc).run()


@pytest.mark.forked
def test_pipeline_non_localize(bucket, tmp_path, caplog):
    infile = f"gs://{BUCKET}/test.txt"
    outfile = f"gs://{BUCKET}/out2/out.txt"

    class TestProc(Proc):
        input = "infile:file"
        input_data = [infile]
        output = f"outfile:file:{outfile}"
        lang = sys.executable
        envs = {"credfile": os.environ["GOOGLE_APPLICATION_CREDENTIALS"]}
        script = """
            import os
            from google.cloud import storage
            from pipen_gcs.utils import parse_gcs_uri
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "{{envs.credfile}}"

            client = storage.Client()
            in_bucket, in_path = parse_gcs_uri("{{in.infile}}")
            out_bucket, out_path = parse_gcs_uri("{{out.outfile}}")
            in_bucket = client.get_bucket(in_bucket)
            out_bucket = client.get_bucket(out_bucket)
            in_blob = in_bucket.blob(in_path)
            content = in_blob.download_as_bytes().decode()
            out_blob = out_bucket.blob(out_path)
            out_blob.upload_from_string(content)
        """

    class TestProc2(Proc):
        """Mixed gs input and local output"""

        requires = TestProc
        input = "infile:file"
        output = "outfile:file:out2.txt"
        lang = sys.executable
        envs = {"credfile": os.environ["GOOGLE_APPLICATION_CREDENTIALS"]}
        script = """
            import os
            from google.cloud import storage
            from pipen_gcs.utils import parse_gcs_uri
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "{{envs.credfile}}"

            client = storage.Client()
            bucket, path = parse_gcs_uri("{{in.infile}}")
            bucket = client.get_bucket(bucket)
            blob = bucket.blob(path)
            blob.download_to_filename("{{out.outfile}}")
        """

    class TestProc3(Proc):
        """Mixed local input and gs output"""

        requires = TestProc2
        input = "infile:file"
        output = f"outdir:dir:gs://{BUCKET}/out4"
        lang = sys.executable
        envs = {"credfile": os.environ["GOOGLE_APPLICATION_CREDENTIALS"]}
        script = """
            import os
            from google.cloud import storage
            from pipen_gcs.utils import parse_gcs_uri
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "{{envs.credfile}}"

            client = storage.Client()
            bucket, path = parse_gcs_uri("{{out.outdir}}/out4.txt")
            bucket = client.get_bucket(bucket)
            blob = bucket.blob(path)
            blob.upload_from_filename("{{in.infile}}")
        """

    p = Pipen(
        name="GcsNonLocalize",
        workdir=tmp_path / "pipen",
        outdir=tmp_path / "outdir",
        plugin_opts={
            "gcs_credentials": os.environ["GOOGLE_APPLICATION_CREDENTIALS"],
            "gs_localize": False,
        },
    ).set_start(TestProc)
    p.run()
    assert gs_file_exists(bucket.client, outfile)

    blob = bucket.get_blob("out2/out.txt")
    assert blob.download_as_bytes() == b"test1"

    assert TestProc2.workdir.joinpath("0", "output", "out2.txt").read_text() == "test1"

    blob = bucket.get_blob("out4/out4.txt")
    assert blob.download_as_bytes() == b"test1"

    caplog.clear()
    p.set_start(TestProc).run()
    assert "[cyan]TestProc:[/cyan] Cached jobs: [0]" in caplog.text
    assert "[cyan]TestProc2:[/cyan] Cached jobs: [0]" in caplog.text
    assert "[cyan]TestProc3:[/cyan] Cached jobs: [0]" in caplog.text


@pytest.mark.forked
def test_pipeline_mixed_input_to_local(tmp_path):
    infile1 = f"gs://{BUCKET}/test.txt"
    infile2 = tmp_path / "test2.txt"
    infile2.write_text("test2")
    infile2 = str(infile2)

    class TestProc(Proc):
        input = "infile:file"
        input_data = [infile1, infile2]
        output = "outfile:file:out.txt"
        lang = sys.executable
        script = """
            import shutil
            shutil.copy("{{in.infile}}", "{{out.outfile}}")
        """

    p = Pipen(
        name="GcsMixedInput",
        workdir=tmp_path / "pipen",
        outdir=tmp_path / "outdir",
        plugin_opts={
            "gcs_credentials": os.environ["GOOGLE_APPLICATION_CREDENTIALS"],
            "gs_localize": tmp_path / "gslocal",
        },
    ).set_start(TestProc)
    p.run()
    assert (
        TestProc.workdir.joinpath("0", "output", "out.txt").read_text() == "test1"
    )
    assert (
        TestProc.workdir.joinpath("1", "output", "out.txt").read_text() == "test2"
    )


@pytest.mark.forked
def test_pipeline_mixed_input_to_cloud(bucket, tmp_path):
    infile1 = f"gs://{BUCKET}/test.txt"
    infile2 = tmp_path / "test2.txt"
    infile2.write_text("test2")
    infile2 = str(infile2)
    outfile1 = f"gs://{BUCKET}/out5/out0.txt"
    outfile2 = f"gs://{BUCKET}/out5/out1.txt"

    class TestProc(Proc):
        input = "infile:file"
        input_data = [infile1, infile2]
        output = f"outfile:file:gs://{BUCKET}/out5/out{{{{job.index}}}}.txt"
        lang = sys.executable
        envs = {"credfile": os.environ["GOOGLE_APPLICATION_CREDENTIALS"]}
        script = """
            import os
            from google.cloud import storage
            from pipen_gcs.utils import parse_gcs_uri
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "{{envs.credfile}}"
            client = storage.Client()
            bucket, path = parse_gcs_uri("{{out.outfile}}")
            bucket = client.get_bucket(bucket)
            blob = bucket.blob(path)

            if "{{in.infile}}".startswith("gs://"):
                inbucket, inpath = parse_gcs_uri("{{in.infile}}")
                inbucket = client.get_bucket(inbucket)
                inblob = inbucket.blob(inpath)
                content = inblob.download_as_string()
                blob.upload_from_string(content)
            else:
                blob.upload_from_filename("{{in.infile}}")
        """

    p = Pipen(
        name="GcsMixedInputCloud",
        workdir=tmp_path / "pipen",
        outdir=tmp_path / "outdir",
        plugin_opts={
            "gcs_credentials": os.environ["GOOGLE_APPLICATION_CREDENTIALS"],
            "gs_localize": False,
        },
    ).set_start(TestProc)
    p.run()
    assert gs_file_exists(bucket.client, outfile1)
    assert gs_file_exists(bucket.client, outfile2)

    blob = bucket.get_blob("out5/out0.txt")
    assert blob.download_as_bytes() == b"test1"

    blob = bucket.get_blob("out5/out1.txt")
    assert blob.download_as_bytes() == b"test2"
