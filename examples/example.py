import os
from pipen import Pipen, Proc
from dotenv import load_dotenv

load_dotenv()
BUCKET = os.getenv("BUCKET")


class Process1(Proc):
    input = "infile:file"
    output = "outfile:file:{{in.infile.stem}}.out"
    script = "cp {{in.infile}} {{out.outfile}}"


class Process2(Proc):
    requires = Process1
    input = "infile:file"
    output = "outfile:file:{{in.infile.stem}}.out"
    script = "cp {{in.infile}} {{out.outfile}}"


class Pipeline(Pipen):
    starts = Process1
    data = [
        [
            f"gs://{BUCKET}/pipen-test/channel/test1.txt",
            f"gs://{BUCKET}/pipen-test/channel/test2.txt",
            f"gs://{BUCKET}/pipen-test/channel/test3.txt",
        ]
    ]
    outdir = f"gs://{BUCKET}/pipen-gcs/outdir"
    loglevel = "debug"
    plugin_opts = {"gcs_loglevel": "debug"}


if __name__ == "__main__":
    Pipeline().run()
