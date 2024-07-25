# pipen-gcs

A plugin for [pipen][1] to handle files in Google Cloud Storage

## Installation

```bash
pip install -U pipen-gcs

# uninstall to disable
pip uninstall pipen-gcs
```

## Usage

```python
from pipen import Proc, Pipen

class MyProc(Proc):
    input = "infile:file"
    input_data = ["gs://bucket/path/to/file"]
    output = "outfile:file:gs://bucket/path/to/output"
    script = "cat {{in.infile}} > {{out.outfile}}"

class MyPipen(Pipen):
    starts = MyProc
    # input files/directories will be downloaded to /tmp
    # output files/directories will be generated in /tmp and then uploaded
    #   to the cloud storage
    plugin_opts = {"gcs_localize": "/tmp"}

if __name__ == "__main__":
    MyPipen().run()
```

You can also disable localization, then you will have to handle the
cloud storage files yourself.

```python
from pipen import Proc, Pipen

class MyProc(Proc):
    input = "infile:file"
    input_data = ["gs://bucket/path/to/file"]
    output = "outfile:file:gs://bucket/path/to/output"
    script = "gsutil cp {{in.infile}} {{out.outfile}}"

class MyPipen(Pipen):
    starts = MyProc
    plugin_opts = {"gcs_localize": False}

if __name__ == "__main__":
    MyPipen().run()
```

## Configuration

- `gcs_localize`: The directory to localize the cloud storage files. If
  set to `False`, the files will not be localized. Default is `False`.
- `gcs_credentials`: The path to the Google Cloud Service Account
  credentials file.

[1]: https://github.com/pwwang/pipen
