from __future__ import annotations

import os

from typing import TYPE_CHECKING
from pathlib import Path

from pipen import plugin
from pipen.defaults import ProcOutputType
from pipen.utils import get_logger
from google.cloud import storage

from .utils import (
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
    InvalidGoogleStorageURIError,
)

if TYPE_CHECKING:
    from pipen import Pipen

__version__ = "0.0.1"
logger = get_logger("gcs")


class PipenGcsPlugin:
    """A plugin for pipen to handle file metadata in Google Cloud Storage

    Configurations:
        gs_localize: False or a directory to download files to and upload back
            the output files. If False, the plugin will not download or upload
            files. The job script should handle the files in the cloud.
        gs_credentials: The path to the Google Cloud Storage credentials file.
    """

    version = __version__
    name = "gcs"

    def __init__(self):
        self.gclient = None  # pragma: no cover

    @plugin.impl
    async def on_init(self, pipen: Pipen) -> None:
        """Initialize the plugin"""
        # Whether to download files to local and upload the output files back
        pipen.config.plugin_opts.setdefault("gcs_localize", False)
        # Google Cloud Storage credentials, otherwise you need to login manually
        pipen.config.plugin_opts.setdefault("gcs_credentials", None)

    @plugin.impl
    async def on_start(self, pipen: Pipen) -> None:
        """Login to Google Cloud Storage using the credentials if provided"""
        logger.setLevel("INFO")
        plugin_opts = pipen.config.plugin_opts or {}
        if plugin_opts.get("gcs_credentials"):
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = plugin_opts[
                "gcs_credentials"
            ]
        self.gclient = storage.Client()

    @plugin.impl
    def norm_inpath(self, job, inpath, is_dir):
        if not isinstance(inpath, str) or not inpath.startswith(
            "gs://"
        ):  # pragma: no cover
            # let next plugin handle it
            return None

        plugin_opts = job.proc.plugin_opts or {}
        gs_localize = plugin_opts.get("gs_localize", False)

        gstype = get_gs_type(self.gclient, inpath)
        if gstype == "none":
            raise InvalidGoogleStorageURIError(
                f"[{job.proc.name}] Input path not exists: {inpath}"
            )
        if gstype == "bucket":
            raise InvalidGoogleStorageURIError(
                f"[{job.proc.name}] Input path expected instead of a bare bucket: "
                f"{inpath}"
            )

        if not gs_localize:
            return inpath

        gs_localize = Path(gs_localize)
        # Download the file to local
        bucket, path = parse_gcs_uri(inpath)
        localpath = gs_localize.joinpath(bucket, path)
        job.log("info", f"Localizing {inpath} ...", logger=logger)
        if is_dir:
            download_gs_dir(self.gclient, inpath, localpath)
        else:
            download_gs_file(self.gclient, inpath, localpath)

        return localpath

    @plugin.impl
    def norm_outpath(self, job, outpath, is_dir):
        if not isinstance(outpath, str) or not outpath.startswith(
            "gs://"
        ):  # pragma: no cover
            # let next plugin handle it
            return None

        plugin_opts = job.proc.plugin_opts or {}
        gs_localize = plugin_opts.get("gs_localize", False)

        gstype = get_gs_type(self.gclient, outpath)
        if gstype == "bucket":
            raise InvalidGoogleStorageURIError(
                f"[{job.proc.name}] Output path expected instead of a bare bucket: "
                f"{outpath}"
            )

        if not gs_localize:
            return outpath

        gs_localize = Path(gs_localize)
        bucket, path = parse_gcs_uri(outpath)
        localpath = gs_localize.joinpath(bucket, path)
        if is_dir:
            outpath = outpath.rstrip("/") + "/"
            create_gs_dir(self.gclient, outpath)
            localpath.mkdir(parents=True, exist_ok=True)
        else:
            localpath.parent.mkdir(parents=True, exist_ok=True)

        # save the original output with gs://
        ret = str(localpath)
        update_plugin_data(job, ret, outpath)

        return ret

    @plugin.impl
    def get_mtime(self, job, path, dirsig):
        if not isinstance(path, str) or not path.startswith(
            "gs://"
        ):  # pragma: no cover
            return None

        return get_gs_mtime(self.gclient, path, dirsig)

    @plugin.impl
    async def clear_path(self, job, path, is_dir):
        if not isinstance(path, str) or not path.startswith(
            "gs://"
        ):  # pragma: no cover
            return None

        if is_dir:
            return clear_gs_dir(self.gclient, path)

        return clear_gs_file(self.gclient, path)

    @plugin.impl
    async def output_exists(self, job, path, is_dir):
        if not isinstance(path, str) or not path.startswith(
            "gs://"
        ):  # pragma: no cover
            return None

        if is_dir:
            return gs_dir_exists(self.gclient, path)

        return gs_file_exists(self.gclient, path)

    @plugin.impl
    async def on_job_succeeded(self, job):
        for key, value in job.output.items():
            gspath = get_plugin_data(job, value)
            if gspath is None:
                continue

            job.log("info", f"Uploading output '{key}' to {gspath} ...", logger=logger)
            if job._output_types[key] == ProcOutputType.FILE:
                upload_gs_file(self.gclient, value, gspath)
            else:
                upload_gs_dir(self.gclient, value, gspath)
