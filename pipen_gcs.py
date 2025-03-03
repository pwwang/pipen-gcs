from __future__ import annotations

from os import PathLike
from hashlib import sha256
from pathlib import Path
from tempfile import gettempdir
from typing import TYPE_CHECKING, Tuple

from yunpath import GSClient, AnyPath, CloudPath
from xqute.path import DualPath, MountedPath
from pipen import plugin
from pipen.defaults import ProcInputType, ProcOutputType
from pipen.utils import get_logger

if TYPE_CHECKING:
    from pipen import Pipen, Proc, Job

__version__ = "0.1.0a3"
logger = get_logger("gcs")


class NotALocalPathError(Exception):
    """Raised when the path is not a local path"""


class WrongPathTypeError(Exception):
    """Raised when pipen-gcs cannot handle the path type"""


def _process_infile(
    file: str | PathLike | CloudPath | MountedPath | DualPath,
    client: GSClient,
) -> Tuple[MountedPath | DualPath, str | None, str]:
    """Sync the file to local"""
    if isinstance(file, (MountedPath, DualPath)):
        return (
            file,
            "warning",
            f"Skip syncing <{type(file).__name__}>: {file}, "
            "it already has a paired path.",
        )

    if not isinstance(file, (str, CloudPath)):  # impossible to be a cloud path
        return file, None, None

    if isinstance(file, CloudPath):
        # Switch client
        file = client.CloudPath(file)
        file._refresh_cache()
        return (
            DualPath(file, mounted=file.fspath),
            "debug",
            f"Synced: {file}",
        )

    # str, let's see if it's a cloud path
    path = AnyPath(file)
    if isinstance(path, CloudPath) and ":" not in path._no_prefix:
        file = client.CloudPath(file)
        file._refresh_cache()
        return (
            DualPath(file, mounted=file.fspath),
            "debug",
            f"Synced: {file}",
        )

    return file, None, None


class PipenGcsPlugin:
    """A plugin for pipen to handle file metadata in Google Cloud Storage

    Configurations:
        gcs_cache: The directory to save the cloud storage files.
    """

    version = __version__
    name = "gcs"
    client: GSClient = None
    # Make sure this plugin runs before the main plugin where we check if the
    # output file has been generated.
    priority = -1001

    @plugin.impl
    async def on_init(self, pipen: Pipen) -> None:
        """Initialize the plugin"""
        # Whether to download files to local and upload the output files back
        # If None, tempdir will be used
        pipen.config.plugin_opts.setdefault("gcs_cache", None)
        # loglevel
        pipen.config.plugin_opts.setdefault("gcs_loglevel", "info")

    @plugin.impl
    async def on_start(self, pipen: Pipen):
        """Initialize the proc"""
        if isinstance(pipen.workdir, CloudPath):
            raise NotALocalPathError(
                f"Pipeline workdir is not a local path: {pipen.workdir}. "
                "Either using a local workdir or disable the pipen-gcs plugin."
            )

        gcs_loglevel = pipen.config.plugin_opts.get("gcs_loglevel", "info")
        logger.setLevel(gcs_loglevel.upper())

        gcs_cache = pipen.config.plugin_opts.get("gcs_cache", None)
        if gcs_cache is None:
            dig = sha256(f"{pipen.workdir}...{pipen.outdir}".encode()).hexdigest()[:8]
            gcs_cache = Path(gettempdir()) / f"pipen-gcs-{dig}"
            gcs_cache.mkdir(exist_ok=True)

        self.client = GSClient(local_cache_dir=gcs_cache)

        if not isinstance(pipen.outdir, CloudPath):
            return

        outdir = self.client.CloudPath(pipen.outdir)
        pipen.outdir = DualPath(outdir, mounted=outdir.fspath)

    @plugin.impl
    def on_proc_input_computed(self, proc: Proc):
        """Handle the input files"""
        if proc.name not in [p.name for p in proc.pipeline.starts]:
            return

        max_log = 5  # per inkey

        for inkey, intype in proc.input.type.items():
            if intype == ProcInputType.VAR:
                continue

            proc.log("info", f"Syncing input {inkey}:{intype} ...", logger=logger)
            log_i = 0
            if intype in (ProcInputType.FILE, ProcInputType.DIR):
                values = []

                for f in proc.input.data[inkey]:
                    value, loglevel, logmsg = _process_infile(f, self.client)
                    values.append(value)

                    if loglevel and log_i < max_log:
                        proc.log(loglevel, logmsg, logger=logger)
                        log_i += 1

                    elif loglevel and log_i == max_log:
                        proc.log(
                            loglevel,
                            "Not showing more similar messages ...",
                            logger=logger,
                        )

                proc.input.data[inkey] = values

            else:  # ProcInputType.FILES / ProcInputType.DIRS
                values = []
                for fs in proc.input.data[inkey]:
                    value = []
                    values.append(value)

                    for f in fs:
                        val, loglevel, logmsg = _process_infile(f, self.client)
                        value.append(val)

                        if loglevel and log_i < max_log:
                            proc.log(loglevel, logmsg, logger=logger)
                            log_i += 1

                        elif loglevel and log_i == max_log:
                            proc.log(
                                loglevel,
                                "Not showing more similar messages ...",
                                logger=logger,
                            )

                proc.input.data[inkey] = values

    @plugin.impl
    async def on_job_cached(self, job: Job):
        """Sync the output files, in case local files are removed"""
        if (
            not job.proc.export
            or not isinstance(job.proc.pipeline.outdir, DualPath)
            or not isinstance(job.proc.pipeline.outdir.path, CloudPath)
        ):
            return

        for outkey, outtype in job._output_types.items():
            if outtype == ProcOutputType.VAR:
                continue

            spec_out = getattr(job.output[outkey], "spec", None)
            if not isinstance(spec_out, CloudPath):
                continue

            job.log("info", f"Syncing: {job.output[outkey]}", logger=logger)
            spec_out = self.client.CloudPath(spec_out)
            spec_out._refresh_cache()

    @plugin.impl
    async def on_job_succeeded(self, job: Job):
        """Upload the output files"""
        if (
            not job.proc.export
            or not isinstance(job.proc.pipeline.outdir, DualPath)
            or not isinstance(job.proc.pipeline.outdir.path, CloudPath)
        ):
            return

        for outkey, outtype in job._output_types.items():
            if outtype == ProcOutputType.VAR:
                continue

            spec_out = getattr(job.output[outkey], "spec", None)
            if not isinstance(spec_out, CloudPath):
                continue

            job.log("info", f"Uploading: {job.output[outkey]}", logger=logger)
            spec_out = self.client.CloudPath(spec_out)
            spec_out._upload_local_to_cloud(force_overwrite_to_cloud=True)


plugin.register(PipenGcsPlugin())
