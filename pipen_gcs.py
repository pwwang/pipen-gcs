from __future__ import annotations

from os import PathLike
from hashlib import sha256
from tempfile import gettempdir
from typing import TYPE_CHECKING, Tuple

from panpath import PanPath, CloudPath
from xqute.path import SpecPath, MountedPath
from pipen import plugin
from pipen.defaults import ProcInputType, ProcOutputType
from pipen.utils import get_logger

if TYPE_CHECKING:
    from pipen import Pipen, Proc, Job

__version__ = "1.1.1"
logger = get_logger("gcs")


class NotALocalPathError(Exception):
    """Raised when the path is not a local path"""


class WrongPathTypeError(Exception):
    """Raised when pipen-gcs cannot handle the path type"""


async def _process_infile(
    file: str | PathLike,
    cachedir: PanPath,
) -> Tuple[MountedPath | SpecPath, str | None, str]:
    """Sync the file to local"""
    if isinstance(file, (MountedPath, SpecPath)):
        return (
            file,
            "warning",
            f"Skip syncing <{type(file).__name__}>: {file}, "
            "it already has a paired path.",
        )

    file = PanPath(file)
    if not isinstance(file, CloudPath):  # impossible to be a cloud path
        return file, None, None

    fspath = cachedir.joinpath(
        file.parts[0].replace(":", ""),
        *file.parts[1:],
    )
    await fspath.parent.a_mkdir(parents=True, exist_ok=True)
    if await file.a_is_dir():
        await file.a_copytree(fspath)
    else:
        await file.a_copy(fspath)

    return (
        SpecPath(file, mounted=fspath),
        "debug",
        f"Synced: {file} \u2192 {fspath}",
    )


class PipenGcsPlugin:
    """A plugin for pipen to handle file metadata in Google Cloud Storage

    Configurations:
        gcs_cache: The directory to save the cloud storage files.
    """

    version = __version__
    name = "gcs"
    gcs_cache: str | PanPath = None
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
        # max number of files to show in the log
        pipen.config.plugin_opts.setdefault("gcs_logmax", 5)

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
            gcs_cache = PanPath(gettempdir()) / f"pipen-gcs-{dig}"
        else:
            gcs_cache = PanPath(gcs_cache)

        await gcs_cache.a_mkdir(exist_ok=True)

        self.gcs_cache = gcs_cache

        if not isinstance(pipen.outdir, CloudPath):
            return

        mounted_outdir = gcs_cache.joinpath(
            pipen.outdir.parts[0].replace(":", ""),
            *pipen.outdir.parts[1:],
        )
        await mounted_outdir.a_mkdir(parents=True, exist_ok=True)
        # Use SpecPath to pair the cloud path with the mounted local path
        pipen.outdir = SpecPath(pipen.outdir, mounted=mounted_outdir)

    @plugin.impl
    async def on_proc_input_computed(self, proc: Proc):
        """Handle the input files"""
        if proc.name not in [p.name for p in proc.pipeline.starts]:
            return

        max_log = proc.pipeline.config.plugin_opts.get("gcs_logmax", 5)

        for inkey, intype in proc.input.type.items():
            if intype == ProcInputType.VAR:
                continue

            proc.log("info", f"Syncing input {inkey}:{intype} ...", logger=logger)
            log_i = 0
            if intype in (ProcInputType.FILE, ProcInputType.DIR):
                values = []

                for f in proc.input.data[inkey]:
                    value, loglevel, logmsg = await _process_infile(f, self.gcs_cache)
                    values.append(value)

                    if loglevel and log_i < max_log:
                        proc.log(loglevel, logmsg, logger=logger)
                        log_i += 1

                    elif loglevel and log_i == max_log:
                        proc.log(
                            loglevel,
                            "Skipping more similar messages "
                            "(increase gcs_logmax to show more) ...",
                            logger=logger,
                        )

                proc.input.data[inkey] = values

            else:  # ProcInputType.FILES / ProcInputType.DIRS
                values = []
                for fs in proc.input.data[inkey]:
                    value = []
                    values.append(value)

                    for f in fs:
                        val, loglevel, logmsg = await _process_infile(f, self.gcs_cache)
                        value.append(val)

                        if loglevel and log_i < max_log:
                            proc.log(loglevel, logmsg, logger=logger)
                            log_i += 1

                        elif loglevel and log_i == max_log:
                            proc.log(
                                loglevel,
                                "Skipping more similar messages "
                                "(increase gcs_logmax to show more) ...",
                                logger=logger,
                            )

                proc.input.data[inkey] = values

    @plugin.impl
    async def on_job_cached(self, job: Job):
        """Sync the output files, in case local files are removed"""
        if (
            not job.proc.export
            or not isinstance(job.proc.pipeline.outdir, SpecPath)
        ):
            return

        for outkey, outtype in job._output_types.items():
            if outtype == ProcOutputType.VAR:
                continue

            spec_out = getattr(job.output[outkey], "spec", None)
            if not isinstance(spec_out, CloudPath):
                continue

            job.log("info", f"Syncing: {job.output[outkey]}", logger=logger)

            fspath = self.gcs_cache.joinpath(
                spec_out.parts[0].replace(":", ""),
                *spec_out.parts[1:],
            )
            await fspath.parent.a_mkdir(parents=True, exist_ok=True)
            if await spec_out.a_is_dir():
                await spec_out.a_copytree(fspath)
            else:
                await spec_out.a_copy(fspath)

    @plugin.impl
    async def on_job_succeeded(self, job: Job):
        """Upload the output files"""
        if (
            not job.proc.export
            or not isinstance(job.proc.pipeline.outdir, SpecPath)
        ):
            return

        for outkey, outtype in job._output_types.items():
            if outtype == ProcOutputType.VAR:
                continue

            spec_out = getattr(job.output[outkey], "spec", None)
            if not isinstance(spec_out, CloudPath):
                continue

            local_out = PanPath(job.output[outkey])
            job.log("info", f"Uploading: {local_out}", logger=logger)

            if await local_out.a_is_dir():
                await local_out.a_copytree(spec_out)
            else:
                await local_out.a_copy(spec_out)


plugin.register(PipenGcsPlugin())
