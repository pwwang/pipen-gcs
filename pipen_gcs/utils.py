from __future__ import annotations

import os
from pathlib import Path
from typing import Any, TYPE_CHECKING

from pipen.utils import mark, get_marked
from google.cloud import storage

if TYPE_CHECKING:
    from pipen import Job


class InvalidGoogleStorageURIError(Exception):
    """Invalid Google Cloud Storage URI"""


def _mtime(blob: storage.Blob) -> float:
    """Get the modification time of a blob

    Args:
        blob (storage.Blob): The blob to get the modification time

    Returns:
        float: The modification time
    """
    if blob.metadata and "mtime" in blob.metadata:
        return float(blob.metadata["mtime"])
    return blob.updated.timestamp() if blob.updated else 0.0


def parse_gcs_uri(uri: str) -> tuple[str, str]:
    """Parse a Google Cloud Storage URI

    Example:
        >>> parse_gcs_uri("gs://bucket/path/to/file")
        ("bucket", "path/to/file")
        >>> parse_gcs_uri("gs://bucket/path/to/dir/")
        ("bucket", "path/to/dir/")
        >>> parse_gcs_uri("gs://bucket/")
        ("bucket", "")

    Args:
        uri (str): The URI to parse

    Returns:
        tuple[str, str]: The bucket and the path
    """
    if uri.startswith("gs://"):
        uri = uri[5:]
    bucket, *path = uri.split("/")
    return bucket, "/".join(path)


def get_gs_type(client: storage.Client, gs_uri: str) -> str:
    """Get the type of a file/dir in Google Cloud Storage

    Example:
        >>> get_gs_type(client, "gs://bucket/")
        "bucket"
        >>> get_gs_type(client, "gs://bucket/path/to/dir/")
        "dir"
        >>> get_gs_type(client, "gs://bucket/path/to/file")
        "file"
        >>> get_gs_type(client, "gs://bucket/not/exist")
        "none"

    Args:
        client (storage.Client): The Google Cloud Storage client
        gs_uri (str): The URI of the file in Google Cloud Storage

    Returns:
        str: The type, either bucket, dir, file or none (not exists)
    """
    bucket, path = parse_gcs_uri(gs_uri)
    if not path:
        return "bucket"

    bucket = client.get_bucket(bucket)
    blob = bucket.get_blob(path)
    if blob:
        return "dir" if blob.name.endswith("/") else "file"

    path = path.rstrip("/") + "/"
    blobs = list(bucket.list_blobs(prefix=path, max_results=1))
    if blobs:
        return "dir"

    return "none"


def update_plugin_data(job: Job, key: str, value: Any) -> None:
    """Update the plugin data for a job

    Args:
        job (Job): The job to update
        key (str): The key to update
        value (Any): The value to update
    """
    plugin_data = get_marked(job.proc.__class__, "plugin_data", {})
    plugin_data.setdefault("gcs", {})
    plugin_data["gcs"][key] = value
    mark(plugin_data=plugin_data)(job.proc.__class__)


def get_plugin_data(job: Job, key: str, default: Any = None) -> Any:
    """Get the plugin data for a job

    Args:
        job (Job): The job to get the data from
        key (str): The key to get
        default (Any, optional): The default value to return. Defaults to None.

    Returns:
        Any: The value of the key
    """
    plugin_data = get_marked(job.proc.__class__, "plugin_data", {})
    return plugin_data.get("gcs", {}).get(key, default)


def download_gs_file(
    client: storage.Client,
    gs_uri: str,
    localpath: str | Path,
) -> None:
    """Download a file from Google Cloud Storage

    Args:
        client (storage.Client): The Google Cloud Storage client
        gs_uri (str): The URI of the file in Google Cloud Storage
        localpath (str | Path): The local path to download
    """
    bucket, path = parse_gcs_uri(gs_uri)
    blob = client.get_bucket(bucket).get_blob(path)
    Path(localpath).parent.mkdir(parents=True, exist_ok=True)
    blob.download_to_filename(str(localpath))
    mtime = _mtime(blob)
    os.utime(localpath, (mtime, mtime))


def download_gs_dir(
    client: storage.Client,
    gs_uri: str,
    localpath: str | Path,
) -> None:
    """Download a file from Google Cloud Storage

    Args:
        client (storage.Client): The Google Cloud Storage client
        gs_uri (str): The URI of the file in Google Cloud Storage
        localpath (str | Path): The local path to download
    """
    bucket, path = parse_gcs_uri(gs_uri)
    path = path.rstrip("/") + "/"
    bucket = client.get_bucket(bucket)
    blobs = bucket.list_blobs(prefix=path)

    for blob in reversed(list(blobs)):
        localfile = Path(localpath).joinpath(blob.name[len(path):])
        if blob.name.endswith("/"):
            localfile.mkdir(parents=True, exist_ok=True)
        else:
            localfile.parent.mkdir(parents=True, exist_ok=True)
            blob.download_to_filename(localfile)

        mtime = _mtime(blob)
        os.utime(localfile, (mtime, mtime))


def get_gs_mtime(client: storage.Client, gs_uri: str, dir_depth: int) -> float:
    """Get the modification time of a file/dir in Google Cloud Storage

    Args:
        client (storage.Client): The Google Cloud Storage client
        gs_uri (str): The URI of the file in Google Cloud Storage
        dir_depth (int): The depth of the directory to check

    Returns:
        float: The modification time
    """
    gstype = get_gs_type(client, gs_uri)  # file or dir, check when pipeline starts
    bucket, path = parse_gcs_uri(gs_uri)
    bucket = client.get_bucket(bucket)
    blob = bucket.get_blob(path)
    if gstype == "file" or dir_depth == 0:
        return _mtime(blob)

    path = path.rstrip("/") + "/"
    blobs = bucket.list_blobs(prefix=path)
    return max(
        (
            _mtime(blob)
            for blob in blobs
            if blob.name.rstrip("/")[len(path):].count("/") < dir_depth
        ),
        default=0.0,
    )


def clear_gs_file(client: storage.Client, gs_uri: str) -> bool:
    """Clear a file from Google Cloud Storage

    Args:
        client (storage.Client): The Google Cloud Storage client
        gs_uri (str): The URI of the file in Google Cloud Storage
    """
    bucket, path = parse_gcs_uri(gs_uri)
    blob = client.get_bucket(bucket).get_blob(path)
    if blob:
        blob.delete()
    return True


def clear_gs_dir(client: storage.Client, gs_uri: str) -> bool:
    """Clear a directory from Google Cloud Storage

    Args:
        client (storage.Client): The Google Cloud Storage client
        gs_uri (str): The URI of the directory in Google Cloud Storage
    """
    bucket, path = parse_gcs_uri(gs_uri)
    path = path.rstrip("/") + "/"
    bucket = client.get_bucket(bucket)
    blobs = bucket.list_blobs(prefix=path)
    # reversed to delete files first
    for blob in reversed(list(blobs)):
        blob.delete()
    return True


def gs_dir_exists(client: storage.Client, gs_uri: str) -> bool:
    """Check if a directory exists in Google Cloud Storage

    Args:
        client (storage.Client): The Google Cloud Storage client
        gs_uri (str): The URI of the directory in Google Cloud Storage

    Returns:
        bool: True if the directory exists
    """
    gstype = get_gs_type(client, gs_uri)
    return gstype == "dir"


def gs_file_exists(client: storage.Client, gs_uri: str) -> bool:
    """Check if a file exists in Google Cloud Storage

    Args:
        client (storage.Client): The Google Cloud Storage client
        gs_uri (str): The URI of the file in Google Cloud Storage

    Returns:
        bool: True if the file exists
    """
    gstype = get_gs_type(client, gs_uri)
    return gstype == "file"


def upload_gs_file(
    client: storage.Client,
    localpath: str | Path,
    gs_uri: str,
) -> None:
    """Upload a file to Google Cloud Storage

    Args:
        client (storage.Client): The Google Cloud Storage client
        localpath (str | Path): The local path to upload
        gs_uri (str): The URI of the file in Google Cloud Storage
    """
    bucket, path = parse_gcs_uri(gs_uri)
    blob = client.get_bucket(bucket).blob(path)
    blob.metadata = {"mtime": Path(localpath).stat().st_mtime}
    blob.upload_from_filename(str(localpath))


def upload_gs_dir(
    client: storage.Client,
    localpath: str | Path,
    gs_uri: str,
) -> None:
    """Upload a directory to Google Cloud Storage

    Args:
        client (storage.Client): The Google Cloud Storage client
        localpath (str | Path): The local path to upload
        gs_uri (str): The URI of the directory in Google Cloud Storage
    """
    bucket, path = parse_gcs_uri(gs_uri)
    path = path.rstrip("/") + "/"
    bucket = client.get_bucket(bucket)
    for localfile in Path(localpath).rglob("*"):
        if localfile.is_dir():
            continue
        blob = bucket.blob(path + str(localfile.relative_to(localpath)))
        blob.metadata = {"mtime": localfile.stat().st_mtime}
        blob.upload_from_filename(str(localfile))


def create_gs_dir(client: storage.Client, gs_uri: str) -> None:
    """Create a directory in Google Cloud Storage

    Args:
        client (storage.Client): The Google Cloud Storage client
        gs_uri (str): The URI of the directory in Google Cloud Storage
    """
    gstype = get_gs_type(client, gs_uri)
    if gstype == "dir":
        return
    bucket, path = parse_gcs_uri(gs_uri)
    path = path.rstrip("/") + "/"
    blob = client.get_bucket(bucket).blob(path)
    blob.upload_from_string("", content_type="application/x-directory")
