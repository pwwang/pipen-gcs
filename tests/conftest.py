import os
import json
import pytest
from datetime import datetime
from google.cloud import storage
from dotenv import load_dotenv

BUCKET = "handy-buffer-287000.appspot.com"


@pytest.fixture(scope="session")
def bucket(tmpdir_factory):
    load_dotenv()
    credfile = tmpdir_factory.mktemp("gs").join("credentials.json")
    creds = {
        "type": os.environ["GAC_TYPE"],
        "project_id": os.environ["GAC_PROJECT_ID"],
        "private_key_id": os.environ["GAC_PRIVATE_KEY_ID"],
        "private_key": os.environ["GAC_PRIVATE_KEY"],
        "client_email": os.environ["GAC_CLIENT_EMAIL"],
        "client_id": os.environ["GAC_CLIENT_ID"],
        "auth_uri": os.environ["GAC_AUTH_URI"],
        "token_uri": os.environ["GAC_TOKEN_URI"],
        "auth_provider_x509_cert_url": os.environ["GAC_AUTH_PROVIDER_X509_CERT_URL"],
        "client_x509_cert_url": os.environ["GAC_CLIENT_X509_CERT_URL"],
        "universe_domain": "googleapis.com",
    }
    with open(credfile, "w") as fout:
        json.dump(creds, fout)

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(credfile)
    return storage.Client().get_bucket(BUCKET)


@pytest.fixture(scope="session", autouse=True)
def init_gs_files(bucket):
    blob = bucket.blob("test.txt")
    blob.metadata = {"mtime": datetime(2021, 1, 1).timestamp()}  # 1609488000.0
    blob.upload_from_string("test1")
    blob = bucket.blob("testdir/test1.txt")
    blob.metadata = {"mtime": datetime(2021, 1, 2).timestamp()}  # 1609574400.0
    blob.upload_from_string("test2")
    blob = bucket.blob("testdir/test2.txt")
    blob.metadata = {"mtime": datetime(2021, 1, 3).timestamp()}  # 1609660800.0
    blob.upload_from_string("test3")
    blob = bucket.blob("testdir2/test9.txt")
    blob.metadata = {"mtime": datetime(2021, 1, 4).timestamp()}  # 1609747200.0
    blob.upload_from_string("test9")
    blob = bucket.blob("testdir2/test2/")
    blob.metadata = {"mtime": datetime(2021, 1, 5).timestamp()}  # 1609833600.0
    blob.upload_from_string("", content_type="application/x-directory")
    blob = bucket.blob("testdir2/test2/test1.txt")
    blob.metadata = {"mtime": datetime(2021, 1, 6).timestamp()}  # 1609920000.0
    blob.upload_from_string("test4")
    blob = bucket.blob("testdir2/test2/test2.txt")
    blob.metadata = {"mtime": datetime(2021, 1, 7).timestamp()}  # 1610006400.0
    blob.upload_from_string("test5")
