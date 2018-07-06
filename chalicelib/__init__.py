import json
import logging
import os
import random
import tempfile
import uuid

import boto3
from botocore.exceptions import ClientError

from chalicelib.constants import BUNDLE_UUIDS_PATH

formatter = logging.Formatter("%(levelname)s:%(name)s:%(message)s")

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

logger = logging.getLogger("hca-matrix-service")
logger.addHandler(stream_handler)


def rand_uuid():
    return str(uuid.uuid4())


def rand_uuids():
    """
    Generate a random list of uuids.
    :return: A list of random uuids.
    """
    uuids = []
    n = random.randint(1, 11)

    for _ in range(n):
        uuids.append(rand_uuid())

    return uuids


def mk_rand_dirs():
    """
    Generate a random list of temp directories containing some random files.
    :return: A list of temp directories.
    """
    temp_dirs = []
    n = random.randint(1, 5)
    suffices = ('.json', '.loom', '.cvs', '',)

    # Generate n directories
    for _ in range(n):
        temp_dir = tempfile.mkdtemp()
        temp_dirs.append(temp_dir)
        k = random.randint(1, 11)

        # Generate k random files within each directory
        for _ in range(k):
            tempfile.mkstemp(suffix=random.choice(suffices), dir=temp_dir)

    return temp_dirs


def scan_dirs(dirs, file_format):
    """
    Scan a list of directories to get the number of files that satisfies
    a specific file format within them.

    :param dirs: A list of directories paths.
    :param file_format: The file format specified.
    :return: The number of file satisfying the file formats.
    """
    result = 0

    for dir_path in dirs:
        for path in os.listdir(dir_path):
            if path.endswith(file_format):
                result += 1

    return result


def get_random_existing_bundle_uuids():
    """
    Get a random subset of existing bundle uuids stored in bundle_uuids.json.
    """
    # Get a random subset of bundle_uuids from sample bundle uuids
    with open(BUNDLE_UUIDS_PATH, "r") as f:
        sample_bundle_uuids = json.loads(f.read())

    n = random.randint(1, 5)
    bundle_uuids_subset = random.sample(sample_bundle_uuids, n)

    return bundle_uuids_subset


def check_s3key_existence(key, bucket_name):
    """
    Check the existence of a key in s3 bucket.
    :param key: Key to check for existence.
    :param bucket_name: S3 bucket name.
    :return: True if the key exists in the bucket; Otherwise, return false.
    """
    s3 = boto3.resource("s3")

    try:
        s3.Object(bucket_name=bucket_name, key=key).get()
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == "NoSuchKey":
            return False
