import json
import os
import random
import tempfile
import uuid
import boto3

from botocore.exceptions import ClientError
from chalicelib.constants import BUNDLE_UUIDS_PATH


def rand_uuid():
    return str(uuid.uuid4())


def rand_uuids(ub):
    """
    Generate a random list of uuids.
    :param ub: Upper bound for the number of uuids generated.
    :return: A list of random uuids.
    """
    uuids = []
    n = random.randint(1, ub)

    for _ in range(n):
        uuids.append(rand_uuid())

    return uuids


def mk_rand_dirs(ub_dir, ub_file):
    """
    Generate a random list of temp directories containing some random files.
    :param ub_dir: Upper bound for the number of directory generated.
    :param ub_file: Upper bound for the number of file within each directory.
    :return: A list of temp directories.
    """
    temp_dirs = []
    n = random.randint(1, ub_dir)
    suffices = ('.json', '.loom', '.cvs', '',)

    # Generate n directories
    for _ in range(n):
        temp_dir = tempfile.mkdtemp()
        temp_dirs.append(temp_dir)
        k = random.randint(1, ub_file)

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


def get_random_existing_bundle_uuids(ub):
    """
    Get a random subset of existing bundle uuids stored in bundle_uuids.json.
    :param ub: Upper bound for the size of uuid subset.
    """
    # Get a random subset of bundle_uuids from sample bundle uuids
    with open(BUNDLE_UUIDS_PATH, "r") as f:
        sample_bundle_uuids = json.loads(f.read())

    n = random.randint(1, ub)
    bundle_uuids_subset = random.sample(sample_bundle_uuids, n)

    return bundle_uuids_subset


def s3key_exists(key, bucket_name):
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


def delete_s3key(key, bucket_name):
    """
    Delete a key from s3 bucket.
    :param key: S3 bucket key.
    :param bucket_name: S3 bucket name.
    """
    s3 = boto3.resource("s3")

    if s3key_exists(key=key, bucket_name=bucket_name):
        s3.Object(bucket_name=bucket_name, key=key).delete()


def mk_rand_loom_file(ub):
    """
    Generate a random number of loom file within a temp directory.
    TODO: Generate valid loom file instead of empty one.

    :param ub: Upper bound for the number of loom file generated.
    :return: A list of loom file paths, and their directory.
    """
    temp_dir = tempfile.mkdtemp()
    temp_files = []

    n = random.randint(1, ub)

    for _ in range(n):
        _, temp_file = tempfile.mkstemp(suffix=".loom", dir=temp_dir)
        temp_files.append(temp_file)

    return temp_dir, temp_files
