import hashlib
import os
import uuid


def rand_uuid():
    return str(uuid.uuid4())


def get_mtx_paths(dir, mtx_suffix):
    """
    Get all matrices file paths within a directory.
    :param dir: Directory that contains matrix files.
    :param mtx_suffix: Suffix for the matrix files.
    :return: A list of mtx that ends with the specific suffix in the directory.
    """
    mtx_paths = []

    for dname, _, fnames in os.walk(dir):
        mtx_paths.extend([os.path.join(dname, fname) for fname in fnames if fname.endswith(mtx_suffix)])

    return mtx_paths


def generate_md5(s):
    """
    Generate MD5 sum of a sting.
    :param s: Input string.
    :return: MD5 sum of the input string.
    """
    return hashlib.md5(s.encode('utf-8')).hexdigest()



