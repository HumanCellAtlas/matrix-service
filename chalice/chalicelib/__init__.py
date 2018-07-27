import os
import uuid

from typing import List


def rand_uuid() -> str:
    return str(uuid.uuid4())


def get_mtx_paths(dir: str, mtx_suffix: str) -> List[str]:
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
