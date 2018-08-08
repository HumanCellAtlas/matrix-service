import os
import shutil
import uuid

from glob import glob
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
    mtx_paths = glob(f'{dir}/**/*{mtx_suffix}', recursive=True)
    return mtx_paths


def get_size(dir_path: str) -> int:
    """
    Get the size of a directory.
    :param dir_path: Path to the directory.
    :return: Directory size in bytes.
    """
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(dir_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size


def clean_dir(dir_path: str) -> None:
    """
    Clean all sub-directories within a directory.
    :param dir_path: Path to a directory.
    """
    paths = glob(f'{dir_path}/**')

    for path in paths:
        if os.path.isdir(path):
            shutil.rmtree(path)
