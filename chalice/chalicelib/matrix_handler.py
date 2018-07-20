import os
import shutil
import tempfile
import time
import traceback
import loompy

from hca.util import SwaggerAPIException
from typing import List, Tuple
from abc import ABC, abstractmethod
from chalicelib import get_mtx_paths
from chalicelib.config import MERGED_MTX_BUCKET_NAME, TEMP_DIR, hca_client, \
    s3_blob_store, logger
from chalicelib.request_handler import RequestHandler, RequestStatus


class MatrixHandler(ABC):
    """
    A generic matrix handler for matrices concatenation
    """
    def __init__(self, suffix) -> None:
        self._suffix = suffix

    def _download_mtx(self, bundle_uuids: List[str]) -> Tuple[str, List[str]]:
        """
        Filter for the matrix files within bundles, and download them locally

        :param bundle_uuids: A list of bundle uuids
        :return: A list of downloaded local matrix files paths and their directory
        """
        # Create a temp directory for storing s3 matrix files
        temp_dir = tempfile.mkdtemp(dir=TEMP_DIR)

        # Filter and download only matrices file that satisfies a specific suffix within bundles
        for bundle_uuid in bundle_uuids:
            dest_name = os.path.join(temp_dir, bundle_uuid)

            try:
                logger.info("Downloading matrices from bundle, {}, into {}."
                            .format(bundle_uuid, dest_name))

                hca_client.download(
                    bundle_uuid=bundle_uuid,
                    replica="aws",
                    dest_name=dest_name,
                    metadata_files=(),
                    data_files=("*{}".format(self._suffix),)
                )

            # Catch file not found exception
            except SwaggerAPIException as e:
                raise e

        # Get all downloaded mtx paths from temp_dir
        local_mtx_paths = get_mtx_paths(temp_dir, self._suffix)

        logger.info("Done downloading %d matrix files.", len(local_mtx_paths))

        return temp_dir, local_mtx_paths
