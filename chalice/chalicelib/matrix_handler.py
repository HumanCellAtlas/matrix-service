import os
import tempfile
import time
import traceback
import loompy

from abc import ABC, abstractmethod
from subprocess import call
from typing import List
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

    def _download_mtx(self, bundle_uuids: List[str], temp_dir: str) -> List[str]:
        """
        Filter for the matrix files within bundles, and download them locally.
        :param bundle_uuids: A list of bundle uuids.
        :param temp_dir: A temporary directory for storing all downloaded matrix files.
        :return: A list of downloaded matrix files paths.
        """
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
            except Exception as e:
                raise e

        # Get all downloaded mtx paths from temp_dir
        local_mtx_paths = get_mtx_paths(temp_dir, self._suffix)

        logger.info("Done downloading %d matrix files.", len(local_mtx_paths))

        return local_mtx_paths

    @abstractmethod
    def _concat_mtx(self, mtx_paths: List[str], out_file: str) -> None:
        """
        Concatenate a list of matrices, and save into a new file on disk.
        :param mtx_paths: A list of downloaded local matrix files paths.
        :param out_file: Path to the concatenated matrix.
        """

    def _upload_mtx(self, path: str, request_id: str):
        """
        Upload a matrix file into an s3 bucket.
        :param path: Path of the merged matrix.
        :param request_id: Merge request id.
        """
        logger.info("%s", "Uploading \"{}\" to s3 bucket: \"{}\".".format(
            os.path.basename(path),
            MERGED_MTX_BUCKET_NAME
        ))

        with open(path, "rb") as merged_matrix:
            try:
                s3_blob_store.upload_file_handle(
                    bucket=MERGED_MTX_BUCKET_NAME,
                    key=request_id + self._suffix,
                    src_file_handle=merged_matrix
                )
            except Exception as e:
                raise e

        logger.info("Done uploading.")

    def run_merge_request(self, bundle_uuids: List[str], request_id: str, job_id: str) -> None:
        """
        Merge matrices within bundles, and upload the merged matrix to an s3 bucket.

        :param bundle_uuids: Bundles' uuid for locating bundles in DSS.
        :param request_id: Merge request id.
        :param job_id: Job id of the request.
        """
        # Print out the current usage of /tmp/ directory
        call(["df", "-H", TEMP_DIR])

        # Update the request status to RUNNING
        try:
            RequestHandler.update_request(
                bundle_uuids=bundle_uuids,
                request_id=request_id,
                job_id=job_id,
                status=RequestStatus.RUNNING
            )

            try:
                # Create a temp directory for storing all temp files
                with tempfile.TemporaryDirectory(dir=TEMP_DIR) as temp_dir:

                    start_time = time.time()
                    mtx_paths = self._download_mtx(bundle_uuids=bundle_uuids, temp_dir=temp_dir)
                    _, merged_mtx_path = tempfile.mkstemp(dir=temp_dir, prefix=request_id, suffix=self._suffix)
                    self._concat_mtx(mtx_paths=mtx_paths, out_file=merged_mtx_path)
                    self._upload_mtx(path=merged_mtx_path, request_id=request_id)
                    end_time = time.time()

                    # Update the request status to DONE
                    RequestHandler.update_request(
                        bundle_uuids=bundle_uuids,
                        request_id=request_id,
                        job_id=job_id,
                        status=RequestStatus.DONE,
                        time_spent_to_complete="{} seconds".format(end_time - start_time)
                    )
            except Exception as e:

                # Update the request status to ABORT
                RequestHandler.update_request(
                    bundle_uuids=bundle_uuids,
                    request_id=request_id,
                    job_id=job_id,
                    status=RequestStatus.ABORT,
                    reason_to_abort=traceback.format_exc()
                )

                raise e

        except Exception as e:
            raise e


class LoomMatrixHandler(MatrixHandler):
    """
    Matrix handler for .loom file format
    """

    def __init__(self) -> None:
        super().__init__(".loom")

    def _concat_mtx(self, mtx_paths: List[str], out_file: str) -> None:
        try:
            logger.info("Combining matrices to %s.", out_file)
            loompy.combine(mtx_paths, out_file)
            logger.info("Done combining.")
        except Exception as e:
            raise e
