import json
import os
import shutil
import tempfile
import hca
import vendor.loompy as loompy

from hca.util import SwaggerAPIException
from typing import List, Tuple
from abc import ABC, abstractmethod
from chalicelib import get_mtx_paths, s3_blob_store, logger
from chalicelib.config import MERGED_MTX_BUCKET_NAME, \
    REQUEST_STATUS_BUCKET_NAME, JSON_SUFFIX, TEMP_DIR
from chalicelib.request_handler import RequestHandler, RequestStatus
from cloud_blobstore import BlobStoreUnknownError, BlobNotFoundError


class MatrixHandler(ABC):
    """
    A generic matrix handler for matrices concatenation
    """
    def __init__(self, suffix) -> None:
        self._suffix = suffix
        self.hca_client = hca.dss.DSSClient()

        # Set this under the dev stage
        self.hca_client.host = "https://dss.dev.data.humancellatlas.org/v1"

    def _download_mtx(self, bundle_uuids) -> Tuple[str, List[str]]:
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

                self.hca_client.download(
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

    @abstractmethod
    def _concat_mtx(self, mtx_paths, mtx_dir, request_id) -> str:
        """
        Concatenate a list of matrices, and save into a new file on disk.

        :param mtx_paths: A list of downloaded local matrix files paths.
        :param mtx_dir: The directory that contains the matrices.
        :param request_id: The request id for the matrices concatenation.
        :return: New concatenated matrix file's path.
        """

    def _upload_mtx(self, path) -> str:
        """
        Upload a matrix file into an s3 bucket.
        :param path: Path of the merged matrix.
        :return: S3 bucket key for uploading file.
        """
        logger.info("%s", "Uploading \"{}\" to s3 bucket: \"{}\".".format(
            os.path.basename(path),
            MERGED_MTX_BUCKET_NAME
        ))

        key = os.path.basename(path)
        with open(path, "rb") as merged_matrix:
            s3_blob_store.upload_file_handle(
                bucket=MERGED_MTX_BUCKET_NAME,
                key=key,
                src_file_handle=merged_matrix
            )

        logger.info("Done uploading.")

        # Remove local merged mtx after uploading it to s3
        shutil.rmtree(os.path.dirname(path))

        return key

    def run_merge_request(self, bundle_uuids, request_id, job_id) -> None:
        """
        Merge matrices within bundles, and upload the merged matrix to an s3 bucket.

        :param bundle_uuids: Bundles' uuid for locating bundles in DSS.
        :param request_id: Merge request id.
        :param job_id: Job id of the request.
        """
        # Update the request status to RUNNING
        RequestHandler.update_request_status(
            bundle_uuids=bundle_uuids,
            request_id=request_id,
            job_id=job_id,
            status=RequestStatus.RUNNING
        )

        try:
            mtx_dir, mtx_paths = self._download_mtx(bundle_uuids)
            merged_mtx_path = self._concat_mtx(mtx_paths, mtx_dir, request_id)
            self._upload_mtx(merged_mtx_path)

            # Update the request status to DONE
            RequestHandler.update_request_status(
                bundle_uuids=bundle_uuids,
                request_id=request_id,
                job_id=job_id,
                status=RequestStatus.DONE
            )
        except SwaggerAPIException as e:

            # Update the request status to ABORT
            RequestHandler.update_request_status(
                bundle_uuids=bundle_uuids,
                request_id=request_id,
                job_id=job_id,
                status=RequestStatus.ABORT
            )

            raise e

    def get_mtx_url(self, request_id) -> str:
        """
        Get url of a matrix in s3 bucket
        :param request_id: Matrices concatenation request id
        :return: URL of the matrix file
        """
        key = request_id + JSON_SUFFIX

        try:
            body = json.loads(s3_blob_store.get(bucket=REQUEST_STATUS_BUCKET_NAME, key=key))
            return body["merged_mtx_url"]
        except BlobStoreUnknownError as e:
            raise e
        except BlobNotFoundError as e:
            raise e


class LoomMatrixHandler(MatrixHandler):
    """
    Matrix handler for .loom file format
    """
    def __init__(self) -> None:
        super().__init__(".loom")

    def _concat_mtx(self, mtx_paths, mtx_dir, request_id) -> str:
        try:
            merged_mtx_dir = tempfile.mkdtemp(dir=TEMP_DIR)
            out_file = os.path.join(merged_mtx_dir, request_id + self._suffix)
            logger.info("Combining matrices to %s.", out_file)
            loompy.combine(mtx_paths, out_file)
            logger.info("Done combining.")

        finally:
            shutil.rmtree(mtx_dir)

        return out_file
