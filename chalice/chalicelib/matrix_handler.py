import os
import tempfile
import traceback
import loompy

from abc import ABC, abstractmethod
from typing import List
from chalicelib import get_mtx_paths, get_size, rand_uuid, clean_dir
from chalicelib.config import MERGED_MTX_BUCKET_NAME, TEMP_DIR, s3_blob_store, logger, hca_client
from chalicelib.request_handler import RequestHandler, RequestStatus
from concurrent.futures import ThreadPoolExecutor


class MatrixHandler(ABC):
    """
    A generic matrix handler for matrices concatenation
    """

    def __init__(self, suffix) -> None:
        self._suffix = suffix

    @property
    def suffix(self):
        return self._suffix

    def _download_mtx(self, bundle_uuids: List[str], temp_dir: str) -> List[str]:
        """
        Filter for the matrix files within bundles, and download them locally.
        :param bundle_uuids: A list of bundle uuids.
        :param temp_dir: A temporary directory for storing all downloaded matrix files.
        :return: A list of downloaded matrix files paths.
        """
        jobs = []

        for bundle_uuid in bundle_uuids:
            dest_name = os.path.join(temp_dir, bundle_uuid)
            keywords = {
                "bundle_uuid": bundle_uuid,
                "replica": "aws",
                "dest_name": dest_name,
                "metadata_files": (),
                "data_files": (f'*{self.suffix}',)
            }
            jobs.append(keywords)

        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(lambda x: hca_client.download(**x), job) for job in jobs]

            # Force futures to join
            for f in futures:
                f.result()

        # Get all downloaded mtx paths from temp_dir
        local_mtx_paths = get_mtx_paths(temp_dir, self.suffix)

        logger.info(f'Done downloading {len(local_mtx_paths)} matrix files.')

        return local_mtx_paths

    @abstractmethod
    def _concat_mtx(self, mtx_paths: List[str], out_file: str) -> None:
        """
        Concatenate a list of matrices, and save into a new file on disk.
        :param mtx_paths: A list of downloaded local matrix files paths.
        :param out_file: Path to the concatenated matrix.
        """

    def _upload_mtx(self, path: str) -> (str, str):
        """
        Upload a matrix file into an s3 bucket.
        :param path: Path of the merged matrix.
        :return: Uploaded merged matrix url and the s3 key.
        """
        logger.info(f'Uploading \"{os.path.basename(path)}\" to s3 bucket: \"{MERGED_MTX_BUCKET_NAME}\".')

        key = f'{rand_uuid()}{self.suffix}'

        with open(path, "rb") as merged_matrix:
            s3_blob_store.upload_file_handle(
                bucket=MERGED_MTX_BUCKET_NAME,
                key=key,
                src_file_handle=merged_matrix
            )

        merged_mtx_url = f's3://{MERGED_MTX_BUCKET_NAME}/{key}'
        return key, merged_mtx_url

    def run_merge_request(self, bundle_uuids: List[str], request_id: str, job_id: str) -> None:
        """
        Merge matrices within bundles, and upload the merged matrix to an s3 bucket.

        :param bundle_uuids: Bundles' uuid for locating bundles in DSS.
        :param request_id: Merge request id.
        :param job_id: Job id of the request.
        """
        try:
            logger.info(f'Concatenate matrices for request_id: {request_id}')
            logger.info(f'tmp directory contains: {os.listdir(TEMP_DIR)}')
            logger.info(f'tmp directory usage: {os.statvfs(TEMP_DIR)}')

            # Clean /tmp folder before each run
            clean_dir(TEMP_DIR)

            # Update the request status to RUNNING
            RequestHandler.put_request(
                bundle_uuids=bundle_uuids,
                request_id=request_id,
                job_id=job_id,
                status=RequestStatus.RUNNING
            )

            # Create a temp directory for storing all temp files
            with tempfile.TemporaryDirectory(dir=TEMP_DIR, prefix=f'{request_id}_') as temp_dir:

                logger.info(f'Before download, the size of tmp directory is: {get_size(TEMP_DIR)} bytes.')
                logger.info(f'tmp directory contains: {os.listdir(TEMP_DIR)}.')
                logger.info(f'tmp directory usage: {os.statvfs(TEMP_DIR)}')
                mtx_paths = self._download_mtx(bundle_uuids=bundle_uuids, temp_dir=temp_dir)
                _, merged_mtx_path = tempfile.mkstemp(dir=temp_dir, prefix=request_id, suffix=self.suffix)

                logger.info(f'Before concat, the size of tmp directory is: {get_size(TEMP_DIR)} bytes.')
                logger.info(f'tmp directory contains: {os.listdir(TEMP_DIR)}.')
                logger.info(f'tmp directory usage: {os.statvfs(TEMP_DIR)}')
                self._concat_mtx(mtx_paths=mtx_paths, out_file=merged_mtx_path)

                logger.info(f'Before upload, the size of tmp directory is: {get_size(TEMP_DIR)} bytes.')
                logger.info(f'tmp directory contains: {os.listdir(TEMP_DIR)}.')
                logger.info(f'tmp directory usage: {os.statvfs(TEMP_DIR)}')
                _, merged_mtx_url = self._upload_mtx(path=merged_mtx_path)

                # Update the request status to DONE
                RequestHandler.put_request(
                    bundle_uuids=bundle_uuids,
                    request_id=request_id,
                    job_id=job_id,
                    status=RequestStatus.DONE,
                    merged_mtx_url=merged_mtx_url
                )

        except Exception as e:
            # Update the request status to ABORT
            RequestHandler.put_request(
                bundle_uuids=bundle_uuids,
                request_id=request_id,
                job_id=job_id,
                status=RequestStatus.ABORT,
                reason_to_abort=traceback.format_exc()
            )

            raise e


class LoomMatrixHandler(MatrixHandler):
    """
    Matrix handler for .loom file format
    """

    def __init__(self) -> None:
        super().__init__(".loom")

    def _concat_mtx(self, mtx_paths: List[str], out_file: str) -> None:
        try:
            logger.info(f'Combining matrices to {out_file}.')
            loompy.combine(mtx_paths, out_file)
            logger.info(f'Done combining.')
        except Exception as e:
            logger.info(f'Exception caught, the size of tmp directory is: {get_size(TEMP_DIR)} bytes')
            logger.info(f'tmp directory usage: {os.statvfs(TEMP_DIR)}')
            logger.info(f'tmp directory contains: {os.listdir(TEMP_DIR)}')
            raise e
