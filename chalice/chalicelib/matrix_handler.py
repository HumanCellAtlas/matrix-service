import os
import tempfile
import traceback
import loompy

from abc import ABC, abstractmethod
from typing import List
from chalicelib import get_mtx_paths, get_size, rand_uuid, clean_dir
from chalicelib.config import MERGED_MTX_BUCKET_NAME, TEMP_DIR, s3_blob_store, logger
from chalicelib.hca_download_worker import HcaDownloadWorker
from chalicelib.request_handler import RequestHandler, RequestStatus


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
        jobs_pool = list(bundle_uuids)
        hca_download_workers = dict()
        parallelism = 4

        # Create and assign worker to work on a job only when needed
        while len(jobs_pool):
            wip_jobs = list(hca_download_workers.keys())

            for wip_job in wip_jobs:
                hca_download_worker = hca_download_workers[wip_job]

                # Catch the exception happens in the worker
                if hca_download_worker.exception is not None:
                    logger.info(f'Exception caught, the size of tmp directory is: {get_size(TEMP_DIR)} bytes.')
                    logger.info(f'tmp directory contains: {os.listdir(TEMP_DIR)}.')
                    raise hca_download_worker.exception

                # Kick off the worker from the list when it finishes its work
                if hca_download_worker.exitcode is not None:
                    del hca_download_workers[wip_job]

            # Assign jobs to workers until reaching the maximum parallelism or there is no more job left.
            while len(jobs_pool) > 0 and len(hca_download_workers) < parallelism:
                job = jobs_pool.pop()

                dest_name = os.path.join(temp_dir, job)
                keywords = {
                    "bundle_uuid": job,
                    "replica": "aws",
                    "dest_name": dest_name,
                    "metadata_files": (),
                    "data_files": (f'*{self.suffix}',)
                }

                hca_download_workers[job] = HcaDownloadWorker(**keywords)
                hca_download_workers[job].start()

        for hca_download_worker in hca_download_workers.values():
            hca_download_worker.join()

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
        try:
            logger.info("Concatenate matrices for request_id: {}".format(request_id))

            # Print out the current usage of /tmp/ directory
            call(["df", "-H", TEMP_DIR])

            logger.info("tmp directory contains: {}".format(str(os.listdir(TEMP_DIR))))

            # Clean /tmp folder before each run
            call('rm -rf /tmp/*', shell=True)

            logger.info("After cleaning, /tmp directory contains: {}".format(str(os.listdir(TEMP_DIR))))

            # Update the request status to RUNNING
            RequestHandler.update_request(
                bundle_uuids=bundle_uuids,
                request_id=request_id,
                job_id=job_id,
                status=RequestStatus.RUNNING
            )

            # Create a temp directory for storing all temp files
            with tempfile.TemporaryDirectory(dir=TEMP_DIR, prefix="{}_".format(request_id)) as temp_dir:

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
            temp_files = os.listdir(TEMP_DIR)
            logger.info("tmp directory contains: {}".format(str(temp_files)))

            # Update the request status to ABORT
            RequestHandler.update_request(
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
            logger.info("Combining matrices to %s.", out_file)
            loompy.combine(mtx_paths, out_file)
            logger.info("Done combining.")
        except Exception as e:
            raise e
