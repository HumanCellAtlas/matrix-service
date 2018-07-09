import json
import os
import shutil
import tempfile
import boto3
import hca
import loompy

from abc import ABC, abstractmethod
from botocore.exceptions import ClientError
from chalicelib.constants import MERGED_MTX_BUCKET_NAME, \
    MERGED_REQUEST_STATUS_BUCKET_NAME, JSON_EXTENSION
from chalicelib.request_handler import RequestHandler, RequestStatus


class MatrixHandler(ABC):
    """
    A generic matrix handler for matrices concatenation
    """
    def __init__(self, extension):
        self._extension = extension
        self.hca_client = hca.dss.DSSClient()

    def _filter_mtx(self, bundle_uuids):
        """
        Filter only matrix files from a list of DSS bundles.
        :param bundle_uuids: A list of bundle uuids.
        :return: A list of matrices uuids.
        """
        mtx_uuids = []

        # Iterate uuids to query DSS for bundle manifest of each
        for uuid in bundle_uuids:
            bundle_manifest = self.hca_client.get_bundle(replica="aws", uuid=uuid)

            # Gather up uuids of all the matrix files we are going to merge
            for file_ in bundle_manifest["bundle"]["files"]:
                if file_["name"].endswith(self._extension):
                    mtx_uuids.append(file_["uuid"])

        return mtx_uuids

    def _download_mtx(self, bundle_uuids):
        """
        Filter for the matrix files within bundles, and download them locally
        TODO: Is there a way to directly connect to a s3 file instead of downloading it locally?

        :param bundle_uuids: A list of bundle uuids
        :return: A list of downloaded local matrix files paths and their directory
        """
        # app.log.info("Downloading matrices from bundles: %s.", str(bundle_uuids))

        # Filter uuids of matrix files within each bundle
        mtx_uuids = self._filter_mtx(bundle_uuids)

        # List to store downloaded mtx file paths
        local_mtx_paths = []

        # Create a temp directory for storing s3 matrix files
        temp_dir = tempfile.mkdtemp()

        # Download matrices from cloud to a local temp directory
        for uuid in mtx_uuids:
            _, path = tempfile.mkstemp(suffix=self._extension, dir=temp_dir)
            with open(path, "wb") as mtx:
                mtx.write(self.hca_client.get_file(uuid=uuid, replica="aws"))
            local_mtx_paths.append(path)

        # app.log.info("Done downloading %d matrix files.", len(local_mtx_paths))
        return temp_dir, local_mtx_paths

    @abstractmethod
    def _concat_mtx(self, mtx_paths, mtx_dir, request_id):
        """
        Concatenate a list of matrices, and save into a new file on disk.

        :param mtx_paths: A list of downloaded local matrix files paths.
        :param mtx_dir: The directory that contains the matrices.
        :param request_id: The request id for the matrices concatenation.
        :return: New concatenated matrix file's path.
        """

    def _upload_mtx(self, path):
        """
        Upload a matrix file into an s3 bucket.
        :param path: Path of the merged matrix.
        :return: S3 bucket key for uploading file.
        """
        # app.log.info("%s", "Uploading \"{}\" to s3 bucket: \"{}\".".format(os.path.basename(path), MERGED_MTX_BUCKET_NAME))
        s3 = boto3.resource("s3")
        key = os.path.basename(path)
        with open(path, "rb") as merged_matrix:
            s3.Bucket(MERGED_MTX_BUCKET_NAME).put_object(Key=key, Body=merged_matrix)
        # app.log.info("Done uploading.")

        # Remove local merged mtx after uploading it to s3
        shutil.rmtree(os.path.dirname(path))

        return key

    def run_merge_request(self, bundle_uuids, request_id):
        """
        Merge matrices within bundles, and upload the merged matrix to an s3 bucket

        :param bundle_uuids: Bundles' uuid for locating bundles in DSS
        :param request_id: Merge request id
        """
        mtx_dir, mtx_paths = self._download_mtx(bundle_uuids)
        merged_mtx_path = self._concat_mtx(mtx_paths, mtx_dir, request_id)
        self._upload_mtx(merged_mtx_path)

        # Update the request status
        RequestHandler.update_request_status(
            bundle_uuids,
            request_id,
            RequestStatus.DONE
        )

    def get_mtx_url(self, request_id):
        """
        Get url of a matrix in s3 bucket
        :param request_id: Matrices concatenation request id
        :return: URL of the matrix file
        """
        s3 = boto3.resource("s3")
        key = request_id + JSON_EXTENSION

        try:
            response = s3.Object(bucket_name=MERGED_REQUEST_STATUS_BUCKET_NAME, key=key).get()
            body = json.loads(response['Body'].read())
            return body["merged_mtx_url"]
        except ClientError as e:
            raise e


class LoomMatrixHandler(MatrixHandler):
    """
    Matrix handler for .loom file format
    """
    def __init__(self):
        super().__init__(".loom")

    def _concat_mtx(self, mtx_paths, mtx_dir, request_id):
        try:
            merged_mtx_dir = tempfile.mkdtemp()
            out_file = os.path.join(merged_mtx_dir, request_id + self._extension)
            # app.log.info("Combining matrices to %s.", out_file)
            loompy.combine(mtx_paths, out_file)
            # app.log.info("Done combining.")

        finally:
            shutil.rmtree(mtx_dir)

        return out_file
