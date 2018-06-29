import json
import os
import tempfile
import boto3
import hca

from abc import ABC, abstractmethod
from botocore.exceptions import ClientError
from chalicelib.constants import MERGED_MTX_BUCKET_NAME, MERGED_REQUEST_STATUS_BUCKET_NAME
from loompy import loompy
from chalicelib import logger


class MatrixHandler(ABC):
    """
    A generic matrix handler for matrices concatenation
    """
    def __init__(self, extension):
        self._extension = extension

    def _download_mtx(self, bundle_uuids):
        """
        Filter for the matrix files within bundles, and download them locally
        TODO: Is there a way to directly connect to a s3 file instead of downloading it locally?

        :param bundle_uuids: A list of bundle uuids
        :return: A list of downloaded local matrix files paths
        """
        logger.info("Downloading matrices from bundles: %s.", str(bundle_uuids))

        client = hca.dss.DSSClient()

        mtx_uuids = []

        # Iterate uuids to query DSS for bundle manifest of each
        for uuid in bundle_uuids:
            bundle_manifest = client.get_bundle(replica="aws", uuid=uuid)

            # Gather up uuids of all the matrix files we are going to merge
            for file in bundle_manifest["bundle"]["files"]:
                if file["name"].endswith(self._extension):
                    mtx_uuids.append(file["uuid"])
                    logger.info("Adding %s to merge.", file["name"])

        # Create a temp directory for storing s3 matrix files
        temp_dir = tempfile.mkdtemp()

        local_mtx_paths = []

        # Download matrices from cloud to a local temp directory
        for uuid in mtx_uuids:
            path = os.path.join(temp_dir, uuid + self._extension)
            with open(path, "wb") as mtx:
                mtx.write(client.get_file(uuid=uuid, replica="aws"))
            local_mtx_paths.append(path)

        logger.info("Done downloading %d matrix files.", len(local_mtx_paths))
        return local_mtx_paths

    @abstractmethod
    def _concat_mtx(self, paths, request_id):
        """
        Concatenate a list of matrices, and save into a new file on disk

        :param paths: A list of downloaded local matrix files paths
        :param request_id: The request id for the matrices concatenation
        :return: New concatenated matrix file's path
        """

    def _upload_mtx(self, path):
        """
        Upload a matrix file into an s3 bucket
        :param path: Path of the merged matrix
        """
        logger.info("%s", "Uploading \"{}\" to s3 bucket: \"{}\".".format(os.path.basename(path), MERGED_MTX_BUCKET_NAME))
        s3 = boto3.resource("s3")
        key = os.path.basename(path)
        with open(path, "rb") as merged_matrix:
            s3.Bucket(MERGED_MTX_BUCKET_NAME).put_object(Key=key, Body=merged_matrix)
        logger.info("Done uploading.")

    def run_merge_request(self, bundle_uuids, request_id):
        """
        Merge matrices within bundles, and upload the merged matrix to an s3 bucket

        :param bundle_uuids: Bundles' uuid for locating bundles in DSS
        :param request_id: Merge request id
        :return:
        """
        mtx_paths = self._download_mtx(bundle_uuids)
        merged_mtx_path = self._concat_mtx(mtx_paths, request_id)
        self._upload_mtx(merged_mtx_path)

        # TODO: Update the request status

    def get_mtx_url(self, request_id):
        """
        Get url of a matrix in s3 bucket
        :param request_id: Matrices concatenation request id
        :return: URL of the matrix file
        """
        s3 = boto3.resource("s3")
        key = request_id + ".json"

        try:
            response = s3.Object(bucket_name=MERGED_REQUEST_STATUS_BUCKET_NAME, key=key).get()
            body = json.loads(response['Body'].read())
            return body["url"]
        except ClientError as e:
            raise e


class LoomMatrixHandler(MatrixHandler):
    """
    Matrix handler for .loom file format
    """
    def __init__(self):
        super().__init__(".loom")
        self._merged_mtx_dir = tempfile.mkdtemp()

    def _concat_mtx(self, paths, request_id):
        out_file = os.path.join(self._merged_mtx_dir, request_id + self._extension)
        logger.info("Combining matrices to %s.", request_id + self._extension)
        loompy.combine(paths, out_file)
        logger.info("Done combining.")
        return out_file
