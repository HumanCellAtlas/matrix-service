import concurrent.futures
import os
import shutil

from matrix.common import etl
from matrix.common.aws.redshift_handler import RedshiftHandler
from matrix.common.etl.transformers import MetadataToPsvTransformer
from matrix.common.logging import Logging

logger = Logging.get_logger(__name__)


class NotificationHandler:

    def __init__(self, bundle_uuid, bundle_version, event_type):
        logger.info(f"Running NotificationHandler with parameters: {bundle_uuid}, {bundle_version}, {event_type}")
        self.bundle_uuid = bundle_uuid
        self.bundle_version = bundle_version
        self.event_type = event_type

        self.redshift = RedshiftHandler()

    def run(self):
        if self.event_type == 'CREATE':
            self.update_bundle()
        elif self.event_type == 'DELETE' or self.event_type == 'TOMBSTONE':
            self.remove_bundle()
        else:
            logger.error(f"Failed to process notification. Received invalid event type {self.event_type}.")
            return

        logger.info(f"Done processing DSS notification for {self.bundle_uuid}.{self.bundle_version}.")

    def update_bundle(self):
        query = {
            "query": {
                "bool": {
                    "must": [{"term": {"uuid": self.bundle_uuid}}]
                }
            }
        }
        content_type_patterns = ['application/json; dcp-type="metadata*"']
        filename_patterns = ["*zarr*",  # match expression data
                             "*.results",  # match SS2 results files
                             "*.mtx", "genes.tsv", "barcodes.tsv"]  # match 10X results files

        # clean up working directory in case of Lambda container reuse
        shutil.rmtree(f"/tmp/{MetadataToPsvTransformer.OUTPUT_DIRNAME}", ignore_errors=True)
        etl.run_etl(query=query,
                    content_type_patterns=content_type_patterns,
                    filename_patterns=filename_patterns,
                    transformer_cb=etl.transform_bundle,
                    finalizer_cb=etl.finalizer_update,
                    staging_directory=os.path.abspath("/tmp"),
                    deployment_stage=os.environ['DEPLOYMENT_STAGE'],
                    max_workers=16,
                    dispatcher_executor_class=concurrent.futures.ThreadPoolExecutor)

    def remove_bundle(self):
        delete_query = f"DELETE FROM analysis WHERE bundle_fqid='{self.bundle_uuid}.{self.bundle_version}'"
        logger.info(f"Executing query: {delete_query}")
        self.redshift.transaction([delete_query])
