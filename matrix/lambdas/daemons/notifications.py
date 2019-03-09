import os

from matrix.common.aws.redshift_handler import RedshiftHandler
from matrix.common.etl import run_etl, transform_bundle, finalizer_update
from matrix.common.logging import Logging

logger = Logging.get_logger(__name__)


class NotificationsHandler:

    def __init__(self, bundle_uuid, bundle_version, event_type):
        logger.info(f"Running NotificationsHandler with parameters: {bundle_uuid}, {bundle_version}, {event_type}")
        self.bundle_uuid = bundle_uuid
        self.bundle_version = bundle_version
        self.event_type = event_type

        self.redshift = RedshiftHandler()

    def run(self):
        if self.event_type == 'CREATE':
            self.update_bundle()
        elif self.event_type == 'DELETE' or self.event_type == 'TOMBSTONE':
            self.remove_bundle()

    def update_bundle(self):
        query = {
            "query": {
                "bool": {
                    "must": [{"term": {"uuid": self.bundle_uuid}}]
                }
            }
        }
        content_type_patterns = ['application/json; dcp-type="metadata*"']
        filename_patterns = ["*zarr*", # match expression data
                             "*.results", # match SS2 raw count files
                             "*.mtx", "genes.tsv", "barcodes.tsv"] # match 10X raw count files

        run_etl(query=query,
                content_type_patterns=content_type_patterns,
                filename_patterns=filename_patterns,
                transformer_cb=transform_bundle,
                finalizer_cb=finalizer_update,
                staging_directory=os.path.abspath("/tmp"),
                deployment_stage=os.environ['DEPLOYMENT_STAGE'],
                max_workers=16)

        logger.info(f"Done processing DSS notification for {self.bundle_uuid}.{self.bundle_version}")

    def remove_bundle(self):
        delete_query = f"DELETE FROM analysis WHERE bundle_fqid='{self.bundle_uuid}.{self.bundle_version}'"
        logger.info(f"Executing query: {delete_query}")
        self.redshift.run_query(delete_query)
