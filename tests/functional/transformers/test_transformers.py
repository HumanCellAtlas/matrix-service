import concurrent.futures
import os
import shutil
import unittest

from matrix.common.constants import MATRIX_ENV_TO_DSS_ENV, BundleType
from matrix.common.etl import etl_dss_bundles
from matrix.common.etl.transformers.analysis import AnalysisTransformer
from matrix.common.etl.transformers.cell_expression import CellExpressionTransformer
from matrix.common.etl.transformers.project_publication_contributor import ProjectPublicationContributorTransformer
from matrix.common.etl.transformers.specimen_library import SpecimenLibraryTransformer
from matrix.common.logging import Logging
from tests.functional.transformers import ETL_TEST_BUNDLES
from tests.functional.transformers.validation import (AnalysisValidator,
                                                      CellExpressionValidator,
                                                      ProjectPublicationContributorValidator,
                                                      SpecimenLibraryValidator)

logger = Logging.get_logger(__name__)


class TestTransformers(unittest.TestCase):
    DEPLOYMENT_STAGE = os.environ['DEPLOYMENT_STAGE']
    DSS_STAGE = MATRIX_ENV_TO_DSS_ENV[DEPLOYMENT_STAGE]
    TRANSFORMERS = [
        AnalysisTransformer,
        CellExpressionTransformer,
        ProjectPublicationContributorTransformer,
        SpecimenLibraryTransformer
    ]
    VALIDATORS = {
        'AnalysisTransformer': AnalysisValidator,
        'CellExpressionTransformer': CellExpressionValidator,
        'ProjectPublicationContributorTransformer': ProjectPublicationContributorValidator,
        'SpecimenLibraryTransformer': SpecimenLibraryValidator
    }
    OUTPUT_DIR = os.path.abspath("./tests/functional/res/etl/test_transformers_output")
    TEST_BUNDLES = ETL_TEST_BUNDLES[DSS_STAGE]

    @classmethod
    def setUpClass(cls):
        os.makedirs(TestTransformers.OUTPUT_DIR, exist_ok=True)

    @classmethod
    def tearDownClass(cls):
        TestTransformers._cleanup()

    def test_transformers(self):
        for btype in BundleType:
            for bundle_uuid in self.TEST_BUNDLES[btype]:
                TestTransformers.setUpClass()
                logger.info(f"Testing ETL transformers for {btype} bundle {bundle_uuid}")
                self._download_bundle(bundle_uuid)
                self._transform_and_validate(bundle_uuid,
                                             btype,
                                             expected_rows=self.TEST_BUNDLES[btype][bundle_uuid])
                TestTransformers._cleanup()

    @staticmethod
    def _transform_and_validate(bundle_uuid: str, btype: BundleType, expected_rows: dict):
        bundles_dir = os.path.join(TestTransformers.OUTPUT_DIR, "bundles")
        bundle_dir = os.path.join(bundles_dir, os.listdir(bundles_dir)[0])

        for transformer_class in TestTransformers.TRANSFORMERS:
            transformer = transformer_class(bundle_dir)
            logger.info(f"Testing {transformer.__class__.__name__} on {bundle_uuid}")

            actual_rows = transformer._parse_from_metadatas(bundle_dir)

            validator = TestTransformers.VALIDATORS[transformer.__class__.__name__]()
            validator.validate(actual_rows, expected_rows, btype)

    @staticmethod
    def _download_bundle(bundle_uuid: str):
        logger.info(f"Downloading bundle {bundle_uuid}")

        query = {
            "query": {
                "bool": {
                    "must": [{"term": {"uuid": bundle_uuid}}]
                }
            }
        }
        content_type_patterns = ['application/json; dcp-type="metadata*"']
        filename_patterns = ["*zarr*",  # match expression data
                             "*.results",  # match SS2 results files
                             "*.mtx", "genes.tsv", "barcodes.tsv"]  # match 10X results files

        etl_dss_bundles(query=query,
                        content_type_patterns=content_type_patterns,
                        filename_patterns=filename_patterns,
                        transformer_cb=None,
                        finalizer_cb=None,
                        staging_directory=os.path.abspath(TestTransformers.OUTPUT_DIR),
                        deployment_stage=TestTransformers.DEPLOYMENT_STAGE,
                        max_workers=4,
                        dispatcher_executor_class=concurrent.futures.ThreadPoolExecutor)

    @staticmethod
    def _cleanup():
        shutil.rmtree(TestTransformers.OUTPUT_DIR, ignore_errors=True)
