import concurrent.futures
import unittest
import uuid
from unittest import mock

import psycopg2
from dcplib.etl import DSSExtractor

from matrix.common.aws.redshift_handler import TableName
from matrix.common.constants import CREATE_QUERY_TEMPLATE
from matrix.common.etl import (run_etl,
                               transform_bundle,
                               finalizer_reload,
                               finalizer_update,
                               load_from_local_files,
                               load_from_s3,
                               _upload_to_s3,
                               _populate_all_tables,
                               _create_tables,
                               get_dss_client)


class TestEtl(unittest.TestCase):
    def setUp(self):
        self.stub_swagger_spec = {
            'info': {
                'description': "test_description"
            },
            'host': "test_host",
            'basePath': "/v1",
            'paths': {}
        }

    @mock.patch("matrix.common.etl.get_dss_client")
    @mock.patch("dcplib.etl.DSSExtractor.extract")
    @mock.patch("os.makedirs")
    def test_run_etl(self, mock_makedirs, mock_extract, mock_get_dss_client):
        run_etl(query={},
                content_type_patterns=[],
                filename_patterns=[],
                transformer_cb=self.stub_transformer,
                finalizer_cb=self.stub_finalizer,
                staging_directory="test_dir",
                deployment_stage="test_stage",
                max_workers=8)

        expected_makedirs_calls = [
            mock.call("test_dir/output/cell", exist_ok=True),
            mock.call("test_dir/output/expression", exist_ok=True)
        ]

        mock_get_dss_client.assert_called_once_with("test_stage")
        mock_makedirs.assert_has_calls(expected_makedirs_calls)
        mock_extract.assert_called_once_with(query={},
                                             transformer=self.stub_transformer,
                                             finalizer=self.stub_finalizer,
                                             max_workers=8,
                                             max_dispatchers=1,
                                             dispatch_executor_class=concurrent.futures.ProcessPoolExecutor)

    @mock.patch("hca.dss.DSSClient.swagger_spec", new_callable=mock.PropertyMock)
    @mock.patch("matrix.common.etl.logger.error")
    @mock.patch("matrix.common.etl.transformers.cell_expression.CellExpressionTransformer.transform")
    def test_transform_bundle(self, mock_cell_expression_transform, mock_error, mock_swagger_spec):
        mock_swagger_spec.return_value = self.stub_swagger_spec
        extractor = DSSExtractor(staging_directory="test_dir",
                                 content_type_patterns=[],
                                 filename_patterns=[],
                                 dss_client=get_dss_client("dev"))

        transform_bundle("test_uuid", "test_version", "test_path", "test_manifest_path", extractor)
        mock_cell_expression_transform.assert_called_once_with("test_path")

        e = Exception()
        mock_cell_expression_transform.side_effect = e
        transform_bundle("test_uuid", "test_version", "test_path", "test_manifest_path", extractor)
        mock_error.assert_called_once_with("Failed to transform bundle test_uuid.test_version.", e)

    @mock.patch("hca.dss.DSSClient.swagger_spec", new_callable=mock.PropertyMock)
    @mock.patch("matrix.common.etl.load_from_local_files")
    @mock.patch("matrix.common.etl.logger.error")
    @mock.patch("matrix.common.etl.transformers."
                "project_publication_contributor.ProjectPublicationContributorTransformer.transform")
    @mock.patch("matrix.common.etl.transformers.specimen_library.SpecimenLibraryTransformer.transform")
    @mock.patch("matrix.common.etl.transformers.analysis.AnalysisTransformer.transform")
    @mock.patch("matrix.common.etl.transformers.feature.FeatureTransformer._fetch_annotations")
    @mock.patch("matrix.common.etl.transformers.feature.FeatureTransformer.transform")
    def test_finalizer_reload(self,
                              mock_feature_transformer,
                              mock_fetch_annotations,
                              mock_analysis_transformer,
                              mock_specimen_library_transformer,
                              mock_project_publication_contributor_transformer,
                              mock_error,
                              mock_load_from_local_files,
                              mock_swagger_spec):
        mock_swagger_spec.return_value = self.stub_swagger_spec
        extractor = DSSExtractor(staging_directory="test_dir",
                                 content_type_patterns=[],
                                 filename_patterns=[],
                                 dss_client=get_dss_client("dev"))

        finalizer_reload(extractor)
        mock_feature_transformer.assert_called_once_with("test_dir/bundles")
        mock_analysis_transformer.assert_called_once_with("test_dir/bundles")
        mock_specimen_library_transformer.assert_called_once_with("test_dir/bundles")
        mock_project_publication_contributor_transformer.assert_called_once_with("test_dir/bundles")
        mock_load_from_local_files.assert_called_once_with("test_dir", is_update=False)

        mock_load_from_local_files.reset_mock()
        mock_feature_transformer.side_effect = Exception()
        finalizer_reload(extractor)
        self.assertTrue(mock_error.called)
        mock_load_from_local_files.assert_called_once_with("test_dir", is_update=False)

    @mock.patch("hca.dss.DSSClient.swagger_spec", new_callable=mock.PropertyMock)
    @mock.patch("matrix.common.etl.load_from_local_files")
    @mock.patch("matrix.common.etl.logger.error")
    @mock.patch("matrix.common.etl.transformers."
                "project_publication_contributor.ProjectPublicationContributorTransformer.transform")
    @mock.patch("matrix.common.etl.transformers.specimen_library.SpecimenLibraryTransformer.transform")
    @mock.patch("matrix.common.etl.transformers.analysis.AnalysisTransformer.transform")
    @mock.patch("matrix.common.etl.transformers.feature.FeatureTransformer._fetch_annotations")
    @mock.patch("matrix.common.etl.transformers.feature.FeatureTransformer.transform")
    def test_finalizer_update(self,
                              mock_feature_transformer,
                              mock_fetch_annotations,
                              mock_analysis_transformer,
                              mock_specimen_library_transformer,
                              mock_project_publication_contributor_transformer,
                              mock_error,
                              mock_load_from_local_files,
                              mock_swagger_spec):
        mock_swagger_spec.return_value = self.stub_swagger_spec
        extractor = DSSExtractor(staging_directory="test_dir",
                                 content_type_patterns=[],
                                 filename_patterns=[],
                                 dss_client=get_dss_client("dev"))

        finalizer_update(extractor)
        self.assertFalse(mock_feature_transformer.called)
        mock_analysis_transformer.assert_called_once_with("test_dir/bundles")
        mock_specimen_library_transformer.assert_called_once_with("test_dir/bundles")
        mock_project_publication_contributor_transformer.assert_called_once_with("test_dir/bundles")
        mock_load_from_local_files.assert_called_once_with("test_dir", is_update=True)

        mock_load_from_local_files.reset_mock()
        mock_analysis_transformer.side_effect = Exception()
        finalizer_update(extractor)
        self.assertTrue(mock_error.called)
        mock_load_from_local_files.assert_called_once_with("test_dir", is_update=True)

    @mock.patch("matrix.common.etl._populate_all_tables")
    @mock.patch("matrix.common.etl._upload_to_s3")
    def test_load_from_local_files(self, mock_upload_to_s3, mock_populate_all_tables):
        load_from_local_files("test_dir")
        mock_upload_to_s3.assert_called_once_with("test_dir/output", mock.ANY)
        mock_populate_all_tables.assert_called_once_with(mock.ANY, is_update=False)

    @mock.patch("matrix.common.etl._populate_all_tables")
    def test_load_from_s3(self, mock_populate_all_tables):
        load_from_s3("test_id")
        mock_populate_all_tables.assert_called_once_with("test_id", is_update=False)

    @mock.patch("matrix.common.etl._upload_file_to_s3")
    def test_upload_to_s3(self, mock_upload_file_to_s3):
        job_id = str(uuid.uuid4())
        out_dir = "tests/functional/res/etl/psv"
        _upload_to_s3(out_dir, job_id)

        expected_calls = [
            mock.call(f"{out_dir}/example/test.psv", f"{job_id}/example/test.psv"),
            mock.call(f"{out_dir}/test.psv", f"{job_id}/test.psv"),
        ]
        mock_upload_file_to_s3.assert_has_calls(expected_calls)

    @mock.patch("matrix.common.etl.logger.error")
    @mock.patch("matrix.common.etl._create_tables")
    @mock.patch("matrix.common.aws.redshift_handler.RedshiftHandler.transaction")
    def test_populate_all_tables_reload(self, mock_transaction, mock_create_tables, mock_error):
        job_id = str(uuid.uuid4())
        _populate_all_tables(job_id, is_update=False)
        mock_transaction.assert_called_once_with(mock.ANY)

        mock_transaction.side_effect = psycopg2.Error()
        _populate_all_tables(job_id, is_update=False)
        self.assertTrue(mock_error.called)

    @mock.patch("matrix.common.etl.logger.error")
    @mock.patch("matrix.common.etl._create_tables")
    @mock.patch("matrix.common.aws.redshift_handler.RedshiftHandler.transaction")
    def test_populate_all_tables_update(self, mock_transaction, mock_create_tables, mock_error):
        job_id = str(uuid.uuid4())
        _populate_all_tables(job_id, is_update=True)
        mock_transaction.assert_called_once_with(mock.ANY)

        mock_transaction.side_effect = psycopg2.Error()
        _populate_all_tables(job_id, is_update=True)
        self.assertTrue(mock_error.called)

    @mock.patch("matrix.common.aws.redshift_handler.RedshiftHandler.transaction")
    def test_create_tables(self, mock_transaction):
        transaction = []
        for table in TableName:
            transaction.append(CREATE_QUERY_TEMPLATE[table.value].format("", "", table.value))

        _create_tables()
        mock_transaction.assert_called_once_with(transaction)

    @mock.patch("hca.dss.DSSClient.swagger_spec", new_callable=mock.PropertyMock)
    def test_get_dss_client(self, mock_swagger_spec):
        mock_swagger_spec.return_value = self.stub_swagger_spec

        self.stub_swagger_spec['host'] = "dss.integration.data.humancellatlas.org"
        self.assertEqual(get_dss_client("integration").host, "https://dss.integration.data.humancellatlas.org/v1")

        self.stub_swagger_spec['host'] = "dss.staging.data.humancellatlas.org"
        self.assertEqual(get_dss_client("staging").host, "https://dss.staging.data.humancellatlas.org/v1")

        self.stub_swagger_spec['host'] = "dss.data.humancellatlas.org"
        self.assertEqual(get_dss_client("predev").host, "https://dss.data.humancellatlas.org/v1")
        self.assertEqual(get_dss_client("dev").host, "https://dss.data.humancellatlas.org/v1")
        self.assertEqual(get_dss_client("prod").host, "https://dss.data.humancellatlas.org/v1")

    def stub_transformer(self):
        pass

    def stub_finalizer(self):
        pass
