import unittest

import mock

from matrix.common import constants
from matrix.common.constants import MetadataSchemaName
from scripts.redshift.loader import (load,
                                     _build_dss_query,
                                     _generate_metadata_schema_version_clause,
                                     _verify_load,
                                     DSS_SEARCH_QUERY_TEMPLATE,
                                     ALL_10X_SS2_MATCH_TERMS)


class TestLoader(unittest.TestCase):
    def setUp(self):
        self.default_args = TestLoader.ArgsStub(max_workers=1,
                                                state=0,
                                                s3_upload_id=None,
                                                project_uuids=None,
                                                bundle_fqids=None)

    @mock.patch("scripts.redshift.loader._verify_load")
    @mock.patch("matrix.common.etl.etl_dss_bundles")
    @mock.patch("scripts.redshift.loader._build_dss_query")
    def test_load__complete(self,
                            mock_build_dss_query,
                            mock_etl_dss_bundles,
                            mock_verify_load):
        args = self.default_args
        load(args)

        mock_build_dss_query.assert_called_once_with(project_uuids=args.project_uuids, bundle_fqids=args.bundle_fqids)
        mock_etl_dss_bundles.assert_called_once()
        mock_verify_load.assert_called_once_with(es_query=mock_build_dss_query.return_value)

    @mock.patch("scripts.redshift.loader._verify_load")
    @mock.patch("matrix.common.etl.upload_and_load")
    @mock.patch("matrix.common.etl.etl_dss_bundles")
    @mock.patch("scripts.redshift.loader._build_dss_query")
    def test_load__skip_transform(self,
                                  mock_build_dss_query,
                                  mock_etl_dss_bundles,
                                  mock_upload_and_load,
                                  mock_verify_load):
        args = self.default_args
        args.state = 2
        load(args)

        mock_build_dss_query.assert_called_once_with(project_uuids=args.project_uuids, bundle_fqids=args.bundle_fqids)
        mock_etl_dss_bundles.assert_not_called()
        mock_upload_and_load.assert_called_once_with("/mnt", is_update=False)
        mock_verify_load.assert_called_once_with(es_query=mock_build_dss_query.return_value)

    @mock.patch("scripts.redshift.loader._verify_load")
    @mock.patch("matrix.common.etl.load_tables")
    @mock.patch("matrix.common.etl.upload_and_load")
    @mock.patch("matrix.common.etl.etl_dss_bundles")
    @mock.patch("scripts.redshift.loader._build_dss_query")
    def test_load__skip_upload__is_update(self,
                                          mock_build_dss_query,
                                          mock_etl_dss_bundles,
                                          mock_upload_and_load,
                                          mock_load_tables,
                                          mock_verify_load):
        args = self.default_args
        args.state = 3
        args.project_uuids = ["test_uuid"]
        load(args)

        mock_build_dss_query.assert_called_once_with(project_uuids=args.project_uuids, bundle_fqids=args.bundle_fqids)
        mock_etl_dss_bundles.assert_not_called()
        mock_upload_and_load.assert_not_called()
        mock_load_tables.assert_called_once_with(args.s3_upload_id, is_update=True)
        mock_verify_load.assert_called_once_with(es_query=mock_build_dss_query.return_value)

    @mock.patch("scripts.redshift.loader._generate_metadata_schema_version_clause")
    def test_build_dss_query(self, mock_generate_metadata_schema_version_clause):
        mock_generate_metadata_schema_version_clause.return_value = {}

        with self.subTest("Default select all query"):
            expected_query = DSS_SEARCH_QUERY_TEMPLATE
            expected_query['query']['bool']['must'][0]['bool']['should'] = ALL_10X_SS2_MATCH_TERMS
            for name in MetadataSchemaName:
                expected_query['query']['bool']['must'].append(
                    mock_generate_metadata_schema_version_clause.return_value)

            query = _build_dss_query(None)
            self.assertEqual(query, expected_query)

        with self.subTest("1 project uuid"):
            project_uuids = ["test_uuid"]
            expected_query = DSS_SEARCH_QUERY_TEMPLATE
            expected_query['query']['bool']['must'][0]['bool']['should'].append(
                {
                    'match': {
                        'files.project_json.provenance.document_id': "test_uuid"
                    }
                }
            )

            query = _build_dss_query(project_uuids)
            self.assertEqual(query, expected_query)

        with self.subTest("2 project uuids"):
            project_uuids = ["test_uuid_1", "test_uuid_2"]
            expected_query = DSS_SEARCH_QUERY_TEMPLATE
            expected_query['query']['bool']['must'][0]['bool']['should'].extend([
                {
                    'match': {
                        'files.project_json.provenance.document_id': "test_uuid_1"
                    }
                },
                {
                    'match': {
                        'files.project_json.provenance.document_id': "test_uuid_2"
                    }
                }
            ])

            query = _build_dss_query(project_uuids)
            self.assertEqual(query, expected_query)

        with self.subTest("2 project uuids, 1 bundle uuid"):
            project_uuids = ["test_uuid_1", "test_uuid_2"]
            bundle_fqids = ["test_bundle_uuid.test.version"]
            expected_query = DSS_SEARCH_QUERY_TEMPLATE
            expected_query['query']['bool']['must'][0]['bool']['should'].extend([
                {
                    'match': {
                        'files.project_json.provenance.document_id': "test_uuid_1"
                    }
                },
                {
                    'match': {
                        'files.project_json.provenance.document_id': "test_uuid_2"
                    }
                },
                {
                    'bool': {
                        'must': [
                            {
                                'match': {
                                    'uuid': "test_bundle_uuid"
                                }
                            },
                            {
                                'match': {
                                    'manifest.version': "test.version"
                                }
                            }
                        ]
                    }
                }
            ])

            query = _build_dss_query(project_uuids, bundle_fqids)
            self.assertEqual(query, expected_query)

    def test_generate_metadata_schema_version_clause(self):
        project_md_schema_versions = constants.SUPPORTED_METADATA_SCHEMA_VERSIONS[MetadataSchemaName.PROJECT]
        constants.SUPPORTED_METADATA_SCHEMA_VERSIONS[MetadataSchemaName.PROJECT] = {
            'max_major': 56,
            'max_minor': 78,
            'min_major': 12,
            'min_minor': 34
        }

        metadata_schema_clause = _generate_metadata_schema_version_clause(MetadataSchemaName.PROJECT)

        expected = {
            "bool": {
                "should": [
                    {
                        "bool": {
                            "must_not": {
                                "exists": {
                                    "field": f"files.project_json.provenance.schema_major_version"
                                }
                            }
                        }
                    },
                    {
                        "bool": {
                            "must": [
                                {
                                    "term": {
                                        f"files.project_json.provenance.schema_major_version": 12
                                    }
                                },
                                {
                                    "range": {
                                        f"files.project_json.provenance.schema_minor_version": {
                                            "gte": 34
                                        }
                                    }
                                }
                            ]
                        }
                    },
                    {
                        "bool": {
                            "must": [
                                {
                                    "term": {
                                        f"files.project_json.provenance.schema_major_version": 56
                                    }
                                },
                                {
                                    "range": {
                                        f"files.project_json.provenance.schema_minor_version": {
                                            "lte": 78
                                        }
                                    }
                                }
                            ]
                        }
                    },
                    {
                        "range": {
                            f"files.project_json.provenance.schema_major_version": {
                                "gte": 13,
                                "lte": 55
                            }
                        }
                    },
                ],
                "minimum_should_match": 1
            }
        }
        self.assertEqual(metadata_schema_clause, expected)
        constants.SUPPORTED_METADATA_SCHEMA_VERSIONS[MetadataSchemaName.PROJECT] = project_md_schema_versions

    @mock.patch("matrix.common.aws.redshift_handler.RedshiftHandler.transaction")
    @mock.patch("matrix.common.query_constructor.format_str_list")
    @mock.patch("matrix.common.etl.get_dss_client")
    def test_verify_load(self, mock_get_dss_client, mock_format_str_list, mock_transaction):
        mock_get_dss_client.return_value = TestLoader.DSSClientStub()
        mock_transaction.return_value = [[1]]

        _verify_load({})

        mock_transaction.assert_called_once_with(
            queries=["SELECT COUNT(*) FROM analysis WHERE bundle_fqid IN ('test_fqid')"],
            return_results=True
        )

    class ArgsStub:
        def __init__(self,
                     max_workers,
                     state,
                     s3_upload_id,
                     project_uuids,
                     bundle_fqids):
            self.max_workers = max_workers
            self.state = state
            self.s3_upload_id = s3_upload_id
            self.project_uuids = project_uuids
            self.bundle_fqids = bundle_fqids

    class DSSClientStub:
        def __init__(self):
            self.post_search = TestLoader.DSSClientStub.SearchStub()

        class SearchStub:
            def iterate(self, es_query, replica, per_page):
                return [{'bundle_fqid': "test_fqid"}]
