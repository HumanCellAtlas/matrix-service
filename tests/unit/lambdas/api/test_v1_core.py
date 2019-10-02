import os
import requests
import unittest
import uuid
from unittest import mock

from matrix.common import constants
from matrix.common.constants import GenusSpecies, MatrixFormat, MatrixRequestStatus
from matrix.common.date import get_datetime_now
from matrix.common.exceptions import MatrixException
from matrix.common.aws.dynamo_handler import RequestTableField
from matrix.common.aws.lambda_handler import LambdaName
from matrix.common.aws.cloudwatch_handler import MetricName
import matrix.lambdas.api.v1.core as core


class TestCore(unittest.TestCase):

    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.create_request_table_entry")
    @mock.patch("matrix.common.aws.lambda_handler.LambdaHandler.invoke")
    @mock.patch("matrix.common.aws.cloudwatch_handler.CloudwatchHandler.put_metric_data")
    def test_post_matrix_with_just_filter_ok(self, mock_cw_put, mock_lambda_invoke, mock_dynamo_create_request):
        filter_ = {"op": ">", "field": "foo", "value": 42}
        format_ = MatrixFormat.LOOM.value

        body = {
            'filter': filter_,
            'format': format_
        }

        response = core.post_matrix(body)
        body.update({'request_id': mock.ANY})
        body.update({'fields': constants.DEFAULT_FIELDS})
        body.update({'feature': constants.DEFAULT_FEATURE})
        body.update({"genus_species": GenusSpecies.HUMAN.value})
        body.pop('format')

        mock_lambda_invoke.assert_called_once_with(LambdaName.DRIVER_V1, body)
        mock_dynamo_create_request.assert_called_once_with(mock.ANY, format_, constants.DEFAULT_FIELDS,
                                                           "gene", GenusSpecies.HUMAN)
        mock_cw_put.assert_called_once_with(metric_name=MetricName.REQUEST, metric_value=1)
        self.assertEqual(type(response[0]['request_id']), str)
        self.assertEqual(response[0]['status'], MatrixRequestStatus.IN_PROGRESS.value)
        self.assertEqual(response[1], requests.codes.accepted)

    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.create_request_table_entry")
    @mock.patch("matrix.common.aws.lambda_handler.LambdaHandler.invoke")
    @mock.patch("matrix.common.aws.cloudwatch_handler.CloudwatchHandler.put_metric_data")
    def test_post_matrix_with_species(self, mock_cw_put, mock_lambda_invoke, mock_dynamo_create_request):
        filter_ = {"op": "=",
                   "field": "specimen_from_organism.genus_species.ontology_label",
                   "value": "monkey whatever"}
        format_ = MatrixFormat.LOOM.value

        body = {
            'filter': filter_,
            'format': format_
        }

        response = core.post_matrix(body)

        body.update({'request_id': mock.ANY})
        body.update({'fields': constants.DEFAULT_FIELDS})
        body.update({'feature': constants.DEFAULT_FEATURE})
        body.pop('format')

        genera_species = list(GenusSpecies)
        self.assertEqual(mock_lambda_invoke.call_count, len(genera_species))
        self.assertEqual(mock_dynamo_create_request.call_count, len(genera_species))
        self.assertEqual(mock_cw_put.call_count, len(genera_species))

        for gs in genera_species:
            gs_body = body.copy()
            gs_body["genus_species"] = gs.value
            mock_lambda_invoke.assert_any_call(LambdaName.DRIVER_V1, gs_body)

            mock_dynamo_create_request.assert_any_call(
                mock.ANY,
                format_,
                constants.DEFAULT_FIELDS,
                constants.DEFAULT_FEATURE,
                gs)

        self.assertEqual(type(response[0]['request_id']), str)
        self.assertEqual(type(response[0]['non_human_request_ids']), dict)
        self.assertIn("Mus musculus", response[0]["non_human_request_ids"])
        self.assertEqual(response[0]['status'], MatrixRequestStatus.IN_PROGRESS.value)
        self.assertEqual(response[1], requests.codes.accepted)

    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.create_request_table_entry")
    @mock.patch("matrix.common.aws.lambda_handler.LambdaHandler.invoke")
    @mock.patch("matrix.common.aws.cloudwatch_handler.CloudwatchHandler.put_metric_data")
    def test_post_matrix_with_fields_and_feature_ok(self, mock_cw_put, mock_lambda_invoke, mock_dynamo_create_request):
        filter_ = {"op": ">", "field": "foo", "value": 42}
        format_ = MatrixFormat.LOOM.value

        body = {
            'filter': filter_,
            'format': format_,
            'fields': ["test.field1", "test.field2"],
            'feature': "transcript"
        }

        response = core.post_matrix(body)
        body.update({'request_id': mock.ANY})
        body.update({"genus_species": GenusSpecies.HUMAN.value})
        body.pop('format')

        mock_lambda_invoke.assert_called_once_with(LambdaName.DRIVER_V1, body)
        mock_dynamo_create_request.assert_called_once_with(mock.ANY,
                                                           format_,
                                                           ["test.field1", "test.field2"],
                                                           "transcript",
                                                           GenusSpecies.HUMAN)
        mock_cw_put.assert_called_once_with(metric_name=MetricName.REQUEST, metric_value=1)
        self.assertEqual(type(response[0]['request_id']), str)
        self.assertEqual(response[0]['status'], MatrixRequestStatus.IN_PROGRESS.value)
        self.assertEqual(response[1], requests.codes.accepted)

    @mock.patch("matrix.common.aws.lambda_handler.LambdaHandler.invoke")
    def test_post_matrix_with_ids_ok_and_unexpected_format(self, mock_lambda_invoke):
        bundle_fqids = ["id1", "id2"]
        format = "fake"

        body = {
            'bundle_fqids': bundle_fqids,
            'format': format
        }
        response = core.post_matrix(body)
        self.assertEqual(response[1], requests.codes.bad_request)

    @mock.patch("matrix.common.aws.lambda_handler.LambdaHandler.invoke")
    def test_post_matrix_with_ids_and_url(self, mock_lambda_invoke):
        bundle_fqids = ["id1", "id2"]
        bundle_fqids_url = "test_url"

        body = {
            'bundle_fqids': bundle_fqids,
            'bundle_fqids_url': bundle_fqids_url
        }
        response = core.post_matrix(body)

        self.assertEqual(mock_lambda_invoke.call_count, 0)
        self.assertEqual(response[1], requests.codes.bad_request)

    @mock.patch("matrix.common.aws.lambda_handler.LambdaHandler.invoke")
    def test_post_matrix_without_ids_or_url(self, mock_lambda_invoke):
        response = core.post_matrix({})

        self.assertEqual(mock_lambda_invoke.call_count, 0)
        self.assertEqual(response[1], requests.codes.bad_request)

    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.get_table_item")
    def test_get_matrix_not_found(self, mock_get_table_item):
        request_id = str(uuid.uuid4())
        mock_get_table_item.side_effect = MatrixException(status=requests.codes.not_found, title=f"Unable to find")

        response = core.get_matrix(request_id)

        self.assertEqual(response[1], 404)
        self.assertTrue(request_id in response[0]['message'])

    @mock.patch("matrix.common.request.request_tracker.RequestTracker.is_initialized")
    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.get_table_item")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.is_request_complete")
    def test_get_matrix_processing__driver_not_initialized(self, mock_is_request_complete, mock_get_table_item,
                                                           mock_initialized):
        request_id = str(uuid.uuid4())
        mock_initialized.return_value = True
        mock_get_table_item.side_effect = MatrixException(status=requests.codes.not_found, title=f"Unable to find")

        response = core.get_matrix(request_id)

        self.assertEqual(response[1], requests.codes.ok)
        self.assertEqual(response[0]['status'], MatrixRequestStatus.IN_PROGRESS.value)

    @mock.patch("matrix.common.request.request_tracker.RequestTracker.is_expired",
                new_callable=mock.PropertyMock)
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.is_initialized")
    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.get_table_item")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.is_request_complete")
    def test_get_matrix_processing__post_driver(self,
                                                mock_is_request_complete,
                                                mock_get_table_item,
                                                mock_initialized,
                                                mock_is_expired):
        request_id = str(uuid.uuid4())
        mock_initialized.return_value = True
        mock_is_request_complete.return_value = False
        mock_is_expired.return_value = False
        mock_get_table_item.return_value = {RequestTableField.ERROR_MESSAGE.value: "",
                                            RequestTableField.FORMAT.value: "test_format",
                                            RequestTableField.CREATION_DATE.value: get_datetime_now(as_string=True)}

        response = core.get_matrix(request_id)

        self.assertEqual(response[1], requests.codes.ok)
        self.assertEqual(response[0]['status'], MatrixRequestStatus.IN_PROGRESS.value)

    @mock.patch("matrix.common.aws.s3_handler.S3Handler.size")
    @mock.patch("matrix.common.aws.batch_handler.BatchHandler.get_batch_job_status")
    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.get_table_item")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.is_request_complete")
    def test_get_matrix_no_cells(self, mock_is_request_complete, mock_get_table_item,
                                 mock_batch_job_status, mock_s3_size):

        request_id = str(uuid.uuid4())
        mock_get_table_item.return_value = {
            RequestTableField.DATA_VERSION.value: 0,
            RequestTableField.ERROR_MESSAGE.value: "",
            RequestTableField.FORMAT.value: "test_format",
            RequestTableField.GENUS_SPECIES.value: GenusSpecies.HUMAN.value,
            RequestTableField.CREATION_DATE.value: get_datetime_now(as_string=True)}
        mock_batch_job_status.return_value = "SUCCEEDED"
        mock_is_request_complete.return_value = True
        mock_s3_size.return_value = 0

        response = core.get_matrix(request_id)
        self.assertEqual(response[1], requests.codes.ok)
        self.assertEqual(response[0]['status'], MatrixRequestStatus.COMPLETE.value)
        self.assertEqual(response[0]['matrix_url'], "")

    @mock.patch("matrix.common.request.request_tracker.RequestTracker.is_initialized")
    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.get_table_item")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.is_request_complete")
    def test_get_matrix_failed(self, mock_is_request_complete, mock_get_table_item, mock_initialized):
        request_id = str(uuid.uuid4())
        mock_initialized.return_value = True
        mock_is_request_complete.return_value = False
        mock_get_table_item.return_value = {RequestTableField.ERROR_MESSAGE.value: "test error",
                                            RequestTableField.FORMAT.value: "test_format"}

        response = core.get_matrix(request_id)

        self.assertEqual(response[1], requests.codes.ok)
        self.assertEqual(response[0]['status'], MatrixRequestStatus.FAILED.value)
        self.assertEqual(response[0]['message'], "test error")

    @mock.patch("matrix.common.aws.s3_handler.S3Handler.size")
    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.get_table_item")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.is_request_complete")
    def test_get_loom_matrix_complete(self, mock_is_request_complete, mock_get_table_item,
                                      mock_size):
        request_id = str(uuid.uuid4())
        mock_size.return_value = 1234
        mock_is_request_complete.return_value = True
        mock_get_table_item.return_value = {RequestTableField.DATA_VERSION.value: 0,
                                            RequestTableField.REQUEST_HASH.value: "hash",
                                            RequestTableField.ERROR_MESSAGE.value: "",
                                            RequestTableField.FORMAT.value: "loom"}

        response = core.get_matrix(request_id)
        self.assertEqual(response[1], requests.codes.ok)
        self.assertEqual(response[0]['matrix_url'],
                         f"https://s3.amazonaws.com/{os.environ['MATRIX_RESULTS_BUCKET']}/0/hash/{request_id}.loom")

        self.assertEqual(response[0]['status'], MatrixRequestStatus.COMPLETE.value)

    @mock.patch("matrix.common.aws.s3_handler.S3Handler.size")
    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.get_table_item")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.is_request_complete")
    def test_get_csv_matrix_complete(self, mock_is_request_complete, mock_get_table_item,
                                     mock_size):
        request_id = str(uuid.uuid4())
        mock_size.return_value = 1234
        mock_is_request_complete.return_value = True
        mock_get_table_item.return_value = {RequestTableField.DATA_VERSION.value: 0,
                                            RequestTableField.REQUEST_HASH.value: "hash",
                                            RequestTableField.ERROR_MESSAGE.value: "",
                                            RequestTableField.FORMAT.value: "csv"}

        response = core.get_matrix(request_id)
        self.assertEqual(response[1], requests.codes.ok)
        self.assertEqual(response[0]['matrix_url'],
                         f"https://s3.amazonaws.com/{os.environ['MATRIX_RESULTS_BUCKET']}/0/hash/{request_id}.csv.zip")

        self.assertEqual(response[0]['status'], MatrixRequestStatus.COMPLETE.value)

    @mock.patch("matrix.common.aws.s3_handler.S3Handler.size")
    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.get_table_item")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.is_request_complete")
    def test_get_mtx_matrix_complete(self, mock_is_request_complete, mock_get_table_item,
                                     mock_size):
        request_id = str(uuid.uuid4())
        mock_size.return_value = 1234
        mock_is_request_complete.return_value = True
        mock_get_table_item.return_value = {RequestTableField.DATA_VERSION.value: 0,
                                            RequestTableField.REQUEST_HASH.value: "hash",
                                            RequestTableField.ERROR_MESSAGE.value: "",
                                            RequestTableField.FORMAT.value: "mtx"}

        response = core.get_matrix(request_id)
        self.assertEqual(response[1], requests.codes.ok)
        self.assertEqual(response[0]['matrix_url'],
                         f"https://s3.amazonaws.com/{os.environ['MATRIX_RESULTS_BUCKET']}/0/hash/{request_id}.mtx.zip")

        self.assertEqual(response[0]['status'], MatrixRequestStatus.COMPLETE.value)

    def test_get_formats(self):
        response = core.get_formats()
        self.assertEqual(response[0], [item.value for item in MatrixFormat])

    @mock.patch("matrix.common.aws.batch_handler.BatchHandler.get_batch_job_status")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.is_expired",
                new_callable=mock.PropertyMock)
    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.get_table_item")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.is_request_complete")
    def test_get_matrix_expired(self, mock_is_request_complete, mock_get_table_item, mock_is_expired,
                                mock_batch_job_status):
        request_id = str(uuid.uuid4())
        mock_is_request_complete.return_value = False
        mock_get_table_item.return_value = {
            RequestTableField.ERROR_MESSAGE.value: "",
            RequestTableField.FORMAT.value: "test_format"}
        mock_batch_job_status.return_value = "SUCCEEDED"
        mock_is_expired.return_value = True

        response = core.get_matrix(request_id)
        self.assertEqual(response[1], requests.codes.ok)
        self.assertEqual(response[0]['status'], MatrixRequestStatus.EXPIRED.value)

    @mock.patch("matrix.common.aws.s3_handler.S3Handler.size")
    @mock.patch("matrix.common.aws.batch_handler.BatchHandler.get_batch_job_status")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.is_expired",
                new_callable=mock.PropertyMock)
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.timeout",
                new_callable=mock.PropertyMock)
    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.get_table_item")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.is_request_complete")
    def test_get_matrix_timeout(self, mock_is_request_complete, mock_get_table_item,
                                mock_timeout, mock_is_expired, mock_batch_job_status,
                                mock_s3_size):
        request_id = str(uuid.uuid4())
        mock_is_request_complete.return_value = False
        mock_s3_size.return_value = 123
        mock_batch_job_status.return_value = "SUCCEEDED"
        mock_is_expired.return_value = False
        mock_get_table_item.return_value = {
            RequestTableField.DATA_VERSION.value: 0,
            RequestTableField.ERROR_MESSAGE.value: "",
            RequestTableField.FORMAT.value: "test_format"}
        mock_timeout.return_value = True

        response = core.get_matrix(request_id)
        self.assertEqual(response[1], requests.codes.ok)
        self.assertEqual(response[0]['status'], MatrixRequestStatus.FAILED.value)

    @mock.patch("matrix.common.request.request_tracker.RequestTracker.log_error")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.batch_job_status",
                new_callable=mock.PropertyMock)
    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.get_table_item")
    def test_get_matrix_batch_failure(self, mock_get_table_item, mock_batch_job_status, mock_log_error):
        request_id = str(uuid.uuid4())
        mock_get_table_item.return_value = {RequestTableField.ERROR_MESSAGE.value: "",
                                            RequestTableField.FORMAT.value: "test_format"}
        mock_batch_job_status.return_value = "FAILED"

        response = core.get_matrix(request_id)
        self.assertEqual(response[1], requests.codes.ok)
        self.assertEqual(response[0]['status'], MatrixRequestStatus.FAILED.value)

    def test_get_filters(self):

        response = core.get_filters()

        self.assertEqual(response[1], requests.codes.ok)
        self.assertListEqual(response[0], list(constants.FILTER_DETAIL.keys()))

    def test_get_fields(self):

        response = core.get_fields()

        self.assertEqual(response[1], requests.codes.ok)
        self.assertListEqual(response[0], list(constants.FIELD_DETAIL.keys()))

    def test_get_features(self):

        response = core.get_features()

        self.assertEqual(response[1], requests.codes.ok)
        self.assertListEqual(response[0], list(constants.FEATURE_DETAIL.keys()))

    @mock.patch("matrix.common.aws.redshift_handler.RedshiftHandler.transaction")
    def test_get_filter_detail(self, mock_transaction):

        response = core.get_filter_detail("not.a.real.filter.")
        self.assertEqual(response[1], requests.codes.not_found)

        filter_ = 'donor_organism.human_specific.ethnicity.ontology'
        description = constants.FILTER_DETAIL[filter_]

        mock_transaction.return_value = [("abc", 123), ("def", 456), (None, 789)]

        response = core.get_filter_detail(filter_)

        self.assertEqual(response[1], requests.codes.ok)
        self.assertDictEqual(
            response[0],
            {
                "field_name": filter_,
                "field_description": description,
                "field_type": "categorical",
                "cell_counts": {"abc": 123, "def": 456, "": 789}})

        filter_ = 'genes_detected'
        description = constants.FILTER_DETAIL[filter_]
        mock_transaction.return_value = [(10, 100)]
        response = core.get_filter_detail(filter_)

        self.assertEqual(response[1], requests.codes.ok)
        self.assertDictEqual(
            response[0],
            {
                "field_name": filter_,
                "field_description": description,
                "field_type": "numeric",
                "minimum": 10,
                "maximum": 100})

    @mock.patch("matrix.common.aws.redshift_handler.RedshiftHandler.transaction")
    def test_get_filter_detail_boolean(self, mock_transaction):
        filter_ = 'emptydrops_is_cell'
        description = constants.FILTER_DETAIL[filter_]

        mock_transaction.return_value = [(True, 123), (False, 456), (None, 789)]

        response = core.get_filter_detail(filter_)

        self.assertEqual(response[1], requests.codes.ok)
        self.assertDictEqual(
            response[0],
            {
                "field_name": filter_,
                "field_description": description,
                "field_type": "categorical",
                "cell_counts": {"True": 123, "False": 456, "": 789}})

    @mock.patch("matrix.common.aws.redshift_handler.RedshiftHandler.transaction")
    def test_get_field(self, mock_transaction):
        response = core.get_field_detail("not.a.real.field.")
        self.assertEqual(response[1], requests.codes.not_found)

        field = 'donor_organism.human_specific.ethnicity.ontology'
        description = constants.FIELD_DETAIL[field]

        mock_transaction.return_value = [("abc", 123), ("def", 456)]

        response = core.get_field_detail(field)

        self.assertEqual(response[1], requests.codes.ok)
        self.assertDictEqual(
            response[0],
            {
                "field_name": field,
                "field_description": description,
                "field_type": "categorical",
                "cell_counts": {"abc": 123, "def": 456}})

        field = 'genes_detected'
        description = constants.FIELD_DETAIL[field]
        mock_transaction.return_value = [(10, 100)]
        response = core.get_field_detail(field)

        self.assertEqual(response[1], requests.codes.ok)
        self.assertDictEqual(
            response[0],
            {
                "field_name": field,
                "field_description": description,
                "field_type": "numeric",
                "minimum": 10,
                "maximum": 100})

    def test_get_feature_detail(self):

        response = core.get_feature_detail("gene")

        self.assertEqual(response[1], requests.codes.ok)
        self.assertDictEqual(
            response[0],
            {
                "feature": "gene",
                "feature_description": constants.FEATURE_DETAIL["gene"]})

        response = core.get_feature_detail("not.a.feature")
        self.assertEqual(response[1], requests.codes.not_found)

    def test_get_format_detail(self):

        response = core.get_format_detail("loom")

        self.assertEqual(response[1], requests.codes.ok)
        self.assertEqual(response[0], constants.FORMAT_DETAIL["loom"])

        response = core.get_format_detail("not.a.format")
        self.assertEqual(response[1], requests.codes.not_found)
