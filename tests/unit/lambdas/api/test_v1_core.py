import os
import requests
import unittest
import uuid
from unittest import mock

from matrix.common import constants, query_constructor
from matrix.common.constants import MatrixFormat, MatrixRequestStatus
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
        body.update({'fields': query_constructor.DEFAULT_FIELDS})
        body.update({'feature': query_constructor.DEFAULT_FEATURE})
        body.pop('format')

        mock_lambda_invoke.assert_called_once_with(LambdaName.DRIVER_V1, body)
        mock_dynamo_create_request.assert_called_once_with(mock.ANY, format_)
        mock_cw_put.assert_called_once_with(metric_name=MetricName.REQUEST, metric_value=1)
        self.assertEqual(type(response[0]['request_id']), str)
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
        body.pop('format')

        mock_lambda_invoke.assert_called_once_with(LambdaName.DRIVER_V1, body)
        mock_dynamo_create_request.assert_called_once_with(mock.ANY, format_)
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

    @mock.patch("matrix.common.request.request_tracker.RequestTracker.is_initialized")
    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.get_table_item")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.is_request_complete")
    def test_get_matrix_processing__post_driver(self, mock_is_request_complete, mock_get_table_item, mock_initialized):
        request_id = str(uuid.uuid4())
        mock_initialized.return_value = True
        mock_is_request_complete.return_value = False
        mock_get_table_item.return_value = {RequestTableField.ERROR_MESSAGE.value: "",
                                            RequestTableField.FORMAT.value: "test_format",
                                            RequestTableField.CREATION_DATE.value: get_datetime_now(as_string=True)}

        response = core.get_matrix(request_id)

        self.assertEqual(response[1], requests.codes.ok)
        self.assertEqual(response[0]['status'], MatrixRequestStatus.IN_PROGRESS.value)

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

    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.get_table_item")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.is_request_complete")
    def test_get_loom_matrix_complete(self, mock_is_request_complete, mock_get_table_item):
        request_id = str(uuid.uuid4())
        mock_is_request_complete.return_value = True
        mock_get_table_item.return_value = {RequestTableField.ERROR_MESSAGE.value: "",
                                            RequestTableField.FORMAT.value: "loom"}

        response = core.get_matrix(request_id)
        self.assertEqual(response[1], requests.codes.ok)
        self.assertEqual(response[0]['matrix_url'],
                         f"https://s3.amazonaws.com/{os.environ['MATRIX_QUERY_RESULTS_BUCKET']}/{request_id}.loom.zip")

        self.assertEqual(response[0]['status'], MatrixRequestStatus.COMPLETE.value)

    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.get_table_item")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.is_request_complete")
    def test_get_csv_matrix_complete(self, mock_is_request_complete, mock_get_table_item):
        request_id = str(uuid.uuid4())
        mock_is_request_complete.return_value = True
        mock_get_table_item.return_value = {RequestTableField.ERROR_MESSAGE.value: "",
                                            RequestTableField.FORMAT.value: "csv"}

        response = core.get_matrix(request_id)
        self.assertEqual(response[1], requests.codes.ok)
        self.assertEqual(response[0]['matrix_url'],
                         f"https://s3.amazonaws.com/{os.environ['MATRIX_QUERY_RESULTS_BUCKET']}/{request_id}.csv.zip")

        self.assertEqual(response[0]['status'], MatrixRequestStatus.COMPLETE.value)

    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.get_table_item")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.is_request_complete")
    def test_get_mtx_matrix_complete(self, mock_is_request_complete, mock_get_table_item):
        request_id = str(uuid.uuid4())
        mock_is_request_complete.return_value = True
        mock_get_table_item.return_value = {RequestTableField.ERROR_MESSAGE.value: "",
                                            RequestTableField.FORMAT.value: "mtx"}

        response = core.get_matrix(request_id)
        self.assertEqual(response[1], requests.codes.ok)
        self.assertEqual(response[0]['matrix_url'],
                         f"https://s3.amazonaws.com/{os.environ['MATRIX_QUERY_RESULTS_BUCKET']}/{request_id}.mtx.zip")

        self.assertEqual(response[0]['status'], MatrixRequestStatus.COMPLETE.value)

    def test_get_formats(self):
        response = core.get_formats()
        self.assertEqual(response[0], [item.value for item in MatrixFormat])

    @mock.patch("matrix.common.aws.sqs_handler.SQSHandler.add_message_to_queue")
    def test_dss_notification(self, mock_sqs_add):
        core.matrix_infra_config.set({'notification_q_url': "notification_q_url"})
        body = {
            'subscription_id': "test_sub_id",
            'event_type': "test_event",
            'match': {
                'bundle_uuid': "test_id",
                'bundle_version': "test_version"
            }
        }
        expected_payload = {
            'bundle_uuid': "test_id",
            'bundle_version': "test_version",
            'event_type': 'test_event'
        }

        resp = core.dss_notification(body)
        mock_sqs_add.assert_called_once_with("notification_q_url", expected_payload)

        self.assertEqual(resp.status_code, requests.codes.ok)

    @mock.patch("matrix.common.request.request_tracker.RequestTracker.timeout",
                new_callable=mock.PropertyMock)
    @mock.patch("matrix.common.aws.dynamo_handler.DynamoHandler.get_table_item")
    @mock.patch("matrix.common.request.request_tracker.RequestTracker.is_request_complete")
    def test_get_matrix_timeout(self, mock_is_request_complete, mock_get_table_item,
                                mock_timeout):
        request_id = str(uuid.uuid4())
        mock_is_request_complete.return_value = False
        mock_get_table_item.return_value = {RequestTableField.ERROR_MESSAGE.value: "",
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
