import argparse
import os
import sys

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from matrix.common.aws.dynamo_handler import DynamoHandler, DynamoTable, DeploymentTableField, RequestTableField
from matrix.common.aws.s3_handler import S3Handler
from matrix.common.request.request_tracker import RequestTracker


def invalidate_cache_entries(request_ids: list,
                             request_hashes: list):
    """
    Invalidates a list of request IDs and/or request hashes.
    Invalidation refers to the invalidation of the request in DynamoDB
    and the deletion of the associated matrix in S3.

    Invalidated requests will return an `ERROR` state and explanation
    to the user via the GET endpoint.

    Request hashes are resolved to a list of associated request IDs.
    :param request_ids: list of request IDs to invalidate
    :param request_hashes: list of request hashes to invalidate
    """
    print(f"Invalidating request IDs: {request_ids}")
    print(f"Invalidating request hashes: {request_hashes}")
    deployment_stage = os.environ['DEPLOYMENT_STAGE']
    dynamo_handler = DynamoHandler()
    s3_results_bucket_handler = S3Handler(os.environ['MATRIX_RESULTS_BUCKET'])
    data_version = dynamo_handler.get_table_item(table=DynamoTable.DEPLOYMENT_TABLE,
                                                 key=deployment_stage)[DeploymentTableField.CURRENT_DATA_VERSION.value]
    for request_hash in request_hashes:
        items = dynamo_handler.filter_table_items(table=DynamoTable.REQUEST_TABLE,
                                                  attrs={
                                                      RequestTableField.REQUEST_HASH.value: request_hash,
                                                      RequestTableField.DATA_VERSION.value: data_version,
                                                      RequestTableField.ERROR_MESSAGE.value: 0
                                                  })
        for item in items:
            request_ids.append(item[RequestTableField.REQUEST_ID.value])

    s3_keys_to_delete = []
    for request_id in request_ids:
        print(f"Writing deletion error to {request_id} in DynamoDB.")
        request_tracker = RequestTracker(request_id=request_id)
        request_tracker.log_error("This request has been deleted and is no longer available for download. "
                                  "Please generate a new matrix at POST /v1/matrix.")
        s3_keys_to_delete.append(request_tracker.s3_results_key)

    print(f"Deleting matrices at the following S3 keys: {s3_keys_to_delete}")
    if s3_keys_to_delete:
        deleted_objects = s3_results_bucket_handler.delete_objects(s3_keys_to_delete)

        deleted_keys = [deleted_object['Key'] for deleted_object in deleted_objects]

        print(f"Successfully deleted the following matrices {deleted_keys}."
              f"({len(deleted_keys)}/{len(s3_keys_to_delete)})")


if __name__ == "__main__":  # pragma: no cover
    parser = argparse.ArgumentParser()
    parser.add_argument("--request-ids",
                        help="List of request IDs to redact.",
                        nargs="*",
                        type=str,
                        default=[])
    parser.add_argument("--request-hashes",
                        help="List of request hashes to redact.",
                        nargs="*",
                        type=str,
                        default=[])
    args = parser.parse_args()

    invalidate_cache_entries(request_ids=args.request_ids,
                             request_hashes=args.request_hashes)
