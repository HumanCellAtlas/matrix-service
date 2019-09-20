import collections
import logging
import os
import re
import requests
import sys

import boto3
import chalice
import connexion
from requests_http_signature import HTTPSignatureAuth

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "chalicelib"))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from matrix.common.aws.redshift_handler import RedshiftHandler
from matrix.common.aws.sqs_handler import SQSHandler
from matrix.common.config import MatrixInfraConfig

ecs_client = boto3.client("ecs", region_name=os.environ['AWS_DEFAULT_REGION'])
redshift_handler = RedshiftHandler()


def create_app():
    app = connexion.App("matrix-service-api")
    swagger_spec_path = os.path.join(pkg_root, "config", "matrix-api.yml")
    app.add_api(swagger_spec_path, validate_responses=True)
    return app


def get_chalice_app(flask_app):
    app = chalice.Chalice(app_name=flask_app.name)
    flask_app.debug = True
    app.debug = flask_app.debug
    app.log.setLevel(logging.DEBUG)

    def dispatch(*args, **kwargs):
        uri_params = app.current_request.uri_params or {}
        path = app.current_request.context["resourcePath"].format(**uri_params)
        req_body = app.current_request.raw_body if app.current_request._body is not None else None
        with flask_app.test_request_context(path=path,
                                            base_url="https://{}".format(app.current_request.headers["host"]),
                                            query_string=app.current_request.query_params,
                                            method=app.current_request.method,
                                            headers=list(app.current_request.headers.items()),
                                            data=req_body,
                                            environ_base=app.current_request.stage_vars):
            flask_res = flask_app.full_dispatch_request()
        res_headers = dict(flask_res.headers)
        # API Gateway/Cloudfront adds a duplicate Content-Length with a different value (not sure why)
        res_headers.pop("Content-Length", None)
        return chalice.Response(status_code=flask_res._status_code,
                                headers=res_headers,
                                body="".join([c.decode() if isinstance(c, bytes) else c for c in flask_res.response]))

    routes = collections.defaultdict(list)
    for rule in flask_app.url_map.iter_rules():
        routes[re.sub(r"<(.+?)(:.+?)?>", r"{\1}", rule.rule).rstrip("/")] += rule.methods
    for route, methods in routes.items():
        app.route(route, methods=list(set(methods) - {"OPTIONS"}), cors=True)(dispatch)

    with open(os.path.join(pkg_root, "index.html")) as fh:
        swagger_ui_html = fh.read()

    @app.route("/")
    def serve_swagger_ui():
        return chalice.Response(status_code=200,
                                headers={"Content-Type": "text/html"},
                                body=swagger_ui_html)

    @app.route("/version")
    def version():
        data = {
            'version_info': {
                'version': os.getenv('MATRIX_VERSION')
            }
        }

        return chalice.Response(status_code=200,
                                headers={'Content-Type': "application/json"},
                                body=data)

    @app.route("/internal/health")
    def health():
        # Level 2 healthcheck: Test connection can be made to redshift cluster but do not run any queries
        redshift_handler.transaction([])
        # Level 2 healthcheck checks that ecs query runner is active with expected number of tasks
        service_name = f"matrix-service-query-runner-{os.environ['DEPLOYMENT_STAGE']}"
        service = ecs_client.describe_services(cluster=service_name, services=[service_name])["services"][0]
        status = service["status"]
        running_task_count = service["runningCount"]
        assert status == 'ACTIVE'
        assert running_task_count > 0
        return chalice.Response(status_code=200,
                                headers={'Content-Type': "text/html"},
                                body="OK")

    @app.route("/dss/notification", methods=['POST'])
    def dss_notification():
        body = app.current_request.json_body
        bundle_uuid = body['match']['bundle_uuid']
        bundle_version = body['match']['bundle_version']
        subscription_id = body['subscription_id']
        event_type = body['event_type']

        config = MatrixInfraConfig()
        HTTPSignatureAuth.verify(requests.Request(url="http://host/dss/notification",
                                                  method=app.current_request.method,
                                                  headers=app.current_request.headers),
                                 key_resolver=lambda key_id, alg: config.dss_subscription_hmac_secret_key.encode())

        payload = {
            'bundle_uuid': bundle_uuid,
            'bundle_version': bundle_version,
            'event_type': event_type,
        }
        queue_url = config.notification_q_url
        SQSHandler().add_message_to_queue(queue_url, payload)

        return chalice.Response(status_code=requests.codes.ok,
                                body=f"Received notification from subscription {subscription_id}: "
                                     f"{event_type} {bundle_uuid}.{bundle_version}")

    return app


app = get_chalice_app(create_app().app)
