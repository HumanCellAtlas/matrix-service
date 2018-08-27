import datetime
import json
import os
import random
import uuid

import boto3

REQUEST_TABLE = boto3.resource("dynamodb").Table(os.environ["REQUEST_TABLE"])


def matrix_post(event, context):

    body = json.loads(event["body"])
    bundle_ids = body.get("bundle_fqids")
    bundle_ids_url = body.get("bundle_fqids_url")

    if not (bundle_ids or bundle_ids_url):
        return {
            "statusCode": "400",
            "body": json.dumps({"msg": "must specify bundle_fqids or bundle_fqids_url"})
        }

    request_id = str(uuid.uuid4())
    finish_time = datetime.datetime.utcnow() + datetime.timedelta(seconds=random.randrange(8, 100))

    REQUEST_TABLE.put_item(
        Item={
            "RequestId": request_id,
            "FinishedBy": finish_time.isoformat()
        }
    )

    status_url = ("https://" + event["headers"]["Host"] +
                  event["requestContext"]["path"] + "/" + request_id)

    return {"statusCode": "202", # Accepted
            "body": json.dumps(
                {"request_id": request_id,
                 "status": "In Progress",
                 "key": "",
                 "eta": random.randrange(1, 100000),
                 "links": [{
                     "rel": "status",
                     "href": status_url,
                     "method": "get"
                     }]
                }
            )}

def matrix_get(event, context):

    request_id = event["pathParameters"]["request_id"]

    response = REQUEST_TABLE.get_item(Key={"RequestId": request_id})

    if "Item" not in response:
        return {"statusCode": "404",
                "body": json.dumps({"msg": "request_id not found"})}

    finished_time = datetime.datetime.strptime(
        response["Item"]["FinishedBy"], "%Y-%m-%dT%H:%M:%S.%f")

    status_url = ("https://" + event["headers"]["Host"] +
                  event["requestContext"]["path"])
    links = [{"rel": "self", "href": status_url, "method": "get"}]

    if datetime.datetime.utcnow() > finished_time:
        succeeded = (hash(request_id) % 5) != 0
        if succeeded:
            response_body = {
                "request_id": request_id,
                "status": "Complete",
                "key": "download_me",
                "message": "",
                "eta": 0,
                "links": links
            }
        else:
            response_body = {
                "request_id": request_id,
                "status": "Failed",
                "key": "",
                "message": "Forgiveness is the attribute of the strong.",
                "eta": 0,
                "links": links
            }
    else:
        response_body = {
            "request_id": request_id,
            "status": "In Progress",
            "key": "",
            "message": "",
            "eta": random.randrange(1, 100000),
            "links": links
        }

    return {"statusCode": "200",
            "body": json.dumps(response_body)}
