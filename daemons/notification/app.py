import json

from matrix.lambdas.daemons.notification import NotificationHandler


def notification_handler(event, context):
    notification = json.loads(event["Records"][0]["body"])
    assert ('bundle_uuid' in notification and 'bundle_version' in notification and 'event_type' in notification)

    bundle_uuid = notification["bundle_uuid"]
    bundle_version = notification["bundle_version"]
    event_type = notification["event_type"]

    notification_handler = NotificationHandler(bundle_uuid, bundle_version, event_type)
    notification_handler.run()
