from matrix.lambdas.daemons.notification import NotificationHandler


def notification_handler(event, context):
    assert ('bundle_uuid' in event and 'bundle_version' in event and 'event_type' in event)

    notification_handler = NotificationHandler(event['bundle_uuid'], event['bundle_version'], event['event_type'])
    notification_handler.run()
