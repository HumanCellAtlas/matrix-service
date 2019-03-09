from matrix.lambdas.daemons.notifications import NotificationsHandler


def notifications_handler(event, context):
    assert ('bundle_uuid' in event and 'bundle_version' in event and 'event_type' in event)

    notifications = NotificationsHandler(event['bundle_uuid'], event['bundle_version'], event['event_type'])
    notifications.run()
