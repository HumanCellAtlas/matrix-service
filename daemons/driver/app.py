from matrix.lambdas.daemons.driver import Driver


def driver_handler(event, context):
    # TODO: better error handling
    assert 'request_id' in event and 'bundle_fqids' in event and 'format' in event
    driver = Driver()
    driver.run(event['request_id'], event['bundle_fqids'], event['format'])
