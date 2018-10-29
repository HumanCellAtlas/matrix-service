from matrix.lambdas.daemons.driver import Driver


def driver_handler(event, context):
    # TODO: better error handling
    assert ('request_id' in event and 'format' in event and 'bundle_fqids' in event and
            'bundle_fqids_url' in event)
    assert bool(event["bundle_fqids"]) != bool(event["bundle_fqids_url"])  # xor these
    driver = Driver(event['request_id'])
    driver.run(event['bundle_fqids'], event["bundle_fqids_url"], event['format'])
