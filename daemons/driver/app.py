from matrix.lambdas.daemons.driver import Driver


def driver_handler(event, context):
    # TODO: better error handling
    assert ('request_id' in event and 'feature' in event and 'fields' in event and
            'filter' in event)
    driver = Driver(event['request_id'])
    driver.run(event["filter"], event["fields"], event["feature"])
