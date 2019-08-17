from matrix.lambdas.daemons.v1.driver import Driver


def driver_handler(event, context):
    assert ('request_id' in event and 'feature' in event and 'fields' in event
            and 'filter' in event)
    driver = Driver(event['request_id'])
    driver.run(event["filter"], event["fields"], event["feature"])
