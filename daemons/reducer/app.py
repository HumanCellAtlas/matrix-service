from matrix.lambdas.daemons.reducer import Reducer


def reducer_handler(event, context):
    # TODO: better error handling
    assert "request_id" in event and "request_hash" in event
    Reducer(event['request_id'], event['request_hash']).run()
