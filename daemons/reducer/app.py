from matrix.lambdas.daemons.reducer import Reducer


def reducer_handler(event, context):
    # TODO: better error handling
    assert "request_hash" in event
    Reducer(event['request_hash']).run()
