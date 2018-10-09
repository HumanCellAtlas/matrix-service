from matrix.lambdas.daemons.mapper import Mapper


def mapper_handler(event, context):
    # TODO: better error handling
    assert "request_id" in event and "bundle_fqid" in event and "format" in event
    Mapper().run(event['request_id'], event['bundle_fqid'], event['format'])
