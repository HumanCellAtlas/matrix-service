from matrix.lambdas.daemons.mapper import Mapper


def mapper_handler(event, context):
    # TODO: better error handling
    assert "request_id" in event and "bundle_uuid" in event and "bundle_version" in event
    mapper = Mapper(event['request_id'])
    mapper.run(event['bundle_uuid'], event['bundle_version'])
