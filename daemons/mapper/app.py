from matrix.lambdas.daemons.mapper import Mapper


def mapper_handler(event, context):
    # TODO: better error handling
    assert ("request_id" in event and
            "bundle_uuid" in event and
            "bundle_version" in event and
            "filter_string" in event and
            "format" in event)

    mapper = Mapper(event['request_id'], event['format'], event['filter_string'])
    mapper.run(event['bundle_uuid'], event['bundle_version'])
