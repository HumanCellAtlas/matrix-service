from matrix.lambdas.daemons.worker import Worker


def worker_handler(event, context):
    assert 'request_id' in event and 'work_chunk_spec' in event and 'format' in event and 'filter_string' in event

    work_chunk_spec = event['work_chunk_spec']
    assert 'bundle_uuid' in work_chunk_spec and 'bundle_version' in work_chunk_spec
    assert 'start_row' in work_chunk_spec and 'num_rows' in work_chunk_spec

    worker = Worker()
    worker.run(event['request_id'], event['filter_string'], work_chunk_spec)
