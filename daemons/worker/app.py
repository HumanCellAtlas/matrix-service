from matrix.lambdas.daemons.worker import Worker


def worker_handler(event, context):
    assert 'request_id' in event and 'worker_chunk_spec' in event and 'format' in event and 'filter_string' in event

    worker_chunk_spec = event['worker_chunk_spec']
    assert 'bundle_uuid' in worker_chunk_spec and 'bundle_version' in worker_chunk_spec
    assert 'start_row' in worker_chunk_spec and 'num_rows' in worker_chunk_spec

    worker = Worker()
    worker.run(event['request_id'], event['filter_string'], worker_chunk_spec)
