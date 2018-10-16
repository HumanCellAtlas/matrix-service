from matrix.lambdas.daemons.worker import Worker


def worker_handler(event: dict, context: dict):
    """
    Lambda worker handler invoked by mapper lambda

    Input:
        event: (dict) keys expected 'request_id': str, 'worker_chunk_spec': dict, 'format': str
            worker_chunk_spec: (dict) keys expected 'start_row', 'num_rows',
                               'bundle_uuid', 'bundle_version'
        context: (dict) lambda context object passed in by AWS
    """
    print(f"worker invoked with {event}")
    assert 'request_id' in event and 'worker_chunk_spec' in event and 'format' in event

    worker_chunk_spec = event['worker_chunk_spec']
    assert 'bundle_uuid' in worker_chunk_spec and 'bundle_version' in worker_chunk_spec
    assert 'start_row' in worker_chunk_spec and 'num_rows' in worker_chunk_spec

    worker = Worker(event['request_id'])
    worker.run(event['format'], event['worker_chunk_spec'])
