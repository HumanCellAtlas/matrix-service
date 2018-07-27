#!/usr/bin/env python3
import json
import sys
import tempfile
import boto3

from datetime import datetime

client = boto3.client('logs')


def iso8601_to_epoch(s):
    utc_dt = datetime.strptime(s, '%Y-%m-%dT%H:%M:%SZ')
    timestamp = (utc_dt - datetime(1970, 1, 1)).total_seconds()
    return int(timestamp * 1000)


def make_query_func(client, log_group, start_time, end_time):
    def inner(filter_pattern, subgrep=None):
        messages = []

        def parse_response(response):
            response_messages = [e['message'] for e in response['events']]
            if subgrep is not None:
                response_messages = [m.split("\n") for m in response_messages]
                response_messages = [item for sublist in response_messages for item in sublist]  # flatten
                response_messages = [m for m in response_messages if subgrep in m]
            return response_messages, response['nextToken'] if 'nextToken' in response else None

        response = client.filter_log_events(
            logGroupName=log_group,
            startTime=start_time,
            endTime=end_time,
            filterPattern=filter_pattern
        )
        new_messages, token = parse_response(response)
        messages += new_messages
        while token is not None:
            response = client.filter_log_events(
                logGroupName=log_group,
                nextToken=token
            )
            new_messages, token = parse_response(response)
            messages += new_messages
        return messages

    return inner


def get_lambda_status(log_group, start_time, end_time):
    start_time = iso8601_to_epoch(start_time)
    end_time = iso8601_to_epoch(end_time)

    query = make_query_func(client, log_group, start_time, end_time)

    error_uuids = set([m.split()[2] for m in query('ERROR', subgrep='ERROR')])
    timeout_uuids = set([m.split()[1] for m in query('Task timed out')])
    durations = [m.split() for m in query('[report, request_id_label, request_id, duration=Duration*, num, ...]')]

    def maybe_convert(ary, field, convert):
        try:
            return convert(ary[field])
        except Exception as e:
            print("conversion failed for input: " + ' '.join(ary))
            print(str(e))
            return None

    durations = [(m[2], maybe_convert(m, 4, lambda s: float(s))) for m in durations]
    durations = [t for t in durations if t[1] is not None]

    results = []

    for uuid, duration in durations:
        results += [dict(
            request_uuid=uuid,
            duration_ms=duration,
            status='error' if uuid in error_uuids else ('timeout' if uuid in timeout_uuids else None)
        )]

    return results


if __name__ == '__main__':
    file_ = sys.argv[1]

    with open(file_, 'r') as f:
        js = json.load(f)

    lambda_info = get_lambda_status("/aws/lambda/matrix-service-sqs-listener", js['start_time'], js['end_time'])
    _, path = tempfile.mkstemp(dir='../data', suffix=".json", prefix="lambda_info_memory_{}_cell_{}_"
                               .format(js['memory'], js['cell_num']))

    result = {
        "memory": js['memory'],
        "cell": js['cell_num'],
        "invocation": len(lambda_info),
        "info": lambda_info
    }

    with open(path, 'w') as f:
        json.dump(result, f)
