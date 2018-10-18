#!/usr/bin/env python
"""
This script fetches the latest desired vcpus for a batch compute environment
"""

import json
import sys
import boto3

batch_client = boto3.client('batch', region_name='us-east-1')


def handler(args):
    output = {'desired_vcpus': '0'}
    compute_environment_name = args['compute_environment_name']
    compute_environments = batch_client.describe_compute_environments()['computeEnvironments']

    for comp_env in compute_environments:
        if comp_env['computeEnvironmentName'] == compute_environment_name:
            desired_vcpus = str(comp_env['computeResources']['desiredvCpus'])
            output = {'desired_vcpus': desired_vcpus}

    json.dump(output, sys.stdout)
    exit(0)

if __name__ == '__main__':
    args = json.load(sys.stdin)
    handler(args)
