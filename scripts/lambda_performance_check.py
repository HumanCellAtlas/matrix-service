import datetime
import json
import multiprocessing
import random
import tempfile
import time
import boto3
import requests

from typing import List
from config import BUNDLE_UUIDS_PATH
from scripts.lambda_log_client import get_lambda_status

matrix_service_url = "https://brp95mk7ig.execute-api.us-east-1.amazonaws.com/dev/matrices/concat"
log_group = "/aws/lambda/matrix-service-sqs-listener"
out_dir = "../data/v1.0.0"


def get_random_existing_bundle_uuids(uuids_num: int) -> List[str]:
    """
    Get a random subset of existing bundle uuids.
    :param uuids_num: Number of bundle uuids to get.
    :return: A list of bundle uuids.
    """
    with open(BUNDLE_UUIDS_PATH, "r") as f:
        sample_bundle_uuids = json.loads(f.read())

    bundle_uuids_subset = random.sample(sample_bundle_uuids, uuids_num)

    return bundle_uuids_subset


def wait_job_to_complete(request_id: str) -> None:
    """
    Wait for a matrices-concatenation request to complete.
    :param request_id: Request id.
    :return: None.
    """
    for _ in range(10):
        r = requests.get(matrix_service_url + "/" + request_id)
        if r.status_code == 200 and r.json()["status"] == "DONE":
            return
        time.sleep(30)


def measure_lambda_duration(memory_size: int, cell_num: int, request_num: int, wait_time: int):
    """
    Measure average duration for the lambda function on concatenating n 23465 * 1 dimensional matrices.
    Default matrix size: 23465 * 1, which means one matrix corresponds to one cell.
    :param memory_size: Lambda memory size.
    :param cell_num: Number of cells to concatenate.
    :param request_num: Number of concatenation requests to make.
    :param wait_time: Time to wait between sending each request.
    """
    request_ids = []
    jobs = []
    start_time = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

    print("Fetching matrices concatenation requests on {} matrices with {}MB lambda machine......".format(cell_num, memory_size))

    # Fetch matrices concatenation requests
    for i in range(request_num):
        print("Send {} request.".format(i + 1))
        data = get_random_existing_bundle_uuids(cell_num)
        r = requests.post(matrix_service_url, json=data)
        if r.status_code != 200:
            continue
        request_ids.append(r.json()["request_id"])
        time.sleep(wait_time)

    print("Done!")

    # Poll for concatenation job until complete
    for request_id in request_ids:
        print("Wait for request {} to complete......".format(request_id))
        job = multiprocessing.Process(target=wait_job_to_complete, args=(request_id, ))
        jobs.append(job)
        job.start()

    for job in jobs:
        job.join()

    print("Finished!")

    end_time = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

    results = {
        "memory": memory_size,
        "cell_num": cell_num,
        "start_time": start_time,
        "end_time": end_time
    }

    print("Saving time information......")

    _, path = tempfile.mkstemp(dir=out_dir, suffix=".json", prefix="time_info_memory_{}_cell_{}_"
                               .format(memory_size, cell_num))

    with open(path, 'w') as f:
        json.dump(results, f)

    print("Done")

    return start_time, end_time


def get_lamb_info(memory_size):
    """
    Get lambda running info on a particular memory size lambda instance.
    :param memory_size: Lambda memory size.
    """
    for cell_num in range(100, 3000, 50):
        start_time, end_time = measure_lambda_duration(
            memory_size=memory_size,
            cell_num=cell_num,
            request_num=20,
            wait_time=60
        )
        try:
            lambda_info = get_lambda_status(log_group=log_group, start_time=start_time, end_time=end_time)
        except Exception:
            continue

        _, path = tempfile.mkstemp(dir=out_dir, suffix=".json", prefix="lambda_info_memory_{}_cell_{}_"
                                   .format(memory_size, cell_num))

        result = {
            "memory": memory_size,
            "cell": cell_num,
            "invocation": len(lambda_info),
            "info": lambda_info
        }

        with open(path, 'w') as f:
            json.dump(result, f)

        error_count = 0

        for info in lambda_info:
            if info['status'] is not None:
                error_count += 1

        # Find a upper limit on the current memory size
        if float(error_count) / float(len(lambda_info)) >= 0.8:
            return

        time.sleep(60)


if __name__ == '__main__':
    memory_sizes = [128, 256, 512, 1024, 2048, 3008]
    lambda_client = boto3.client('lambda')

    for memory_size in memory_sizes:
        response = lambda_client.update_function_configuration(
            FunctionName="matrix-service-sqs-listener",
            MemorySize=memory_size
        )
        assert response['MemorySize'] == memory_size
        get_lamb_info(memory_size)
        time.sleep(200)
