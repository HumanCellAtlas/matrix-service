import datetime
import json
import multiprocessing
import random
import subprocess
import tempfile
import time
import requests

from typing import List
from config import GET_LAMBDA_DURATION_SCRIPT_PATH, BUNDLE_UUIDS_PATH, DATA_DIR

matrix_service_url = "https://6gbgppoyi5.execute-api.us-east-1.amazonaws.com/dev/matrices/concat"
log_group = "/aws/lambda/matrix-service-sqs-listener"


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
    for _ in range(15):
        r = requests.get(matrix_service_url + "/" + request_id)
        if r.status_code != 200:
            raise Exception("Request {}: {}".format(request_id, r.text))
        if r.json()["status"] == "DONE":
            return
        time.sleep(30)
    raise Exception("Request {}: timeout".format(request_id))


def measure_lambda_duration(memory_size: int, n: int, request_num: int, wait_time: int) -> None:
    """
    Measure average duration for the lambda function on concatenating n 23465 * 1 dimensional matrices.
    Default matrix size: 23465 * 1.
    :param n: Number of matrices for concatenating.
    :param memory_size: Memory size for the lambda.
    :param request_num: Number of concatenation requests to make.
    :param wait_time: Time to wait before sending/checking each request.
    :return: Average duration(ms) of executing 20 matrices-concatenation requests.
    """
    request_ids = []
    jobs = []
    start_time = datetime.datetime.utcnow().isoformat()

    print("Fetching {} matrices concatenation requests on {} matrices......".format(request_num, n))

    # Fetch matrices concatenation requests
    for i in range(request_num):
        print("Send {} request.".format(i))
        data = get_random_existing_bundle_uuids(n)
        r = requests.post(matrix_service_url, json=data)
        if r.status_code != 200:
            raise Exception(r.text)
        request_ids.append(r.json()["request_id"])
        time.sleep(wait_time)

    print("Actual requests fetched(remove duplicates): {}".format(len(set(request_ids))))
    print("Done!")

    # Poll for concatenation job until complete
    for request_id in request_ids:
        print("Checking request status for {}......".format(request_id))
        job = multiprocessing.Process(target=wait_job_to_complete, args=(request_id, ))
        jobs.append(job)
        job.start()

    for job in jobs:
        job.join()

    print("Done!")

    end_time = datetime.datetime.utcnow().isoformat()

    # Get duration for the lambda
    output = subprocess.check_output([GET_LAMBDA_DURATION_SCRIPT_PATH, log_group, str(start_time), str(end_time)])
    durations = output.decode("utf-8").split("\n")
    durations.pop()

    durations = [float(duration) for duration in durations]

    _, temp_path = tempfile.mkstemp(prefix="lambda_{}_cells_{}_".format(memory_size, n), suffix=".json", dir=DATA_DIR)

    with open(temp_path, "w") as f:
        data = {
            "memory_size": memory_size,
            "durations": durations,
            "request_ids": request_ids
        }
        json.dump(data, f)


if __name__ == '__main__':
    # measure_lambda_duration(memory_size=128, n=100, request_num=20, wait_time=30)
    # measure_lambda_duration(memory_size=128, n=150, request_num=20, wait_time=60)
    # measure_lambda_duration(memory_size=128, n=200, request_num=20, wait_time=100)
    # measure_lambda_duration(memory_size=128, n=250, request_num=20, wait_time=150)
    # measure_lambda_duration(memory_size=128, n=300, request_num=20, wait_time=250)

    # measure_lambda_duration(memory_size=256, n=100, request_num=20, wait_time=15)
    # measure_lambda_duration(memory_size=256, n=150, request_num=20, wait_time=30)
    # measure_lambda_duration(memory_size=256, n=200, request_num=20, wait_time=50)
    # measure_lambda_duration(memory_size=256, n=250, request_num=20, wait_time=75)
    # measure_lambda_duration(memory_size=256, n=300, request_num=20, wait_time=200)

    measure_lambda_duration(memory_size=512, n=100, request_num=20, wait_time=8)
    # measure_lambda_duration(memory_size=512, n=150, request_num=20, wait_time=15)
    # measure_lambda_duration(memory_size=512, n=200, request_num=20, wait_time=25)
    # measure_lambda_duration(memory_size=512, n=250, request_num=20, wait_time=40)
    # measure_lambda_duration(memory_size=512, n=300, request_num=20, wait_time=100)

    # measure_lambda_duration(memory_size=1024, n=100, request_num=20, wait_time=4)
    # measure_lambda_duration(memory_size=1024, n=150, request_num=20, wait_time=8)
    # measure_lambda_duration(memory_size=1024, n=200, request_num=20, wait_time=18)
    # measure_lambda_duration(memory_size=1024, n=250, request_num=20, wait_time=30)
    # measure_lambda_duration(memory_size=1024, n=300, request_num=20, wait_time=50)
    #
    # measure_lambda_duration(memory_size=2048, n=100, request_num=20, wait_time=2)
    # measure_lambda_duration(memory_size=2048, n=150, request_num=20, wait_time=4)
    # measure_lambda_duration(memory_size=2048, n=200, request_num=20, wait_time=9)
    # measure_lambda_duration(memory_size=2048, n=250, request_num=20, wait_time=10)
    # measure_lambda_duration(memory_size=2048, n=300, request_num=20, wait_time=25)
