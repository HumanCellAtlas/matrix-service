import json
import random

from chalicelib import rand_uuid
from config import BUNDLE_UUIDS_PATH


def rand_uuids(ub):
    """
    Generate a random list of uuids.
    :param ub: Upper bound for the number of uuids generated.
    :return: A list of random uuids.
    """
    uuids = []
    n = random.randint(1, ub)

    for _ in range(n):
        uuids.append(rand_uuid())

    return uuids


def get_random_existing_bundle_uuids(ub):
    """
    Get a random subset of existing bundle uuids stored in bundle_uuids.json.
    :param ub: Upper bound for the size of uuid subset.
    """
    # Get a random subset of bundle_uuids from sample bundle uuids
    with open(BUNDLE_UUIDS_PATH, "r") as f:
        sample_bundle_uuids = json.loads(f.read())

    n = random.randint(1, ub)
    bundle_uuids_subset = random.sample(sample_bundle_uuids, n)

    return bundle_uuids_subset
