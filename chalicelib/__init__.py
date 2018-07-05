import logging
import uuid

formatter = logging.Formatter("%(levelname)s:%(name)s:%(message)s")

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

logger = logging.getLogger("hca-matrix-service")
logger.addHandler(stream_handler)


def rand_uuid():
    return str(uuid.uuid4())


def rand_uuids(n):
    """
    Generate a random list of uuids.
    :param n: Number of uuid to generate.
    :return: A list of random uuids.
    """
    uuids = []

    for _ in range(n):
        uuids.append(rand_uuid())

    return uuids
