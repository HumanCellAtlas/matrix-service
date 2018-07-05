import logging
import random
import tempfile
import uuid

formatter = logging.Formatter("%(levelname)s:%(name)s:%(message)s")

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

logger = logging.getLogger("hca-matrix-service")
logger.addHandler(stream_handler)


def rand_uuid():
    return str(uuid.uuid4())


def rand_uuids():
    """
    Generate a random list of uuids.
    :return: A list of random uuids.
    """
    uuids = []
    n = random.randint(1, 11)

    for _ in range(n):
        uuids.append(rand_uuid())

    return uuids


def mk_temp_dirs():
    """
    Generate a random list of temp directories containing some random files.
    :return: A list of temp directories.
    """
    temp_dirs = []
    n = random.randint(1, 11)
    suffices = ('.json', '.loom', '.cvs', '',)

    # Generate n directories
    for _ in range(n):
        temp_dir = tempfile.mkdtemp()
        temp_dirs.append(temp_dir)
        k = random.randint(1, 11)

        # Generate k random files within each directory
        for _ in range(k):
            tempfile.mktemp(suffix=random.choice(suffices), dir=temp_dir)

    return temp_dirs
